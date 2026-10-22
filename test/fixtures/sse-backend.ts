/* SSE backend for connection recovery tests. Behaves like the Python MCP servers behind the
 * funnel: sessions live as long as their event stream, a request on a session that has not
 * been initialized is answered with -32602 "Invalid request parameters", and the process
 * can be restarted on the same port. */

import http from "node:http"
import type { Socket } from "node:net"
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js"
import { z } from "zod"

export interface SseBackend {
    url: string
    /* Number of initialize requests served, over all restarts */
    initCount: () => number
    /* How often each tool has started executing, over all restarts */
    executions: (tool: string) => number
    /* Stops listening and cuts every connection, as a process that is killed */
    stop: () => Promise<void>
    start: () => Promise<void>
    /* Ends all event streams but keeps running, as a server that expires its sessions */
    endStreams: () => void
    /* Answers the next tools/call with this JSON-RPC error instead of executing it */
    rejectNextToolCall: (code: number, message: string) => void
    /* Answers pings with 404, as a server that has forgotten the session behind the stream */
    failPings: (on?: boolean) => void
    /* Answers new event streams with this HTTP status, e.g. 401; null accepts them again */
    rejectStreams: (status: number | null) => void
    shutdown: () => Promise<void>
}

interface Session {
    transport: SSEServerTransport
    server: McpServer
    initialized: boolean
}

export async function createSseBackend (port = 0, name = "sse"): Promise<SseBackend> {
    const sessions = new Map<string, Session>()
    const sockets = new Set<Socket>()
    const counts = new Map<string, number>()
    let inits = 0
    let rejection: { code: number, message: string } | null = null
    let pingsFail = false
    let streamStatus: number | null = null
    let httpServer: http.Server | null = null
    let listenPort = port

    const count = (tool: string) => counts.set(tool, (counts.get(tool) ?? 0) + 1)

    const createServer = (): McpServer => {
        const server = new McpServer({ name: "sse-test-backend", version: "1.0.0" })
        server.registerTool("spec_whoami", {
            description: "Returns the name of the backend instance",
            annotations: { readOnlyHint: true }
        }, async () => {
            count("spec_whoami")
            return { content: [{ type: "text", text: name }] }
        })
        server.registerTool("spec_slow_read", {
            description: "Waits, then answers; read-only",
            inputSchema: { ms: z.number() },
            annotations: { readOnlyHint: true }
        }, async (args) => {
            count("spec_slow_read")
            await new Promise((resolve) => setTimeout(resolve, args.ms))
            return { content: [{ type: "text", text: "read done" }] }
        })
        server.registerTool("spec_slow_write", {
            description: "Waits, then answers; changes state, not idempotent",
            inputSchema: { ms: z.number() }
        }, async (args) => {
            count("spec_slow_write")
            await new Promise((resolve) => setTimeout(resolve, args.ms))
            return { content: [{ type: "text", text: "write done" }] }
        })
        return server
    }

    const handler = async (req: http.IncomingMessage, res: http.ServerResponse) => {
        const url = new URL(req.url ?? "/", "http://localhost")
        if (req.method === "GET" && url.pathname === "/sse") {
            if (streamStatus !== null) {
                res.writeHead(streamStatus).end()
                return
            }
            const transport = new SSEServerTransport("/messages", res)
            const server = createServer()
            const session: Session = { transport, server, initialized: false }
            sessions.set(transport.sessionId, session)
            transport.onclose = () => { sessions.delete(transport.sessionId) }
            await server.connect(transport)
            return
        }
        if (req.method === "POST" && url.pathname === "/messages") {
            const session = sessions.get(url.searchParams.get("sessionId") ?? "")
            if (!session) {
                res.writeHead(404).end("Could not find session")
                return
            }
            const chunks: Buffer[] = []
            for await (const chunk of req) chunks.push(chunk as Buffer)
            const message = JSON.parse(Buffer.concat(chunks).toString()) as { id?: number | string, method?: string }

            if (message.method === "initialize") {
                inits++
                session.initialized = true
            }
            if (message.method === "ping" && pingsFail) {
                res.writeHead(404).end("Could not find session")
                return
            }
            const answer = (code: number, text: string) => {
                res.writeHead(202).end("Accepted")
                session.transport.send({ jsonrpc: "2.0", id: message.id ?? 0, error: { code, message: text } }).catch(() => { /* stream gone */ })
            }
            if (!session.initialized && message.id !== undefined && message.method !== "ping") {
                answer(-32602, "Invalid request parameters")
                return
            }
            if (message.method === "tools/call" && rejection) {
                const { code, message: text } = rejection
                rejection = null
                answer(code, text)
                return
            }
            await session.transport.handlePostMessage(req, res, message)
            return
        }
        res.writeHead(404).end()
    }

    const start = async () => {
        const server = http.createServer((req, res) => { handler(req, res).catch(() => res.destroy()) })
        server.on("connection", (socket) => {
            sockets.add(socket)
            socket.on("close", () => sockets.delete(socket))
        })
        await new Promise<void>((resolve) => server.listen(listenPort, "127.0.0.1", () => resolve()))
        listenPort = (server.address() as { port: number }).port
        httpServer = server
    }

    const endStreams = () => {
        for (const { transport } of [...sessions.values()]) {
            transport.close().catch(() => { /* already closed */ })
        }
        sessions.clear()
    }

    const stop = async () => {
        const server = httpServer
        httpServer = null
        for (const socket of sockets) socket.destroy()
        sessions.clear()
        if (server) await new Promise<void>((resolve) => server.close(() => resolve()))
    }

    await start()

    return {
        url: `http://127.0.0.1:${listenPort}/sse`,
        initCount: () => inits,
        executions: (tool) => counts.get(tool) ?? 0,
        stop,
        start,
        endStreams,
        rejectNextToolCall: (code, message) => { rejection = { code, message } },
        failPings: (on = true) => { pingsFail = on },
        rejectStreams: (status) => { streamStatus = status },
        shutdown: stop
    }
}
