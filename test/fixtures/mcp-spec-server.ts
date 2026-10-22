/* MCP Spec Test Server
 * Exercises all MCP 2025-11-25 spec features for integration testing */

import http from "node:http"
import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js"
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js"
import { z } from "zod"

export interface SpecServerOptions {
    /* Returned by spec_whoami, so a test can tell which backend answered a call */
    name?: string
}

export interface SpecServer {
    url: string
    httpServer: http.Server
    /* Closes and forgets all sessions, as a backend restart would. The next request on an
     * old session id gets a 404 "Session not found". */
    dropSessions: () => Promise<void>
    /* Number of initialize requests served so far */
    initCount: () => number
    /* Whether sessions initialized from now on offer spec_whoami */
    offerWhoami: (offered: boolean) => void
    /* Answers the next tools/call requests with these HTTP errors, one per request */
    failNextToolCalls: (failures: Array<{ status: number, body: string }>) => void
    /* Holds initialize requests from now on until the returned function is called;
     * initCount() already counts a held request */
    holdInitialize: () => () => void
    shutdown: () => Promise<void>
}

function createMcpServer (name: string, withWhoami: boolean): McpServer {
    const server = new McpServer(
        { name: "mcp-spec-test-server", version: "1.0.0" },
        { capabilities: { logging: {} } }
    )

    registerTools(server, name, withWhoami)
    registerResources(server)
    registerPrompts(server)
    return server
}

export async function createSpecServer (port?: number, options: SpecServerOptions = {}): Promise<SpecServer> {
    const name = options.name ?? "spec"
    let inits = 0
    let withWhoami = true
    let initGate: Promise<void> | null = null
    let releaseInit: (() => void) | null = null
    const pendingFailures: Array<{ status: number, body: string }> = []

    /* One McpServer per session: a Protocol instance can only serve a single transport,
     * so a client that reconnects needs a fresh one. */
    const servers = new Set<McpServer>()

    /* HTTP wrapper with StreamableHTTP transport */
    const transports = new Map<string, StreamableHTTPServerTransport>()
    const httpServer = http.createServer(async (req, res) => {
        if (req.url !== "/mcp") {
            res.writeHead(404)
            res.end()
            return
        }

        /* Collect body */
        const chunks: Buffer[] = []
        for await (const chunk of req) chunks.push(chunk as Buffer)
        const bodyStr = Buffer.concat(chunks).toString()
        let body: unknown
        try {
            body = bodyStr ? JSON.parse(bodyStr) : undefined
        }
        catch {
            res.writeHead(400, { "Content-Type": "application/json" })
            res.end(JSON.stringify({ jsonrpc: "2.0", error: { code: -32700, message: "Parse error" }, id: null }))
            return
        }
        const rawReq = req as unknown as (http.IncomingMessage & { body?: unknown })
        rawReq.body = body

        /* Route by method */
        if (req.method === "POST") {
            const isInit = body && typeof body === "object" && !Array.isArray(body) && (body as Record<string, unknown>).method === "initialize"

            if (isInit) {
                inits++
                if (initGate) await initGate
                const server = createMcpServer(name, withWhoami)
                servers.add(server)
                const transport = new StreamableHTTPServerTransport({
                    sessionIdGenerator: () => crypto.randomUUID(),
                    enableJsonResponse: true,
                    onsessioninitialized: (sessionId: string) => {
                        transports.set(sessionId, transport)
                    }
                })
                transport.onclose = () => {
                    const sid = transport.sessionId
                    if (sid) transports.delete(sid)
                    servers.delete(server)
                }
                await server.server.connect(transport)
                await transport.handleRequest(rawReq, res, body)
                return
            }

            /* Non-initialize POST: look up session */
            const sessionId = req.headers["mcp-session-id"] as string | undefined
            const isToolCall = body && typeof body === "object" && !Array.isArray(body) && (body as Record<string, unknown>).method === "tools/call"
            const failure = isToolCall ? pendingFailures.shift() : undefined
            if (failure) {
                res.writeHead(failure.status, { "Content-Type": "text/plain" })
                res.end(failure.body)
                return
            }
            if (sessionId && transports.has(sessionId)) {
                await transports.get(sessionId)!.handleRequest(rawReq, res, body)
                return
            }

            res.writeHead(404, { "Content-Type": "application/json" })
            res.end(JSON.stringify({ jsonrpc: "2.0", error: { code: -32000, message: "Session not found" }, id: null }))
        }
        else if (req.method === "GET" || req.method === "DELETE") {
            /* SSE stream (GET) or session termination (DELETE) */
            const sessionId = req.headers["mcp-session-id"] as string | undefined
            if (sessionId && transports.has(sessionId)) {
                await transports.get(sessionId)!.handleRequest(rawReq, res, body)
                return
            }
            res.writeHead(404)
            res.end()
        }
        else {
            res.writeHead(405)
            res.end()
        }
    })

    const listenPort = port ?? 0
    await new Promise<void>((resolve) => {
        httpServer.listen(listenPort, "127.0.0.1", () => resolve())
    })
    const addr = httpServer.address() as { port: number }
    const url = `http://127.0.0.1:${addr.port}/mcp`

    const dropSessions = async () => {
        const open = [...transports.values()]
        transports.clear()
        for (const transport of open) {
            try { await transport.close() } catch { /* ignore */ }
        }
    }

    const shutdown = async () => {
        releaseInit?.()
        await dropSessions()
        for (const server of servers) {
            try { await server.close() } catch { /* ignore */ }
        }
        servers.clear()
        httpServer.closeAllConnections()
        await new Promise<void>((resolve) => httpServer.close(() => resolve()))
    }

    return {
        url,
        httpServer,
        dropSessions,
        initCount: () => inits,
        offerWhoami: (offered) => { withWhoami = offered },
        failNextToolCalls: (failures) => { pendingFailures.push(...failures) },
        holdInitialize: () => {
            let open = () => { /* replaced below */ }
            initGate = new Promise<void>((resolve) => { open = resolve })
            releaseInit = () => {
                initGate = null
                releaseInit = null
                open()
            }
            return releaseInit
        },
        shutdown
    }
}

function registerTools (server: McpServer, name: string, withWhoami: boolean): void {
    /* spec_whoami — names the backend instance that served the call */
    if (withWhoami) {
        server.registerTool("spec_whoami", {
            description: "Returns the name of the backend instance",
            annotations: { readOnlyHint: true }
        }, async () => {
            return { content: [{ type: "text", text: name }] }
        })
    }

    /* spec_echo — basic tool with string input */
    server.registerTool("spec_echo", {
        description: "Echoes the input message back",
        inputSchema: { message: z.string() }
    }, async (args) => {
        return { content: [{ type: "text", text: args.message }] }
    })

    /* spec_annotated — tool with title, annotations */
    server.registerTool("spec_annotated", {
        title: "Annotated Tool",
        description: "A tool with annotations for testing metadata preservation",
        inputSchema: { input: z.string() },
        annotations: {
            readOnlyHint: true,
            destructiveHint: false,
            idempotentHint: true,
            openWorldHint: false
        }
    }, async (args) => {
        return { content: [{ type: "text", text: `annotated: ${args.input}` }] }
    })

    /* spec_structured — tool with outputSchema returning structuredContent */
    server.registerTool("spec_structured", {
        description: "Returns structured output",
        inputSchema: { value: z.string() },
        outputSchema: { result: z.string(), processed: z.boolean() }
    }, async (args) => {
        return { structuredContent: { result: args.value.toUpperCase(), processed: true }, content: [] }
    })

    /* spec_error — tool that returns isError */
    server.registerTool("spec_error", {
        description: "Always returns an error",
        inputSchema: { reason: z.string().optional() }
    }, async (args) => {
        return {
            content: [{ type: "text", text: args.reason || "Something went wrong" }],
            isError: true
        }
    })

    /* spec_progress — sends progress notifications */
    server.registerTool("spec_progress", {
        description: "Sends progress notifications",
        inputSchema: { steps: z.number().optional() }
    }, async (args, extra) => {
        const steps = args.steps || 5
        const meta = extra._meta
        const token = meta?.progressToken
        if (token !== undefined) {
            for (let i = 0; i < steps; i++) {
                await extra.sendNotification({
                    method: "notifications/progress",
                    params: { progressToken: token, progress: i + 1, total: steps }
                })
            }
        }
        return { content: [{ type: "text", text: `Completed ${steps} steps` }] }
    })

    /* spec_logging — sends logging messages at all levels */
    server.registerTool("spec_logging", {
        description: "Sends logging messages at various levels"
    }, async () => {
        const levels = ["debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"] as const
        for (const level of levels) {
            await server.sendLoggingMessage({ level, logger: "spec-test", data: `Test ${level} message` })
        }
        return { content: [{ type: "text", text: "Sent logging messages at all levels" }] }
    })

    /* spec_sampling — triggers sampling from backend to client */
    server.registerTool("spec_sampling", {
        description: "Triggers a sampling request to the client",
        inputSchema: { prompt: z.string() }
    }, async (args) => {
        try {
            const result = await server.server.createMessage({
                messages: [{ role: "user", content: { type: "text", text: args.prompt } }],
                maxTokens: 100
            })
            return { content: [{ type: "text", text: `Sampling response: ${JSON.stringify(result)}` }] }
        }
        catch (err) {
            return { content: [{ type: "text", text: `Sampling failed: ${(err as Error).message}` }], isError: true }
        }
    })

    /* spec_elicitation — triggers elicitation from backend to client */
    server.registerTool("spec_elicitation", {
        description: "Triggers an elicitation request to the client",
        inputSchema: { question: z.string() }
    }, async (args) => {
        try {
            const result = await server.server.elicitInput({
                message: args.question,
                requestedSchema: {
                    type: "object" as const,
                    properties: { answer: { type: "string" as const, description: "Your answer" } }
                }
            })
            return { content: [{ type: "text", text: `Elicitation response: ${JSON.stringify(result)}` }] }
        }
        catch (err) {
            return { content: [{ type: "text", text: `Elicitation failed: ${(err as Error).message}` }], isError: true }
        }
    })

    /* spec_roots — triggers roots list request from backend to client */
    server.registerTool("spec_roots", {
        description: "Requests root list from the client"
    }, async () => {
        try {
            const result = await server.server.listRoots()
            return { content: [{ type: "text", text: `Roots: ${JSON.stringify(result)}` }] }
        }
        catch (err) {
            return { content: [{ type: "text", text: `Roots failed: ${(err as Error).message}` }], isError: true }
        }
    })

    /* spec_list_changed — registers a dynamic tool, sends listChanged */
    server.registerTool("spec_list_changed", {
        description: "Dynamically adds a tool and sends list_changed notification"
    }, async () => {
        const dynamicName = `spec_dynamic_${Date.now()}`
        server.registerTool(dynamicName, {
            description: "Dynamically registered tool"
        }, async () => {
            return { content: [{ type: "text", text: "I am dynamic" }] }
        })
        server.sendToolListChanged()
        return { content: [{ type: "text", text: `Registered ${dynamicName} and sent list_changed` }] }
    })
}

function registerResources (server: McpServer): void {
    /* spec://static/readme — static text resource */
    server.registerResource("spec-readme", "spec://static/readme", {
        title: "Spec Test README",
        description: "A static test resource",
        mimeType: "text/plain",
        annotations: { audience: ["user"] }
    }, async () => {
        return {
            contents: [{ uri: "spec://static/readme", text: "This is the spec test server README.", mimeType: "text/plain" }]
        }
    })

    /* spec://static/data — static JSON resource */
    server.registerResource("spec-data", "spec://static/data", {
        title: "Spec Test Data",
        description: "A static JSON resource",
        mimeType: "application/json"
    }, async () => {
        return {
            contents: [{ uri: "spec://static/data", text: JSON.stringify({ key: "value", numbers: [1, 2, 3] }), mimeType: "application/json" }]
        }
    })

    /* spec://template/{id} — URI template with completion */
    server.registerResource("spec-template", new ResourceTemplate("spec://template/{id}", {
        list: undefined,
        complete: {
            id: () => ["alpha", "beta", "gamma"]
        }
    }), {
        title: "Spec Template Resource",
        description: "A templated resource",
        mimeType: "text/plain"
    }, async (uri, variables) => {
        const id = variables.id as string
        return {
            contents: [{ uri: uri.href, text: `Template resource with id: ${id}`, mimeType: "text/plain" }]
        }
    })
}

function registerPrompts (server: McpServer): void {
    /* spec_simple — no args, with title */
    server.registerPrompt("spec_simple", {
        title: "Simple Prompt",
        description: "A simple prompt without arguments"
    }, async () => {
        return {
            messages: [{ role: "user", content: { type: "text", text: "This is a simple test prompt." } }]
        }
    })

    /* spec_with_args — with argsSchema and completion */
    server.registerPrompt("spec_with_args", {
        title: "Prompt With Args",
        description: "A prompt that accepts language and topic arguments",
        argsSchema: {
            language: z.string(),
            topic: z.string()
        }
    }, async (args) => {
        return {
            messages: [{
                role: "user",
                content: { type: "text", text: `Write about ${args.topic} in ${args.language}.` }
            }]
        }
    })

    /* spec_multi_message — multiple messages with different roles */
    server.registerPrompt("spec_multi_message", {
        title: "Multi-Message Prompt",
        description: "Returns multiple messages with different roles"
    }, async () => {
        return {
            messages: [
                { role: "assistant", content: { type: "text", text: "I am a helpful assistant." } },
                { role: "user", content: { type: "text", text: "Please help me with testing." } }
            ]
        }
    })
}
