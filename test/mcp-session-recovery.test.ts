/* Recovery of lost backend connections: a backend that restarts, expires its sessions or
 * drops the connection in the middle of a call. Runs the proxy directly against an SSE
 * backend that rejects requests on sessions that were never initialized, as the Python
 * MCP servers do. */

import { describe, it, afterEach } from "node:test"
import assert from "node:assert/strict"
import { Writable } from "node:stream"
import fs from "fs"
import path from "path"
import os from "os"
import winston from "winston"

import logger from "../src/mcp-funnel-log.js"
import { McpServerManager } from "../src/mcp-server-manager.js"
import { McpProxyService } from "../src/mcp-proxy-service.js"
import type { ReconnectTiming } from "../src/mcp-proxy-service.js"
import { createSseBackend } from "./fixtures/sse-backend.js"
import type { SseBackend } from "./fixtures/sse-backend.js"
import { createSpecServer } from "./fixtures/mcp-spec-server.js"
import type { SpecServer } from "./fixtures/mcp-spec-server.js"

const USER = "test"
const SERVER_ID = "sse-backend"

/* Background reconnects only after 10 s: the tests see what a call does on its own, as in
 * the first seconds after a backend restart in production */
const TIMING: Partial<ReconnectTiming> = {
    maxAttempts: 50,
    initialBackoffMs: 10000,
    maxBackoffMs: 10000,
    cooldownMs: 60000,
    connectWaitMs: 5000,
    connectRetryMs: 100
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function waitFor (what: string, condition: () => boolean, timeoutMs = 5000): Promise<void> {
    const start = Date.now()
    while (!condition()) {
        if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for: ${what}`)
        await sleep(10)
    }
}

function writeRegistry (dataDir: string, entry: { type: string, config: Record<string, unknown> }): void {
    const serversDir = path.join(dataDir, "servers")
    fs.mkdirSync(serversDir, { recursive: true })
    fs.writeFileSync(path.join(serversDir, `${USER}.json`), JSON.stringify({
        servers: [{
            id: SERVER_ID,
            name: "alpha",
            type: entry.type,
            enabled: true,
            config: entry.config,
            disabledTools: [],
            createdAt: new Date().toISOString(),
            updatedAt: null,
            lastConnected: null,
            lastError: null
        }]
    }, null, 2))
}

describe("recovery of lost backend connections", () => {
    let tmpDir: string | undefined
    let serverManager: McpServerManager | undefined
    let proxy: McpProxyService | undefined
    let backend: SseBackend | undefined
    let httpBackend: SpecServer | undefined

    async function startProxy (entry: { type: string, config: Record<string, unknown> }, timing: Partial<ReconnectTiming>): Promise<McpProxyService> {
        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "mcp-funnel-recovery-"))
        writeRegistry(tmpDir, entry)
        serverManager = new McpServerManager(tmpDir, USER)
        proxy = new McpProxyService(serverManager, USER, { ...TIMING, ...timing })
        await proxy.initialize()
        assert.ok(proxy.isConnected(SERVER_ID), "initial connect failed")
        return proxy
    }

    async function setup (timing: Partial<ReconnectTiming> = {}): Promise<{ proxy: McpProxyService, backend: SseBackend }> {
        backend = await createSseBackend(0, "alpha")
        return { proxy: await startProxy({ type: "sse", config: { url: backend.url } }, timing), backend }
    }

    async function call (tool: string, args: Record<string, unknown> = {}): Promise<string> {
        assert.ok(proxy)
        const result = await proxy.callTool(tool, args) as { content: Array<{ text: string }> }
        return result.content[0].text
    }

    afterEach(async () => {
        try { await proxy?.shutdown() }
        finally {
            try {
                await backend?.shutdown()
                await httpBackend?.shutdown()
            }
            finally {
                if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true })
                proxy = undefined
                serverManager = undefined
                backend = undefined
                httpBackend = undefined
                tmpDir = undefined
            }
        }
    })

    it("a call a few seconds after a backend restart succeeds", async () => {
        const { backend } = await setup()

        await backend.stop()
        await sleep(1000)
        await backend.start()
        /* By now the event stream has reconnected on its own, into a session that was never
         * initialized; using it would fail with -32602 */
        await sleep(3200)

        assert.equal(await call("spec_whoami"), "alpha")
    })

    it("a call while the backend is down waits for it to come back", async () => {
        const { backend } = await setup()

        await backend.stop()
        await sleep(100)
        const started = Date.now()
        const pending = call("spec_whoami")
        await sleep(1500)
        await backend.start()

        assert.equal(await pending, "alpha")
        assert.ok(Date.now() - started >= 1400)
    })

    it("a session the backend has expired is replaced", async () => {
        const { backend } = await setup()
        const before = backend.initCount()

        backend.endStreams()
        await sleep(3500)

        assert.equal(await call("spec_whoami"), "alpha")
        assert.equal(backend.initCount(), before + 1)
    })

    it("a backend that stays down fails the call once the wait is over", async () => {
        const { backend } = await setup({ connectWaitMs: 800 })

        await backend.stop()
        await sleep(100)
        const started = Date.now()
        await assert.rejects(() => call("spec_whoami"), /could not be reached/)
        const elapsed = Date.now() - started
        assert.ok(elapsed >= 600 && elapsed < 3000, `gave up after ${elapsed} ms`)
    })

    it("tools of a lost connection stay listed while it reconnects", async () => {
        const { proxy, backend } = await setup()

        backend.endStreams()
        await sleep(100)

        const status = proxy.getStatus().find((s) => s.id === SERVER_ID)
        assert.equal(status?.connected, false)
        assert.equal(status?.connectionState, "reconnecting")
        assert.ok(proxy.getAllToolsInternal().some((t) => t.name === "spec_whoami"))
    })

    it("a read-only call cut off mid-way is repeated", async () => {
        const { backend } = await setup()

        const pending = call("spec_slow_read", { ms: 1500 })
        await waitFor("tool running", () => backend.executions("spec_slow_read") === 1)
        backend.endStreams()

        assert.equal(await pending, "read done")
        assert.equal(backend.executions("spec_slow_read"), 2)
    })

    it("a call that changes state is not repeated when cut off mid-way", async () => {
        const { backend } = await setup()

        const pending = call("spec_slow_write", { ms: 1500 }).catch((e: Error) => e)
        await waitFor("tool running", () => backend.executions("spec_slow_write") === 1)
        backend.endStreams()

        const result = await pending
        assert.ok(result instanceof Error, `expected an error, got ${String(result)}`)
        assert.match(result.message, /may or may not have taken effect/)
        assert.equal(backend.executions("spec_slow_write"), 1)
    })

    it("concurrent calls on a lost connection share one reconnect", async () => {
        const { backend } = await setup()
        const before = backend.initCount()

        backend.endStreams()
        await sleep(100)
        const results = await Promise.all([call("spec_whoami"), call("spec_whoami"), call("spec_whoami")])

        assert.deepEqual(results, ["alpha", "alpha", "alpha"])
        assert.equal(backend.initCount(), before + 1)
    })

    it("an error answer from the backend is passed on, not retried", async () => {
        const { proxy, backend } = await setup()
        const before = backend.initCount()

        backend.rejectNextToolCall(-32602, "Invalid request parameters")
        await assert.rejects(() => call("spec_whoami"), /Invalid request parameters/)

        /* Its text must not be read as a connection problem */
        backend.rejectNextToolCall(-32000, "Database not connected")
        await assert.rejects(() => call("spec_whoami"), /Database not connected/)

        assert.equal(backend.initCount(), before, "the connection was rebuilt for an error answer")
        assert.ok(proxy.isConnected(SERVER_ID))
    })

    it("calls waiting for the same backend share one retry schedule", async () => {
        const { backend } = await setup()
        const lines: string[] = []
        const capture = new winston.transports.Stream({
            stream: new Writable({
                write (chunk, _encoding, callback) {
                    lines.push(String(chunk))
                    callback()
                }
            })
        })

        await backend.stop()
        await sleep(100)
        logger.add(capture)
        try {
            /* Three calls arriving one after another while the backend is down */
            const pending: Array<Promise<string>> = []
            for (let i = 0; i < 3; i++) {
                pending.push(call("spec_whoami"))
                await sleep(150)
            }
            await sleep(1200)
            await backend.start()
            assert.deepEqual(await Promise.all(pending), ["alpha", "alpha", "alpha"])
        }
        finally {
            logger.remove(capture)
        }

        /* One schedule: waits of 100, 200, 400, 800 ms give about five attempts in 1.6 s;
         * each call on its own schedule would make three times as many */
        const attempts = lines.filter((l) => l.includes("Connecting to MCP server: alpha")).length
        assert.ok(attempts >= 2 && attempts <= 7, `${attempts} connection attempts for three waiting calls`)
    })

    it("an explicit reconnect replaces a healthy connection without a gap", async () => {
        const { proxy, backend } = await setup()
        const before = backend.initCount()

        await proxy.reconnectServer(SERVER_ID)
        await sleep(100)

        assert.ok(proxy.isConnected(SERVER_ID), "closing the old transport removed the new connection")
        assert.equal(await call("spec_whoami"), "alpha")
        assert.equal(backend.initCount(), before + 1)
    })

    it("a failing keep-alive ping marks the connection as lost", async () => {
        const { proxy, backend } = await setup({ pingIntervalMs: 100 })

        backend.failPings()
        await waitFor("connection marked lost", () => !proxy.isConnected(SERVER_ID), 2000)
        backend.failPings(false)
        assert.equal(await call("spec_whoami"), "alpha")
    })

    it("a backend that refuses the connection is not waited for", async () => {
        const { backend } = await setup()

        backend.rejectStreams(401)
        backend.endStreams()
        await sleep(100)

        const started = Date.now()
        await assert.rejects(() => call("spec_whoami"), /could not be reached/)
        assert.ok(Date.now() - started < 2000, "an authentication error should not be waited out")
    })

    it("a lost connection to a server that gets disabled is dropped", async () => {
        const { proxy, backend } = await setup({ initialBackoffMs: 100, maxBackoffMs: 100 })

        await backend.stop()
        await waitFor("connection marked lost", () => !proxy.isConnected(SERVER_ID))
        serverManager?.toggleServer(SERVER_ID)

        await waitFor("tools dropped", () => !proxy.getAllToolsInternal().some((t) => t.name === "spec_whoami"), 3000)
    })

    it("shutdown ends a call that waits for the backend", async () => {
        const { proxy, backend } = await setup({ connectRetryMs: 3000 })

        await backend.stop()
        await sleep(100)
        const pending = call("spec_whoami").catch((e: Error) => e)
        await sleep(300)
        const started = Date.now()
        await proxy.shutdown()

        const result = await pending
        assert.ok(result instanceof Error)
        assert.ok(Date.now() - started < 1000, "the waiting call outlived the shutdown")
    })

    it("a lost connection is dropped with its tools once the reconnect series is exhausted", async () => {
        const { proxy, backend } = await setup({ maxAttempts: 1, initialBackoffMs: 100, maxBackoffMs: 100 })

        await backend.stop()
        await waitFor("tools dropped", () => !proxy.getAllToolsInternal().some((t) => t.name === "spec_whoami"), 3000)
        assert.equal(proxy.getStatus().find((s) => s.id === SERVER_ID)?.connectionState, "failed")

        const started = Date.now()
        await assert.rejects(() => call("spec_whoami"), /Tool not found/)
        assert.ok(Date.now() - started < 1000, "a resting server should fail the call right away")
    })

    it("a stdio backend that exits is started again for the next call", async () => {
        const script = path.join(__dirname, "fixtures", "stdio-backend.js")
        const proxy = await startProxy({ type: "stdio", config: { command: process.execPath, args: [script] } }, {})
        const firstPid = await call("spec_pid")

        assert.equal(await call("spec_exit"), "bye")
        await waitFor("connection marked lost", () => !proxy.isConnected(SERVER_ID), 3000)
        assert.ok(proxy.getAllToolsInternal().some((t) => t.name === "spec_pid"), "tools should stay listed")

        const secondPid = await call("spec_pid")
        assert.notEqual(secondPid, firstPid)
    })

    it("a call while a streamable HTTP backend restarts waits for it", async () => {
        httpBackend = await createSpecServer(0, { name: "alpha" })
        const port = Number(new URL(httpBackend.url).port)
        await startProxy({ type: "http", config: { url: httpBackend.url } }, {})

        await httpBackend.shutdown()
        httpBackend = undefined
        const pending = call("spec_whoami")
        await sleep(1000)
        httpBackend = await createSpecServer(port, { name: "alpha" })

        assert.equal(await pending, "alpha")
    })
})
