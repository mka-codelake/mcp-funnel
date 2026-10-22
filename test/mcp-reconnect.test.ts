/* Reconnect bookkeeping of McpProxyService: backoff, cooldown, on-demand reconnects and
 * shutdown. Runs the proxy directly against a fake backend that refuses every connection
 * attempt and records when each one arrived. */

import { describe, it, afterEach } from "node:test"
import assert from "node:assert/strict"
import http from "node:http"
import { Writable } from "node:stream"
import fs from "fs"
import path from "path"
import os from "os"
import winston from "winston"

import logger from "../src/mcp-funnel-log.js"
import { McpServerManager } from "../src/mcp-server-manager.js"
import { McpProxyService } from "../src/mcp-proxy-service.js"
import type { ReconnectTiming } from "../src/mcp-proxy-service.js"
import { createSpecServer } from "./fixtures/mcp-spec-server.js"
import type { SpecServer } from "./fixtures/mcp-spec-server.js"

const USER = "test"
const SERVER_ID = "down-backend"

/* Answers every POST (each one is a connection attempt) with 503. Holds it instead while
 * `hold` is set or from attempt number `holdFrom` on, until release() */
interface FakeBackend {
    port: number
    attempts: number[]
    hold: boolean
    holdFrom: number
    release: () => void
    close: () => Promise<void>
}

async function startFakeBackend (port = 0): Promise<FakeBackend> {
    const held: http.ServerResponse[] = []
    const refuse = (res: http.ServerResponse) => {
        /* No keep-alive: a pooled socket to this fake would break the first request once a
         * real backend has taken over the port */
        res.writeHead(503, { "Content-Type": "text/plain", "Connection": "close" })
        res.end("unavailable")
    }
    const fake = {
        port: 0,
        attempts: [] as number[],
        hold: false,
        holdFrom: Infinity,
        release: () => { for (const res of held.splice(0)) refuse(res) },
        close: async () => {
            fake.release()
            server.closeAllConnections()
            await new Promise<void>((resolve) => server.close(() => resolve()))
        }
    }
    const server = http.createServer((req, res) => {
        req.resume()
        if (req.method !== "POST") {
            res.writeHead(405)
            res.end()
            return
        }
        fake.attempts.push(Date.now())
        if (fake.hold || fake.attempts.length >= fake.holdFrom) held.push(res)
        else refuse(res)
    })
    await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", () => resolve()))
    fake.port = (server.address() as { port: number }).port
    return fake
}

function writeRegistry (dataDir: string, port: number): void {
    const serversDir = path.join(dataDir, "servers")
    fs.mkdirSync(serversDir, { recursive: true })
    fs.writeFileSync(path.join(serversDir, `${USER}.json`), JSON.stringify({
        servers: [{
            id: SERVER_ID,
            name: "down",
            type: "http",
            enabled: true,
            config: { url: `http://127.0.0.1:${port}/mcp` },
            disabledTools: [],
            createdAt: new Date().toISOString(),
            updatedAt: null,
            lastConnected: null,
            lastError: null
        }]
    }, null, 2))
}

type ServerStatus = ReturnType<McpProxyService["getStatus"]>[number]

function statusOf (proxy: McpProxyService): ServerStatus {
    const status = proxy.getStatus().find((s) => s.id === SERVER_ID)
    assert.ok(status, "server missing from getStatus()")
    return status
}

async function waitFor (what: string, condition: () => boolean, timeoutMs = 5000): Promise<void> {
    const start = Date.now()
    while (!condition()) {
        if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for: ${what}`)
        await new Promise((r) => setTimeout(r, 5))
    }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function gaps (times: number[]): number[] {
    return times.slice(1).map((t, i) => t - times[i])
}

describe("reconnect bookkeeping", () => {
    let tmpDir: string | undefined
    let serverManager: McpServerManager | undefined
    let proxy: McpProxyService | undefined
    let fake: FakeBackend | undefined
    let backend: SpecServer | undefined

    async function startProxy (timing: Partial<ReconnectTiming>): Promise<{ proxy: McpProxyService, fake: FakeBackend }> {
        fake = await startFakeBackend()
        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "mcp-funnel-reconnect-"))
        writeRegistry(tmpDir, fake.port)
        serverManager = new McpServerManager(tmpDir, USER)
        proxy = new McpProxyService(serverManager, USER, timing)
        await proxy.initialize()
        return { proxy, fake }
    }

    /* Replaces the fake with a working MCP backend on the same port */
    async function bringBackendUp (name = "back"): Promise<SpecServer> {
        assert.ok(fake)
        const port = fake.port
        await fake.close()
        fake = undefined
        backend = await createSpecServer(port, { name })
        return backend
    }

    afterEach(async () => {
        try { await proxy?.shutdown() }
        finally {
            try { await fake?.close() }
            finally {
                try { await backend?.shutdown() }
                finally {
                    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true })
                    proxy = undefined
                    serverManager = undefined
                    fake = undefined
                    backend = undefined
                    tmpDir = undefined
                }
            }
        }
    })

    it("counts failed attempts and gives up after the configured number", async () => {
        const { proxy, fake } = await startProxy({ maxAttempts: 3, initialBackoffMs: 20, maxBackoffMs: 1000, cooldownMs: 60000 })

        await waitFor("state failed", () => statusOf(proxy).connectionState === "failed")
        assert.equal(statusOf(proxy).reconnectAttempts, 3)
        await sleep(100)
        assert.equal(fake.attempts.length, 4, "initial connect plus three reconnects, then rest")
    })

    it("doubles the wait between attempts up to the maximum", async () => {
        const { fake } = await startProxy({ maxAttempts: 4, initialBackoffMs: 150, maxBackoffMs: 600, cooldownMs: 60000 })

        await waitFor("four reconnects", () => fake.attempts.length === 5, 5000)
        /* Expected waits: 150, 300, 600, 600. Each gap also holds the duration of one failed
         * attempt; the waits are long enough that it cannot blur the factors. */
        const [g1, g2, g3, g4] = gaps(fake.attempts)
        assert.ok(g2 >= 1.5 * g1, `second wait ${g2} ms is not about twice the first ${g1} ms`)
        assert.ok(g3 >= 1.5 * g2, `third wait ${g3} ms is not about twice the second ${g2} ms`)
        assert.ok(g4 < 1.3 * g3, `fourth wait ${g4} ms grows past the maximum (third: ${g3} ms)`)
    })

    it("rests for the cooldown counted from the end of the last failed attempt", async () => {
        const { proxy, fake } = await startProxy({ maxAttempts: 2, initialBackoffMs: 20, maxBackoffMs: 1000, cooldownMs: 400 })

        /* Hold the last attempt of the series (initial connect, then two) for 300 ms */
        fake.holdFrom = 3
        await waitFor("last attempt in flight", () => fake.attempts.length === 3)
        await sleep(300)
        fake.holdFrom = Infinity
        fake.release()

        await waitFor("first attempt of the new series", () => fake.attempts.length === 4, 3000)
        const rest = gaps(fake.attempts)[2]
        assert.ok(rest >= 650, `the new series started ${rest} ms after the last attempt began; expected 300 ms attempt plus 400 ms cooldown`)
        assert.notEqual(statusOf(proxy).connectionState, "failed")
    })

    it("reconnects in the background once the backend is back", async () => {
        const { proxy } = await startProxy({ maxAttempts: 50, initialBackoffMs: 50, maxBackoffMs: 100, cooldownMs: 60000 })
        await waitFor("first failure", () => statusOf(proxy).reconnectAttempts >= 1)

        await bringBackendUp()
        await waitFor("connected", () => statusOf(proxy).connectionState === "connected", 3000)
        assert.equal(statusOf(proxy).reconnectAttempts, 0, "a successful reconnect resets the counter")
    })

    it("an explicit reconnectServer() starts over with a clean counter", async () => {
        const { proxy } = await startProxy({ maxAttempts: 50, initialBackoffMs: 20, maxBackoffMs: 100, cooldownMs: 60000 })
        await waitFor("two failures", () => statusOf(proxy).reconnectAttempts >= 2)

        await bringBackendUp()
        await proxy.reconnectServer(SERVER_ID)
        assert.equal(statusOf(proxy).connectionState, "connected")
        assert.equal(statusOf(proxy).reconnectAttempts, 0)
    })

    it("a qualified call reaches a resting server, an unqualified one leaves it alone", async () => {
        const { proxy } = await startProxy({ maxAttempts: 1, initialBackoffMs: 20, maxBackoffMs: 1000, cooldownMs: 60000 })
        await waitFor("state failed", () => statusOf(proxy).connectionState === "failed")
        await sleep(30)

        const back = await bringBackendUp()

        await assert.rejects(() => proxy.callTool("spec_whoami", {}), /Tool not found/)
        assert.equal(back.initCount(), 0, "an unqualified name must not wake a resting server")

        const result = await proxy.callTool("down:spec_whoami", {}) as { content: Array<{ text: string }> }
        assert.equal(result.content[0].text, "back")
        assert.equal(statusOf(proxy).connectionState, "connected")
        assert.equal(statusOf(proxy).reconnectAttempts, 0)
    })

    it("client calls neither multiply nor postpone the background attempts", async () => {
        const { proxy, fake } = await startProxy({ maxAttempts: 50, initialBackoffMs: 100, maxBackoffMs: 200, cooldownMs: 60000 })

        /* A client hammering a qualified name for 700 ms */
        const end = Date.now() + 700
        let calls = 0
        while (Date.now() < end) {
            calls++
            await proxy.callTool("down:spec_whoami", {}).catch(() => { /* expected: not connected */ })
            await sleep(10)
        }

        /* Background alone: initial + attempts at ~100, ~300, ~500, ~700 ms. On-demand
         * attempts add at most one per 100 ms; without the limit every call would try. */
        assert.ok(calls > 10, `only ${calls} calls made`)
        assert.ok(statusOf(proxy).reconnectAttempts >= 3, `background series stalled at ${statusOf(proxy).reconnectAttempts} attempts`)
        assert.ok(fake.attempts.length <= 14, `${fake.attempts.length} connection attempts for ${calls} calls`)
    })

    it("an on-demand reconnect joins a background attempt in flight", async () => {
        const { proxy, fake } = await startProxy({ maxAttempts: 50, initialBackoffMs: 200, maxBackoffMs: 200, cooldownMs: 60000 })
        fake.hold = true
        await waitFor("background attempt in flight", () => fake.attempts.length === 2)

        const call = proxy.callTool("down:spec_whoami", {}).catch((e: Error) => e)
        await sleep(50)
        assert.equal(fake.attempts.length, 2, "the on-demand reconnect started a second attempt")

        fake.hold = false
        fake.release()
        assert.ok(await call instanceof Error)
    })

    it("an explicit reconnect waits for an attempt in flight, then connects anew", async () => {
        const { proxy, fake } = await startProxy({ maxAttempts: 50, initialBackoffMs: 200, maxBackoffMs: 5000, cooldownMs: 60000 })
        fake.hold = true
        await waitFor("background attempt in flight", () => fake.attempts.length === 2)

        const reconnect = proxy.reconnectServer(SERVER_ID).catch((e: Error) => e)
        await sleep(50)
        assert.equal(fake.attempts.length, 2, "the explicit reconnect ran in parallel to the attempt in flight")

        fake.hold = false
        fake.release()
        assert.ok(await reconnect instanceof Error)
        assert.equal(fake.attempts.length, 3, "the explicit reconnect should make exactly one attempt of its own")
    })

    it("a connection that completes after the server was disabled is discarded", async () => {
        const { proxy } = await startProxy({ maxAttempts: 50, initialBackoffMs: 50, maxBackoffMs: 50, cooldownMs: 60000 })
        const back = await bringBackendUp()
        const release = back.holdInitialize()
        await waitFor("background attempt in initialize", () => back.initCount() >= 1, 3000)

        serverManager?.toggleServer(SERVER_ID)
        release()
        await sleep(200)
        assert.equal(proxy.isConnected(SERVER_ID), false)
        assert.equal(statusOf(proxy).connectionState, "offline")
    })

    it("a connection that completes after shutdown is discarded", async () => {
        const { proxy } = await startProxy({ maxAttempts: 50, initialBackoffMs: 50, maxBackoffMs: 50, cooldownMs: 60000 })
        const back = await bringBackendUp()
        const release = back.holdInitialize()
        await waitFor("background attempt in initialize", () => back.initCount() >= 1, 3000)

        await proxy.shutdown()
        release()
        await sleep(200)
        assert.equal(proxy.isConnected(SERVER_ID), false)
    })

    it("shutdown during a reconnect attempt schedules no further attempts", async () => {
        const { proxy, fake } = await startProxy({ maxAttempts: 50, initialBackoffMs: 30, maxBackoffMs: 30, cooldownMs: 60000 })
        fake.hold = true
        await waitFor("attempt in flight", () => fake.attempts.length === 2)

        const lines: string[] = []
        const capture = new winston.transports.Stream({
            stream: new Writable({
                write (chunk, _encoding, callback) {
                    lines.push(String(chunk))
                    callback()
                }
            })
        })
        logger.add(capture)
        try {
            await proxy.shutdown()
            fake.release()
            await sleep(200)
        }
        finally {
            logger.remove(capture)
        }
        assert.equal(fake.attempts.length, 2, "an attempt was made after shutdown")
        const late = lines.filter((l) => l.includes("MCP transport error"))
        assert.deepEqual(late, [], "transport errors after shutdown should be ignored")
    })

    it("refresh() keeps the one session cleanup timer", async () => {
        const { proxy } = await startProxy({ maxAttempts: 1, initialBackoffMs: 60000, maxBackoffMs: 60000, cooldownMs: 60000 })

        const original = globalThis.setInterval
        let started = 0
        globalThis.setInterval = ((handler: () => void, ms?: number) => {
            started++
            return original(handler, ms)
        }) as typeof setInterval
        try {
            await proxy.refresh()
            await proxy.refresh()
            assert.equal(started, 0, "refresh() started another interval timer")

            /* Positive control: with no cleanup timer running, initialize() starts one */
            await proxy.shutdown()
            await proxy.initialize()
            assert.equal(started, 1)
        }
        finally {
            globalThis.setInterval = original
        }
    })
})
