/* Integration tests: MCP Spec Test Server → Funnel → SDK Client
 * Tests MCP 2025-11-25 spec compliance through the funnel proxy */

import { describe, it, before, after } from "node:test"
import assert from "node:assert/strict"
import http from "node:http"
import fs from "fs"
import path from "path"
import os from "os"

import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js"
import {
    CreateMessageRequestSchema, ElicitRequestSchema, ListRootsRequestSchema
} from "@modelcontextprotocol/sdk/types.js"

import { createApp } from "../src/mcp-funnel-server.js"
import type { McpFunnelConfig } from "../src/mcp-funnel-config.js"
import { createSpecServer } from "./fixtures/mcp-spec-server.js"

/* ── helpers ────────────────────────────────────────── */

function makeTmpDir (): string {
    return fs.mkdtempSync(path.join(os.tmpdir(), "mcp-funnel-spec-"))
}

function writeServerConfig (dataDir: string, backendUrl: string, disabledTools: string[] = []): void {
    writeServersConfig(dataDir, [{ id: "spec-test-backend", name: "spec-test", url: backendUrl, disabledTools }])
}

function writeServersConfig (dataDir: string, entries: Array<{ id: string; name: string; url: string; disabledTools?: string[] }>): void {
    const serversDir = path.join(dataDir, "servers")
    fs.mkdirSync(serversDir, { recursive: true })
    const serverConfig = {
        servers: entries.map((e) => ({
            id: e.id,
            name: e.name,
            type: "http",
            enabled: true,
            config: { url: e.url },
            disabledTools: e.disabledTools ?? [],
            createdAt: new Date().toISOString(),
            updatedAt: null,
            lastConnected: null,
            lastError: null
        }))
    }
    fs.writeFileSync(path.join(serversDir, "local.json"), JSON.stringify(serverConfig, null, 2))
}

async function startFunnel (dataDir: string): Promise<{ baseUrl: string; server: http.Server; shutdown: () => Promise<void>; userProxyManager: ReturnType<typeof createApp>["userProxyManager"]; statsManager: ReturnType<typeof createApp>["statsManager"] }> {
    const config: McpFunnelConfig = {
        port: 0,
        dataDir,
        sessionSecret: "test-secret-spec",
        sessionMaxAge: 86400000,
        adminUser: "",
        adminPass: "",
        nodeEnv: "test",
        singleUser: true,
        authMode: "both",
        allowedOrigins: [],
        mcpProtocolVersions: ["2025-11-25", "2025-03-26"]
    }
    const result = createApp(config)

    const server = await new Promise<http.Server>((resolve) => {
        const s = result.app.listen(0, "127.0.0.1", () => resolve(s))
    })
    const addr = server.address() as { port: number }
    const baseUrl = `http://127.0.0.1:${addr.port}`

    const shutdown = async () => {
        result.statsManager.flush()
        await result.userProxyManager.shutdown()
        await new Promise<void>((resolve) => server.close(() => resolve()))
    }

    return { baseUrl, server, shutdown, userProxyManager: result.userProxyManager, statsManager: result.statsManager }
}

async function createTestClient (funnelUrl: string): Promise<Client> {
    const client = new Client(
        { name: "spec-test-client", version: "1.0.0" },
        {
            capabilities: {
                sampling: {},
                elicitation: {},
                roots: { listChanged: true }
            }
        }
    )

    const transport = new StreamableHTTPClientTransport(new URL(`${funnelUrl}/mcp`))
    await client.connect(transport)
    return client
}

/* Raw HTTP helper for non-SDK tests */
function request (url: string, opts: { method?: string; body?: unknown; headers?: Record<string, string> } = {}): Promise<{ status: number; headers: http.IncomingHttpHeaders; body: string }> {
    return new Promise((resolve, reject) => {
        const parsed = new URL(url)
        const payload = opts.body ? JSON.stringify(opts.body) : undefined
        const reqHeaders: Record<string, string> = { ...opts.headers }
        if (payload) {
            reqHeaders["Content-Type"] = "application/json"
            reqHeaders["Content-Length"] = Buffer.byteLength(payload).toString()
        }
        const req = http.request({
            hostname: parsed.hostname,
            port: parsed.port,
            path: parsed.pathname + parsed.search,
            method: opts.method || "GET",
            headers: reqHeaders
        }, (res) => {
            let data = ""
            res.on("data", (chunk: string) => { data += chunk })
            res.on("end", () => resolve({ status: res.statusCode!, headers: res.headers, body: data }))
        })
        req.on("error", reject)
        if (payload) req.write(payload)
        req.end()
    })
}

/* ── test suite ─────────────────────────────────────── */

describe("MCP spec proxy integration", () => {
    let tmpDir: string
    let backendUrl: string
    let backendShutdown: () => Promise<void>
    let funnelBaseUrl: string
    let funnelShutdown: () => Promise<void>
    let client: Client

    before(async () => {
        /* 1. Start spec test backend */
        const backend = await createSpecServer(0)
        backendUrl = backend.url
        backendShutdown = backend.shutdown

        /* 2. Write server config for funnel */
        tmpDir = makeTmpDir()
        writeServerConfig(tmpDir, backendUrl)

        /* 3. Start funnel */
        const funnel = await startFunnel(tmpDir)
        funnelBaseUrl = funnel.baseUrl
        funnelShutdown = funnel.shutdown

        /* 4. Connect SDK client to funnel (triggers lazy proxy initialization) */
        client = await createTestClient(funnelBaseUrl)

        /* 5. Wait for proxy to connect to backend */
        await new Promise(r => setTimeout(r, 3000))
    })

    after(async () => {
        try { await (client as unknown as { close: () => Promise<void> }).close() } catch { /* ignore */ }
        await funnelShutdown()
        await backendShutdown()
        fs.rmSync(tmpDir, { recursive: true, force: true })
    })

    /* ── A. Tool Proxying via meta-tools ────────────── */

    describe("A. Tool proxying", () => {
        it("spec_echo is discoverable via mcp_discover_tools", async () => {
            const result = await client.callTool({
                name: "mcp_discover_tools",
                arguments: { words: "spec echo" }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.ok(text.includes("spec_echo"), "should find spec_echo")
        })

        it("discovery results name the owning server", async () => {
            const result = await client.callTool({
                name: "mcp_discover_tools",
                arguments: { words: "spec echo" }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.ok(text.includes("spec_echo [spec-test]:"),
                `expected "- spec_echo [spec-test]: ..." in:\n${text}`)
        })

        it("a server-qualified tool name is callable", async () => {
            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "spec-test:spec_echo", arguments: { message: "qualified" } }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.equal(text, "qualified",
                "the backend must receive the unqualified name")
        })

        it("a server-qualified schema lookup resolves to that server", async () => {
            const result = await client.callTool({
                name: "mcp_get_tool_schema",
                arguments: { tool_name: "spec-test:spec_annotated" }
            })
            const schema = JSON.parse((result.content as Array<{ type: string; text: string }>)[0].text)
            assert.equal(schema.title, "Annotated Tool")
            assert.equal(schema.server, "spec-test")
        })

        it("a known server without the tool reports which server was meant", async () => {
            const result = await client.callTool({
                name: "mcp_get_tool_schema",
                arguments: { tool_name: "spec-test:does_not_exist" }
            })
            const parsed = JSON.parse((result.content as Array<{ type: string; text: string }>)[0].text)
            assert.ok(parsed.error.includes("does_not_exist"), parsed.error)
            assert.ok(parsed.error.includes("spec-test"), parsed.error)
        })

        it("an unknown prefix is treated as part of the tool name", async () => {
            const result = await client.callTool({
                name: "mcp_get_tool_schema",
                arguments: { tool_name: "nosuchserver:spec_echo" }
            })
            const parsed = JSON.parse((result.content as Array<{ type: string; text: string }>)[0].text)
            assert.ok(parsed.error.includes("nosuchserver:spec_echo"), parsed.error)
            assert.ok(parsed.error.includes("<server>:<tool>"), parsed.error)
        })

        it("spec_echo is callable via mcp_call_tool", async () => {
            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "spec_echo", arguments: { message: "hello world" } }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.equal(text, "hello world")
        })

        it("spec_annotated schema contains annotations", async () => {
            const result = await client.callTool({
                name: "mcp_get_tool_schema",
                arguments: { tool_name: "spec_annotated" }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            const schema = JSON.parse(text)
            assert.equal(schema.title, "Annotated Tool")
            assert.ok(schema.annotations)
            assert.equal(schema.annotations.readOnlyHint, true)
            assert.equal(schema.annotations.destructiveHint, false)
        })

        it("spec_structured returns structuredContent", async () => {
            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "spec_structured", arguments: { value: "test" } }
            })
            assert.ok(result.structuredContent, "should have structuredContent")
            const structured = result.structuredContent as Record<string, unknown>
            assert.equal(structured.result, "TEST")
            assert.equal(structured.processed, true)
        })

        it("spec_error returns isError: true", async () => {
            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "spec_error", arguments: { reason: "test failure" } }
            })
            assert.equal(result.isError, true)
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.equal(text, "test failure")
        })
    })

    /* ── B. Resource Proxying ───────────────────────── */

    describe("B. Resource proxying", () => {
        it("resources/list includes backend resources", async () => {
            const result = await client.listResources()
            assert.ok(result.resources.length >= 2, "should have at least 2 resources")
            const uris = result.resources.map(r => r.uri)
            assert.ok(uris.includes("spec://static/readme"), "should include spec://static/readme")
            assert.ok(uris.includes("spec://static/data"), "should include spec://static/data")
        })

        it("resources/read returns static resource content", async () => {
            const result = await client.readResource({ uri: "spec://static/readme" })
            assert.ok(result.contents.length > 0)
            const content = result.contents[0]
            const text = "text" in content ? content.text : undefined
            assert.ok(typeof text === "string")
            assert.ok(text.includes("spec test server README"))
        })

        it("resources/templates/list includes template", async () => {
            const result = await client.listResourceTemplates()
            assert.ok(result.resourceTemplates.length >= 1, "should have at least 1 template")
            const template = result.resourceTemplates.find(t => t.uriTemplate.includes("spec://template/"))
            assert.ok(template, "should have spec://template/{id} template")
        })

        it("completion/complete for template parameter returns values", async () => {
            const result = await client.complete({
                ref: { type: "ref/resource", uri: "spec://template/{id}" },
                argument: { name: "id", value: "" }
            })
            assert.ok(result.completion.values.length > 0, "should return completion values")
            assert.ok(result.completion.values.includes("alpha"))
        })
    })

    /* ── C. Prompt Proxying ─────────────────────────── */

    describe("C. Prompt proxying", () => {
        it("prompts/list includes backend prompts with title", async () => {
            const result = await client.listPrompts()
            assert.ok(result.prompts.length >= 3, "should have at least 3 prompts")
            const simple = result.prompts.find(p => p.name === "spec_simple")
            assert.ok(simple, "should find spec_simple")
        })

        it("prompts/get returns prompt messages", async () => {
            const result = await client.getPrompt({ name: "spec_with_args", arguments: { language: "TypeScript", topic: "testing" } })
            assert.ok(result.messages.length > 0)
            const text = result.messages[0].content as { type: string; text: string }
            assert.ok(text.text.includes("testing"))
            assert.ok(text.text.includes("TypeScript"))
        })

        it("prompts/get for multi_message returns multiple messages", async () => {
            const result = await client.getPrompt({ name: "spec_multi_message" })
            assert.ok(result.messages.length >= 2, "should have at least 2 messages")
        })
    })

    /* ── D. Notification Forwarding ─────────────────── */

    describe("D. Notification forwarding", () => {
        it("spec_logging sends logging messages through funnel", async () => {
            /* Set log level to debug first to receive all levels */
            await client.setLoggingLevel("debug")

            /* Call the logging tool */
            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "spec_logging", arguments: {} }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.ok(text.includes("Sent logging messages"))
        })
    })

    /* ── E. Bidirectional Features ──────────────────── */

    describe("E. Bidirectional features", () => {
        it("spec_sampling triggers sampling through funnel", async () => {
            /* Register sampling handler on client */
            client.setRequestHandler(CreateMessageRequestSchema, async () => {
                return {
                    model: "test-model",
                    role: "assistant" as const,
                    content: { type: "text" as const, text: "Sampling response from client" }
                }
            })

            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "spec_sampling", arguments: { prompt: "test prompt" } }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.ok(text.includes("Sampling response"), `Expected sampling response, got: ${text}`)
        })

        it("spec_elicitation triggers elicitation through funnel", async () => {
            /* Register elicitation handler on client */
            client.setRequestHandler(ElicitRequestSchema, async () => {
                return {
                    action: "accept" as const,
                    content: { answer: "42" }
                }
            })

            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "spec_elicitation", arguments: { question: "What is the answer?" } }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.ok(text.includes("Elicitation response"), `Expected elicitation response, got: ${text}`)
        })

        it("spec_roots triggers roots list through funnel", async () => {
            /* Register roots handler on client */
            client.setRequestHandler(ListRootsRequestSchema, async () => {
                return {
                    roots: [{ uri: "file:///test/root", name: "Test Root" }]
                }
            })

            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "spec_roots", arguments: {} }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.ok(text.includes("Roots"), `Expected roots response, got: ${text}`)
            assert.ok(text.includes("Test Root"), `Expected Test Root in response, got: ${text}`)
        })
    })

    /* ── F. OAuth ───────────────────────────────────── */

    describe("F. OAuth endpoints", () => {
        it("protected resource metadata is accessible", async () => {
            const res = await request(`${funnelBaseUrl}/.well-known/oauth-protected-resource`)
            assert.equal(res.status, 200)
            const data = JSON.parse(res.body)
            assert.ok(data.resource)
        })

        it("authorization server metadata is accessible", async () => {
            const res = await request(`${funnelBaseUrl}/.well-known/oauth-authorization-server`)
            assert.equal(res.status, 200)
            const data = JSON.parse(res.body)
            assert.ok(data.issuer)
            assert.ok(data.token_endpoint)
            assert.ok(data.authorization_endpoint)
        })

        it("dynamic client registration works", async () => {
            const res = await request(`${funnelBaseUrl}/oauth/register`, {
                method: "POST",
                body: {
                    client_name: "test-client",
                    redirect_uris: ["http://localhost:9999/callback"],
                    grant_types: ["authorization_code"],
                    response_types: ["code"],
                    token_endpoint_auth_method: "none"
                }
            })
            assert.equal(res.status, 201, `Registration failed: ${res.body}`)
            const data = JSON.parse(res.body)
            assert.ok(data.client_id)
        })
    })

    /* ── F2. OAuth Client Credentials ──────────────── */

    describe("F2. OAuth client_credentials", () => {
        let ccClientId: string
        let ccClientSecret: string

        it("register client with client_credentials grant", async () => {
            const res = await request(`${funnelBaseUrl}/oauth/register`, {
                method: "POST",
                body: {
                    client_name: "m2m-test-client",
                    redirect_uris: ["http://localhost:9999/callback"],
                    grant_types: ["client_credentials"],
                    response_types: ["code"],
                    token_endpoint_auth_method: "none"
                }
            })
            assert.equal(res.status, 201)
            const data = JSON.parse(res.body)
            ccClientId = data.client_id
            ccClientSecret = data.client_secret
            assert.ok(ccClientId)
            assert.ok(ccClientSecret)
        })

        it("client_credentials grant returns access token", async () => {
            const res = await request(`${funnelBaseUrl}/oauth/token`, {
                method: "POST",
                body: {
                    grant_type: "client_credentials",
                    client_id: ccClientId,
                    client_secret: ccClientSecret
                }
            })
            assert.equal(res.status, 200, `Token request failed: ${res.body}`)
            const data = JSON.parse(res.body)
            assert.ok(data.access_token)
            assert.equal(data.token_type, "Bearer")
            assert.ok(data.expires_in > 0)
        })

        it("client_credentials rejects wrong secret", async () => {
            const res = await request(`${funnelBaseUrl}/oauth/token`, {
                method: "POST",
                body: {
                    grant_type: "client_credentials",
                    client_id: ccClientId,
                    client_secret: "wrong-secret"
                }
            })
            assert.equal(res.status, 401)
            const data = JSON.parse(res.body)
            assert.equal(data.error, "invalid_client")
        })

        it("client_credentials rejects missing secret", async () => {
            const res = await request(`${funnelBaseUrl}/oauth/token`, {
                method: "POST",
                body: {
                    grant_type: "client_credentials",
                    client_id: ccClientId
                }
            })
            assert.equal(res.status, 400)
        })

        it("client_credentials rejects unauthorized grant type", async () => {
            /* Register a client without client_credentials grant */
            const regRes = await request(`${funnelBaseUrl}/oauth/register`, {
                method: "POST",
                body: {
                    client_name: "no-cc-client",
                    redirect_uris: ["http://localhost:9999/callback"],
                    grant_types: ["authorization_code"],
                    response_types: ["code"]
                }
            })
            const regData = JSON.parse(regRes.body)

            const res = await request(`${funnelBaseUrl}/oauth/token`, {
                method: "POST",
                body: {
                    grant_type: "client_credentials",
                    client_id: regData.client_id,
                    client_secret: regData.client_secret
                }
            })
            assert.equal(res.status, 400)
            const data = JSON.parse(res.body)
            assert.equal(data.error, "unauthorized_client")
        })

        it("metadata advertises client_credentials grant type", async () => {
            const res = await request(`${funnelBaseUrl}/.well-known/oauth-authorization-server`)
            const data = JSON.parse(res.body)
            assert.ok(data.grant_types_supported.includes("client_credentials"))
        })
    })

    /* ── F3. Backend OAuth Routes ───────────────────── */

    describe("F3. Backend OAuth routes", () => {
        /* These tests use the funnel's web session (single-user mode bypasses auth) */
        let serverId: string

        before(async () => {
            /* Get the backend server ID from the funnel */
            const dashRes = await request(`${funnelBaseUrl}/dashboard`)
            const cookies = dashRes.headers["set-cookie"]
            const cookie = cookies ? (Array.isArray(cookies) ? cookies[0] : cookies).split(";")[0] : ""

            const listRes = await request(`${funnelBaseUrl}/mcp-servers/api/list`, {
                headers: cookie ? { Cookie: cookie } : {}
            })
            const listData = JSON.parse(listRes.body)
            serverId = listData.servers[0]?.id
            assert.ok(serverId, "should have at least one server")
        })

        it("oauth/status returns not configured for server without OAuth", async () => {
            const dashRes = await request(`${funnelBaseUrl}/dashboard`)
            const cookies = dashRes.headers["set-cookie"]
            const cookie = cookies ? (Array.isArray(cookies) ? cookies[0] : cookies).split(";")[0] : ""

            const res = await request(`${funnelBaseUrl}/mcp-servers/api/${serverId}/oauth/status`, {
                headers: cookie ? { Cookie: cookie } : {}
            })
            assert.equal(res.status, 200)
            const data = JSON.parse(res.body)
            assert.equal(data.configured, false)
        })

        it("oauth/discover against spec test backend discovers metadata", async () => {
            const dashRes = await request(`${funnelBaseUrl}/dashboard`)
            const cookies = dashRes.headers["set-cookie"]
            const cookie = cookies ? (Array.isArray(cookies) ? cookies[0] : cookies).split(";")[0] : ""

            /* The spec test backend doesn't have OAuth endpoints, so this should fail gracefully */
            const res = await request(`${funnelBaseUrl}/mcp-servers/api/${serverId}/oauth/discover`, {
                method: "POST",
                body: { serverUrl: backendUrl.replace("/mcp", "") },
                headers: cookie ? { Cookie: cookie } : {}
            })
            /* Should return 400 because spec test server has no .well-known endpoints */
            assert.equal(res.status, 400)
            const data = JSON.parse(res.body)
            assert.ok(data.error.includes("No OAuth metadata found"))
        })

        it("oauth/client saves manual client credentials", async () => {
            const dashRes = await request(`${funnelBaseUrl}/dashboard`)
            const cookies = dashRes.headers["set-cookie"]
            const cookie = cookies ? (Array.isArray(cookies) ? cookies[0] : cookies).split(";")[0] : ""

            /* First do a discover against a server that has no registration endpoint
             * We can't self-discover in tests (issuer URL mismatch), so we'll
             * manually set up OAuth config via the client endpoint instead.
             * First, set up a minimal OAuth config via discover (which will fail on registration) */

            /* Use the oauth/client endpoint to manually configure */
            /* But it requires discovery first — so we test the "no registration" path:
             * discover against spec backend fails, then we can test remove */

            /* Instead, test the manual client flow by POSTing directly */
            const clientRes = await request(`${funnelBaseUrl}/mcp-servers/api/${serverId}/oauth/client`, {
                method: "POST",
                body: { clientId: "test-client-id", clientSecret: "test-secret", scope: "mcp:full" },
                headers: cookie ? { Cookie: cookie } : {}
            })
            /* Should fail because no OAuth config (no prior discovery) */
            assert.equal(clientRes.status, 400)
            const data = JSON.parse(clientRes.body)
            assert.ok(data.error.includes("Run OAuth discovery first"))
        })

        it("oauth/remove succeeds even without OAuth config", async () => {
            const dashRes = await request(`${funnelBaseUrl}/dashboard`)
            const cookies = dashRes.headers["set-cookie"]
            const cookie = cookies ? (Array.isArray(cookies) ? cookies[0] : cookies).split(";")[0] : ""

            const delRes = await request(`${funnelBaseUrl}/mcp-servers/api/${serverId}/oauth`, {
                method: "DELETE",
                headers: cookie ? { Cookie: cookie } : {}
            })
            assert.equal(delRes.status, 200)
            const delData = JSON.parse(delRes.body)
            assert.equal(delData.success, true)

            /* Verify status shows not configured */
            const statusRes = await request(`${funnelBaseUrl}/mcp-servers/api/${serverId}/oauth/status`, {
                headers: cookie ? { Cookie: cookie } : {}
            })
            const statusData = JSON.parse(statusRes.body)
            assert.equal(statusData.configured, false)
        })

        it("oauth/discover rejects missing serverUrl", async () => {
            const dashRes = await request(`${funnelBaseUrl}/dashboard`)
            const cookies = dashRes.headers["set-cookie"]
            const cookie = cookies ? (Array.isArray(cookies) ? cookies[0] : cookies).split(";")[0] : ""

            const res = await request(`${funnelBaseUrl}/mcp-servers/api/${serverId}/oauth/discover`, {
                method: "POST",
                body: {},
                headers: cookie ? { Cookie: cookie } : {}
            })
            assert.equal(res.status, 400)
        })
    })

    /* ── G. Protocol validation ─────────────────────── */

    describe("G. Protocol validation", () => {
        it("initialize reports correct capabilities", async () => {
            const res = await request(`${funnelBaseUrl}/mcp`, {
                method: "POST",
                body: {
                    jsonrpc: "2.0", id: 1, method: "initialize",
                    params: { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "test", version: "1.0" } }
                },
                headers: { Accept: "application/json, text/event-stream" }
            })
            assert.equal(res.status, 200)
            const data = JSON.parse(res.body)
            assert.ok(data.result.capabilities.tools.listChanged)
            assert.ok(data.result.capabilities.prompts.listChanged)
            assert.ok(data.result.capabilities.resources.listChanged)
            assert.ok(data.result.capabilities.resources.subscribe)
        })
    })
})

/* ── H. Disabled tools ──────────────────────────────────
 * Guards the pre-existing split: getToolSchema hides disabled tools, while callTool still
 * reaches them. A resolver that unified both tool sets would break one of the two. */

describe("MCP proxy with a disabled tool", () => {
    let tmpDir: string
    let backendShutdown: () => Promise<void>
    let funnelShutdown: () => Promise<void>
    let client: Client

    before(async () => {
        const backend = await createSpecServer(0)
        backendShutdown = backend.shutdown

        tmpDir = makeTmpDir()
        writeServerConfig(tmpDir, backend.url, ["spec_echo"])

        const funnel = await startFunnel(tmpDir)
        funnelShutdown = funnel.shutdown

        client = await createTestClient(funnel.baseUrl)
        await new Promise(r => setTimeout(r, 3000))
    })

    after(async () => {
        try { await (client as unknown as { close: () => Promise<void> }).close() } catch { /* ignore */ }
        await funnelShutdown()
        await backendShutdown()
        fs.rmSync(tmpDir, { recursive: true, force: true })
    })

    it("hides a disabled tool from discovery", async () => {
        const result = await client.callTool({
            name: "mcp_discover_tools",
            arguments: { words: "spec echo" }
        })
        const text = (result.content as Array<{ type: string; text: string }>)[0].text
        assert.ok(!text.includes("spec_echo ["), `spec_echo should be hidden, got:\n${text}`)
    })

    it("hides a disabled tool from mcp_get_tool_schema", async () => {
        const result = await client.callTool({
            name: "mcp_get_tool_schema",
            arguments: { tool_name: "spec_echo" }
        })
        const parsed = JSON.parse((result.content as Array<{ type: string; text: string }>)[0].text)
        assert.ok(parsed.error, "disabled tool must not expose a schema")
    })

    it("still calls a disabled tool (pre-existing behaviour)", async () => {
        const result = await client.callTool({
            name: "mcp_call_tool",
            arguments: { tool_name: "spec_echo", arguments: { message: "still reachable" } }
        })
        const text = (result.content as Array<{ type: string; text: string }>)[0].text
        assert.equal(text, "still reachable")
    })
})

/* -- I. Two backends with colliding tool names ------------
 * The scenario the qualified-name syntax exists for: the same tool name served by two
 * backends. Unqualified lookups hit whichever connected first; qualified ones must reach
 * exactly the addressed server. */

describe("MCP proxy with two backends exposing the same tool", () => {
    let tmpDir: string
    let shutdownA: () => Promise<void>
    let shutdownB: () => Promise<void>
    let funnelShutdown: () => Promise<void>
    let client: Client

    before(async () => {
        const backendA = await createSpecServer(0)
        const backendB = await createSpecServer(0)
        shutdownA = backendA.shutdown
        shutdownB = backendB.shutdown

        tmpDir = makeTmpDir()
        writeServersConfig(tmpDir, [
            { id: "backend-a", name: "alpha", url: backendA.url },
            { id: "backend-b", name: "beta", url: backendB.url }
        ])

        const funnel = await startFunnel(tmpDir)
        funnelShutdown = funnel.shutdown

        client = await createTestClient(funnel.baseUrl)
        await new Promise(r => setTimeout(r, 3000))
    })

    after(async () => {
        try { await (client as unknown as { close: () => Promise<void> }).close() } catch { /* ignore */ }
        await funnelShutdown()
        await shutdownA()
        await shutdownB()
        fs.rmSync(tmpDir, { recursive: true, force: true })
    })

    it("discovery lists the shared tool once per server", async () => {
        const result = await client.callTool({
            name: "mcp_discover_tools",
            arguments: { words: "spec echo", limit: 50 }
        })
        const text = (result.content as Array<{ type: string; text: string }>)[0].text
        assert.ok(text.includes("spec_echo [alpha]:"), `missing alpha entry in:\n${text}`)
        assert.ok(text.includes("spec_echo [beta]:"), `missing beta entry in:\n${text}`)
    })

    it("each qualified name reaches its own backend", async () => {
        for (const server of ["alpha", "beta"]) {
            const result = await client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: `${server}:spec_echo`, arguments: { message: server } }
            })
            const text = (result.content as Array<{ type: string; text: string }>)[0].text
            assert.equal(text, server)
        }
    })

    it("a qualified schema lookup reports the addressed server", async () => {
        const result = await client.callTool({
            name: "mcp_get_tool_schema",
            arguments: { tool_name: "beta:spec_annotated" }
        })
        const schema = JSON.parse((result.content as Array<{ type: string; text: string }>)[0].text)
        assert.equal(schema.server, "beta")
    })

    it("an unqualified name still resolves to a single backend", async () => {
        const result = await client.callTool({
            name: "mcp_call_tool",
            arguments: { tool_name: "spec_echo", arguments: { message: "unqualified" } }
        })
        const text = (result.content as Array<{ type: string; text: string }>)[0].text
        assert.equal(text, "unqualified")
    })

    it("calling a known server that lacks the tool names that server", async () => {
        await assert.rejects(
            () => client.callTool({
                name: "mcp_call_tool",
                arguments: { tool_name: "alpha:does_not_exist", arguments: {} }
            }),
            (err: Error) => err.message.includes("alpha") && err.message.includes("does_not_exist")
        )
    })
})
