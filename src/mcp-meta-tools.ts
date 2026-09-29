/* MCP-Funnel — Multi-user MCP server management
 * Copyright (c) 2026 Matthias Brusdeylins
 * SPDX-License-Identifier: GPL-3.0-only
 * 100% AI-generated code (agentic coding with Claude Code) */

type SearchWords = string | { and: string[] } | { or: string[] }

interface ToolWithServer {
    name: string
    description: string
    inputSchema: unknown
    title?: string
    icons?: unknown[]
    annotations?: Record<string, unknown>
    outputSchema?: unknown
    execution?: Record<string, unknown>
    _serverId: string
    _serverName: string
    [key: string]: unknown
}

interface ToolSearchResult {
    name: string
    description: string
    server: string
    score?: number
}

/* Outcome of resolving a (possibly server-qualified) tool name. */
type ToolResolution<T> =
    | { status: "found", tool: T }
    | { status: "server-known-tool-missing", serverName: string, bareName: string }
    | { status: "not-found", toolName: string }

interface MetaTool {
    name: string
    description: string
    inputSchema: Record<string, unknown>
}

const DISCOVER_DESCRIPTION_SUFFIX = "keywords in their names and descriptions. IMPORTANT: Use minimal, specific keywords that directly match the user's intent. Each keyword will match tools containing that word - avoid generic terms unless explicitly needed."

const META_TOOLS: MetaTool[] = [
    {
        name: "mcp_discover_tools",
        description: `Search for tools by ${DISCOVER_DESCRIPTION_SUFFIX}`,
        inputSchema: {
            type: "object",
            properties: {
                words: {
                    oneOf: [
                        {
                            type: "string",
                            description: "Space-separated keywords (uses AND logic - tool must contain ALL keywords)"
                        },
                        {
                            type: "object",
                            properties: {
                                and: {
                                    type: "array",
                                    items: { type: "string" },
                                    description: "Tool must contain ALL of these keywords"
                                }
                            },
                            required: ["and"]
                        },
                        {
                            type: "object",
                            properties: {
                                or: {
                                    type: "array",
                                    items: { type: "string" },
                                    description: "Tool must contain ANY of these keywords"
                                }
                            },
                            required: ["or"]
                        }
                    ],
                    description: "Search keywords - string (AND logic), {and: [...]} (ALL keywords), or {or: [...]} (ANY keyword)"
                },
                limit: {
                    type: "number",
                    description: "Maximum number of results to return",
                    default: 10
                }
            },
            required: ["words"]
        }
    },
    {
        name: "mcp_get_tool_schema",
        description: "Retrieve the full input schema for a specific tool. Use this after discovering a tool to understand its required and optional parameters before calling it.",
        inputSchema: {
            type: "object",
            properties: {
                tool_name: {
                    type: "string",
                    description: "The exact tool name as returned by mcp_discover_tools. If two servers expose the same tool name, qualify it as \"<server>:<tool>\" using the server shown in brackets by mcp_discover_tools."
                }
            },
            required: ["tool_name"]
        }
    },
    {
        name: "mcp_call_tool",
        description: "Execute a tool dynamically. Always use mcp_get_tool_schema first to get the required arguments. Pass the exact tool name and arguments object.",
        inputSchema: {
            type: "object",
            properties: {
                tool_name: {
                    type: "string",
                    description: "The exact tool name to execute. If two servers expose the same tool name, qualify it as \"<server>:<tool>\" using the server shown in brackets by mcp_discover_tools."
                },
                arguments: {
                    type: "object",
                    description: "The arguments object matching the tool's input schema"
                }
            },
            required: ["tool_name", "arguments"]
        }
    }
]

function isMetaTool (toolName: string): boolean {
    return META_TOOLS.some((t) => t.name === toolName)
}

function getMetaTools (toolCount?: number): MetaTool[] {
    if (toolCount === undefined || toolCount === 0) return META_TOOLS
    return META_TOOLS.map((tool) => {
        if (tool.name === "mcp_discover_tools") {
            return {
                ...tool,
                description: `Search across ${toolCount} available tools by ${DISCOVER_DESCRIPTION_SUFFIX}`
            }
        }
        return tool
    })
}

function parseWordsParameter (words: SearchWords): { keywords: string[], mode: "and" | "or" } {
    let keywords: string[] = []
    let mode: "and" | "or" = "and"

    if (typeof words === "string") {
        keywords = words.toLowerCase().split(/\s+/).filter((w) => w.length > 0)
    }
    else if (typeof words === "object" && words !== null) {
        if ("and" in words && Array.isArray(words.and)) {
            keywords = words.and.map((k) => k.toLowerCase())
            mode = "and"
        }
        else if ("or" in words && Array.isArray(words.or)) {
            keywords = words.or.map((k) => k.toLowerCase())
            mode = "or"
        }
        else {
            throw new Error("Invalid words format - use string, {and: [...]}, or {or: [...]}")
        }
    }
    else {
        throw new Error("Missing or invalid \"words\" parameter")
    }

    return { keywords, mode }
}

/* Backward-compatible keyword matching (simple substring). */
function matchesKeywords (tool: ToolWithServer, keywords: string[], mode: "and" | "or" = "and"): boolean {
    const searchText = `${tool.name || ""} ${tool.description || ""} ${tool._serverName || ""}`.toLowerCase()

    if (mode === "or") {
        return keywords.some((kw) => searchText.includes(kw))
    }
    else {
        return keywords.every((kw) => searchText.includes(kw))
    }
}

/* ── Scoring helpers ──────────────────────────────────── */

function levenshtein (a: string, b: string): number {
    if (a === b) return 0
    if (a.length === 0) return b.length
    if (b.length === 0) return a.length

    let prev = new Array<number>(b.length + 1)
    let curr = new Array<number>(b.length + 1)

    for (let j = 0; j <= b.length; j++) prev[j] = j

    for (let i = 1; i <= a.length; i++) {
        curr[0] = i
        for (let j = 1; j <= b.length; j++) {
            const cost = a[i - 1] === b[j - 1] ? 0 : 1
            curr[j] = Math.min(
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost
            )
        }
        ;[prev, curr] = [curr, prev]
    }
    return prev[b.length]
}

function tokenize (text: string): string[] {
    return text.toLowerCase().split(/[_\-.\s]+/).filter((t) => t.length > 0)
}

function fuzzyThreshold (keyword: string): number {
    return Math.min(Math.floor(keyword.length / 4), 2)
}

function fuzzyMatchTokens (keyword: string, tokens: string[], threshold: number): boolean {
    if (threshold === 0) return false
    for (const token of tokens) {
        if (Math.abs(token.length - keyword.length) > threshold) continue
        const dist = levenshtein(keyword, token)
        if (dist > 0 && dist <= threshold) return true
    }
    return false
}

/* Expects both text and keyword to be already lowercased. */
function matchesWordBoundary (text: string, keyword: string): boolean {
    let idx = text.indexOf(keyword)
    while (idx !== -1) {
        if (idx === 0) return true
        const charBefore = text[idx - 1]
        if (charBefore === "_" || charBefore === "-" || charBefore === "." || charBefore === " ")
            return true
        idx = text.indexOf(keyword, idx + 1)
    }
    return false
}

/* Expects name, desc, server to be already lowercased. */
function scoreKeyword (
    kw: string,
    name: string,
    desc: string,
    server: string,
    nameTokens: string[],
    descTokens: string[]
): number {
    /* Exact name match */
    if (name === kw) return 100

    /* Word boundary in name */
    if (matchesWordBoundary(name, kw)) return 50

    /* Substring in name */
    if (name.includes(kw)) return 30

    /* Word boundary in description */
    if (matchesWordBoundary(desc, kw)) return 10

    /* Word boundary in server name */
    if (matchesWordBoundary(server, kw)) return 8

    /* Substring in description */
    if (desc.includes(kw)) return 5

    /* Substring in server name */
    if (server.includes(kw)) return 3

    /* Fuzzy matching */
    const threshold = fuzzyThreshold(kw)
    if (threshold > 0) {
        if (fuzzyMatchTokens(kw, nameTokens, threshold)) return 15
        if (fuzzyMatchTokens(kw, descTokens, threshold)) return 3
    }

    return 0
}

function scoreTool (tool: ToolWithServer, keywords: string[], mode: "and" | "or"): number {
    const name    = (tool.name || "").toLowerCase()
    const desc    = (tool.description || "").toLowerCase()
    const server  = (tool._serverName || "").toLowerCase()
    const nameTokens = tokenize(name)
    const descTokens = tokenize(desc)

    let total = 0
    for (const kw of keywords) {
        const kwScore = scoreKeyword(kw, name, desc, server, nameTokens, descTokens)
        if (mode === "and" && kwScore === 0) return 0
        total += kwScore
    }

    return total
}

/* ── Truncation ───────────────────────────────────────── */

function truncateDescription (description: string): string {
    if (!description) return ""
    if (description.length <= 100) return description

    const firstBreak = description.indexOf("\n", 100)
    if (firstBreak !== -1) {
        return description.substring(0, firstBreak)
    }

    if (description.length > 200) {
        return description.substring(0, 197) + "..."
    }
    return description
}

/* ── Qualified tool names ─────────────────────────────── */

/* Splits "<server>:<tool>" at the FIRST colon.
 * A leading colon or no colon at all means the whole string is the tool name. */
function splitQualifiedToolName (toolName: string): { serverName?: string, bareName: string } {
    const idx = toolName.indexOf(":")
    if (idx <= 0) return { bareName: toolName }
    return { serverName: toolName.slice(0, idx), bareName: toolName.slice(idx + 1) }
}

/* Resolves a tool name against the given list.
 * knownServerNames is passed in rather than derived from the tools, so that a configured but
 * currently disconnected server is still recognised as a known prefix. Matching is
 * case-sensitive.
 *
 * Nothing stops a backend from putting a colon into a tool name: only the three meta-tools are
 * exposed as function names, backend tool names travel as a plain string argument. The
 * qualified reading therefore wins, but a tool whose own name merely looks qualified stays
 * reachable through the literal fallback. */
function resolveToolName<T extends { name: string, _serverName: string }> (
    tools: T[],
    toolName: string,
    knownServerNames: readonly string[]
): ToolResolution<T> {
    const { serverName, bareName } = splitQualifiedToolName(toolName)

    if (serverName !== undefined && knownServerNames.includes(serverName)) {
        const qualified = tools.find((t) => t._serverName === serverName && t.name === bareName)
        if (qualified) return { status: "found", tool: qualified }

        const literal = tools.find((t) => t.name === toolName)
        if (literal) return { status: "found", tool: literal }

        return { status: "server-known-tool-missing", serverName, bareName }
    }

    const tool = tools.find((t) => t.name === toolName)
    if (tool) return { status: "found", tool }
    return { status: "not-found", toolName }
}

/* ── Search ───────────────────────────────────────────── */

function searchTools (allTools: ToolWithServer[], words: SearchWords, limit = 10): ToolSearchResult[] {
    const { keywords, mode } = parseWordsParameter(words)

    if (keywords.length === 0) {
        return allTools.slice(0, limit).map((t) => ({
            name: t.name,
            description: truncateDescription(t.description),
            server: t._serverName
        }))
    }

    const scored: Array<{ tool: ToolWithServer, score: number }> = []
    for (const tool of allTools) {
        const score = scoreTool(tool, keywords, mode)
        if (score > 0) scored.push({ tool, score })
    }

    scored.sort((a, b) => b.score - a.score)

    return scored.slice(0, limit).map((s) => ({
        name: s.tool.name,
        description: truncateDescription(s.tool.description),
        server: s.tool._serverName,
        score: s.score
    }))
}

export { META_TOOLS, isMetaTool, getMetaTools, searchTools, parseWordsParameter, matchesKeywords, splitQualifiedToolName, resolveToolName }
export { levenshtein, tokenize, fuzzyThreshold, fuzzyMatchTokens, matchesWordBoundary, scoreKeyword, scoreTool }
export type { SearchWords, ToolWithServer, ToolSearchResult, MetaTool, ToolResolution }
