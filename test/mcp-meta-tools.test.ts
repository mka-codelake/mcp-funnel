// Tests for mcp-meta-tools: scoring, fuzzy-matching, dynamic tool count

import { describe, it } from "node:test"
import assert from "node:assert/strict"

import {
    levenshtein, tokenize, fuzzyThreshold, fuzzyMatchTokens,
    matchesWordBoundary, scoreKeyword, scoreTool, searchTools, getMetaTools,
    splitQualifiedToolName, resolveToolName
} from "../src/mcp-meta-tools.js"
import type { ToolWithServer } from "../src/mcp-meta-tools.js"

/* ── helpers ────────────────────────────────────────── */

function makeTool (name: string, description = "", serverName = "TestServer"): ToolWithServer {
    return {
        name,
        description,
        inputSchema: {},
        _serverId: "srv-1",
        _serverName: serverName
    }
}

/* ── levenshtein ───────────────────────────────────── */

describe("levenshtein", () => {
    it("returns 0 for identical strings", () => {
        assert.equal(levenshtein("abc", "abc"), 0)
    })

    it("returns correct distance for 1 edit", () => {
        assert.equal(levenshtein("cat", "car"), 1)
    })

    it("is symmetric", () => {
        assert.equal(levenshtein("kitten", "sitting"), levenshtein("sitting", "kitten"))
    })

    it("handles empty string vs non-empty", () => {
        assert.equal(levenshtein("", "abc"), 3)
        assert.equal(levenshtein("abc", ""), 3)
    })

    it("returns 0 for two empty strings", () => {
        assert.equal(levenshtein("", ""), 0)
    })
})

/* ── tokenize ──────────────────────────────────────── */

describe("tokenize", () => {
    it("splits on underscores", () => {
        assert.deepEqual(tokenize("list_issues"), ["list", "issues"])
    })

    it("splits on dashes", () => {
        assert.deepEqual(tokenize("get-user"), ["get", "user"])
    })

    it("splits on mixed separators", () => {
        assert.deepEqual(tokenize("get_user-info.detail extra"), ["get", "user", "info", "detail", "extra"])
    })

    it("returns empty array for empty string", () => {
        assert.deepEqual(tokenize(""), [])
    })
})

/* ── fuzzyThreshold ────────────────────────────────── */

describe("fuzzyThreshold", () => {
    it("returns 0 for short keywords (1-3 chars)", () => {
        assert.equal(fuzzyThreshold("abc"), 0)
        assert.equal(fuzzyThreshold("a"), 0)
    })

    it("returns 1 for medium keywords (4-7 chars)", () => {
        assert.equal(fuzzyThreshold("list"), 1)
        assert.equal(fuzzyThreshold("searchi"), 1)
    })

    it("returns 2 for long keywords (8+ chars)", () => {
        assert.equal(fuzzyThreshold("repositor"), 2)
        assert.equal(fuzzyThreshold("something"), 2)
    })
})

/* ── matchesWordBoundary ───────────────────────────── */

describe("matchesWordBoundary", () => {
    it("matches at start of string", () => {
        assert.equal(matchesWordBoundary("list_issues", "list"), true)
    })

    it("matches after underscore", () => {
        assert.equal(matchesWordBoundary("list_issues", "issues"), true)
    })

    it("matches after dash", () => {
        assert.equal(matchesWordBoundary("get-user", "user"), true)
    })

    it("does not match mid-word", () => {
        assert.equal(matchesWordBoundary("repository", "pos"), false)
    })
})

/* ── scoreKeyword ──────────────────────────────────── */

describe("scoreKeyword", () => {
    it("returns 100 for exact name match", () => {
        const score = scoreKeyword("get_me", "get_me", "some desc", "server", ["get", "me"], ["some", "desc"])
        assert.equal(score, 100)
    })

    it("returns 50 for word boundary match in name", () => {
        const score = scoreKeyword("list", "list_issues", "desc", "server", ["list", "issues"], ["desc"])
        assert.equal(score, 50)
    })

    it("returns 30 for substring match in name (no word boundary)", () => {
        const score = scoreKeyword("ssue", "list_issues", "desc", "server", ["list", "issues"], ["desc"])
        assert.equal(score, 30)
    })

    it("returns 10 for word boundary in description", () => {
        const score = scoreKeyword("search", "other_tool", "search for items", "server", ["other", "tool"], ["search", "for", "items"])
        assert.equal(score, 10)
    })

    it("returns 15 for fuzzy match in name", () => {
        /* "crete" is 1 edit from "create" */
        const score = scoreKeyword("crete", "create_item", "desc", "server", ["create", "item"], ["desc"])
        assert.equal(score, 15)
    })

    it("returns 0 for no match", () => {
        const score = scoreKeyword("xyz", "create_item", "make something", "server", ["create", "item"], ["make", "something"])
        assert.equal(score, 0)
    })
})

/* ── scoreTool ─────────────────────────────────────── */

describe("scoreTool", () => {
    it("returns 0 in AND mode when one keyword has no match", () => {
        const tool = makeTool("list_issues", "List all issues")
        const score = scoreTool(tool, ["list", "zzzzz"], "and")
        assert.equal(score, 0)
    })

    it("sums scores in AND mode when all keywords match", () => {
        const tool = makeTool("list_issues", "List all issues")
        const score = scoreTool(tool, ["list", "issues"], "and")
        assert.ok(score > 0)
        /* "list" = word boundary in name (50), "issues" = word boundary in name (50) */
        assert.equal(score, 100)
    })

    it("matches in OR mode when at least one keyword hits", () => {
        const tool = makeTool("list_issues", "List all issues")
        const score = scoreTool(tool, ["list", "zzzzz"], "or")
        assert.ok(score > 0)
    })

    it("returns 0 in OR mode when no keywords match", () => {
        const tool = makeTool("list_issues", "List all issues")
        const score = scoreTool(tool, ["xxx", "yyy"], "or")
        assert.equal(score, 0)
    })
})

/* ── searchTools ───────────────────────────────────── */

describe("searchTools", () => {
    const tools: ToolWithServer[] = [
        makeTool("create_issue", "Create a new issue", "GitHub"),
        makeTool("list_issues", "List all issues", "GitHub"),
        makeTool("search_repos", "Search repositories", "GitHub"),
        makeTool("get_user", "Get user profile", "Slack")
    ]

    it("returns all tools (up to limit) when keywords are empty", () => {
        const results = searchTools(tools, "")
        assert.equal(results.length, 4)
    })

    it("ranks exact and word-boundary matches higher", () => {
        const results = searchTools(tools, "list")
        assert.ok(results.length > 0)
        assert.equal(results[0].name, "list_issues")
    })

    it("finds fuzzy matches (typo in keyword)", () => {
        /* "serch" is 1 edit from "search" */
        const results = searchTools(tools, "serch")
        assert.ok(results.length > 0)
        assert.ok(results.some((r) => r.name === "search_repos"))
    })

    it("respects limit parameter", () => {
        const results = searchTools(tools, { or: ["issue", "search", "user"] }, 2)
        assert.equal(results.length, 2)
    })

    it("includes score in results", () => {
        const results = searchTools(tools, "list")
        assert.ok(results.length > 0)
        assert.ok(typeof results[0].score === "number")
        assert.ok(results[0].score! > 0)
    })

    it("returns results sorted by score descending", () => {
        const results = searchTools(tools, { or: ["create", "issue"] })
        for (let i = 1; i < results.length; i++) {
            assert.ok(results[i - 1].score! >= results[i].score!)
        }
    })
})

/* ── getMetaTools ──────────────────────────────────── */

describe("getMetaTools", () => {
    it("returns META_TOOLS unchanged when called without count", () => {
        const tools = getMetaTools()
        const discover = tools.find((t) => t.name === "mcp_discover_tools")!
        assert.ok(!discover.description.includes("Search across"))
    })

    it("injects tool count into discover description", () => {
        const tools = getMetaTools(42)
        const discover = tools.find((t) => t.name === "mcp_discover_tools")!
        assert.ok(discover.description.includes("Search across 42 available tools"))
    })

    it("does not change other tool descriptions", () => {
        const tools = getMetaTools(42)
        const schema = tools.find((t) => t.name === "mcp_get_tool_schema")!
        assert.ok(!schema.description.includes("42"))
    })
})

/* -- splitQualifiedToolName ------------------------------ */

describe("splitQualifiedToolName", () => {
    it("U1: splits a qualified name at the colon", () => {
        assert.deepEqual(splitQualifiedToolName("gitlab:list_branches"),
            { serverName: "gitlab", bareName: "list_branches" })
    })

    it("U2: leaves an unqualified name untouched", () => {
        assert.deepEqual(splitQualifiedToolName("list_branches"), { bareName: "list_branches" })
    })

    it("U3: splits at the FIRST colon only", () => {
        assert.deepEqual(splitQualifiedToolName("srv:a:b"), { serverName: "srv", bareName: "a:b" })
    })

    it("U4: treats a leading colon as part of the tool name", () => {
        assert.deepEqual(splitQualifiedToolName(":foo"), { bareName: ":foo" })
    })
})

/* -- resolveToolName ------------------------------------- */

describe("resolveToolName", () => {
    const gitlabBranches = makeTool("list_branches", "gitlab variant", "gitlab")
    const sonarBranches = makeTool("list_branches", "sonar variant", "sonarqube")
    const tools = [gitlabBranches, sonarBranches]
    const known = ["gitlab", "sonarqube"]

    it("U5: a qualified name selects the tool of that server", () => {
        const viaSonar = resolveToolName(tools, "sonarqube:list_branches", known)
        assert.equal(viaSonar.status, "found")
        assert.equal(viaSonar.status === "found" && viaSonar.tool.description, "sonar variant")

        const viaGitlab = resolveToolName(tools, "gitlab:list_branches", known)
        assert.equal(viaGitlab.status === "found" && viaGitlab.tool.description, "gitlab variant")
    })

    it("U6: an unqualified name keeps the legacy first-match behaviour", () => {
        const res = resolveToolName(tools, "list_branches", known)
        assert.equal(res.status, "found")
        assert.equal(res.status === "found" && res.tool.description, "gitlab variant")
    })

    it("U7: an unknown prefix falls back to a lookup of the full string", () => {
        const odd = makeTool("weird:name", "colon in its own name", "gitlab")
        const res = resolveToolName([...tools, odd], "weird:name", known)
        assert.equal(res.status, "found")
        assert.equal(res.status === "found" && res.tool.description, "colon in its own name")
    })

    it("U8: a known server without that tool reports server-known-tool-missing", () => {
        const res = resolveToolName(tools, "gitlab:show_rule", known)
        assert.equal(res.status, "server-known-tool-missing")
        assert.equal(res.status === "server-known-tool-missing" && res.serverName, "gitlab")
        assert.equal(res.status === "server-known-tool-missing" && res.bareName, "show_rule")
    })

    it("U9: server matching is case-sensitive", () => {
        const res = resolveToolName(tools, "GitLab:list_branches", ["gitlab"])
        assert.equal(res.status, "not-found")
    })

    it("U12: a tool whose own name looks qualified stays reachable", () => {
        /* "hafas" is a real server here, so "hafas:search" reads as qualified first.
         * No server "hafas" offers "search", but gitlab offers a tool literally named
         * "hafas:search" - that must still win over reporting it as missing. */
        const odd = makeTool("hafas:search", "literal colon name", "gitlab")
        const res = resolveToolName([...tools, odd], "hafas:search", ["gitlab", "hafas"])
        assert.equal(res.status, "found")
        assert.equal(res.status === "found" && res.tool.description, "literal colon name")
    })

    it("U13: the qualified reading takes precedence over the literal one", () => {
        const literal = makeTool("sonarqube:list_branches", "literal", "gitlab")
        const res = resolveToolName([...tools, literal], "sonarqube:list_branches", known)
        assert.equal(res.status, "found")
        assert.equal(res.status === "found" && res.tool.description, "sonar variant")
    })
})
