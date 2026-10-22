/* stdio backend for connection recovery tests: a process that can be told to exit, as a
 * backend that crashes or is updated. Started by the funnel itself. */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js"
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js"

const server = new McpServer({ name: "stdio-test-backend", version: "1.0.0" })

server.registerTool("spec_pid", {
    description: "Returns the process id of this backend",
    annotations: { readOnlyHint: true }
}, async () => {
    return { content: [{ type: "text", text: String(process.pid) }] }
})

server.registerTool("spec_exit", {
    description: "Answers, then ends the process"
}, async () => {
    setTimeout(() => process.exit(0), 50)
    return { content: [{ type: "text", text: "bye" }] }
})

server.connect(new StdioServerTransport()).catch((error) => {
    console.error(error)
    process.exit(1)
})
