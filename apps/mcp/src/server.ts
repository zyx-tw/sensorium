import { createMcpHandler, McpServer } from "@modelcontextprotocol/server";
import type { Pool } from "@sensorium/db";
import { createMcpAuth, McpAuthError, type McpAuthConfig } from "./auth.js";
import { registerTools } from "./tools.js";

export const SERVER_INFO = { name: "sensorium-mcp", version: "0.0.0" };

function buildServer(pool: Pool): McpServer {
  const server = new McpServer(SERVER_INFO);
  registerTools(server, pool);
  return server;
}

/**
 * Builds the MCP endpoint as a Web Standard fetch handler (works with Bun.serve directly).
 * Stateless: the SDK's handler builds a fresh McpServer per request, which suits read-only
 * tools, and it serves both the 2025 and the 2026-07-28 protocol revisions.
 */
export function createMcpApp(pool: Pool, auth: McpAuthConfig | ReturnType<typeof createMcpAuth>) {
  const gate = "authenticate" in auth ? auth : createMcpAuth(auth);
  const handler = createMcpHandler(() => buildServer(pool), {
    onerror: (err) => console.error("mcp:", err.message),
  });

  async function fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);

    if (req.method === "GET" && url.pathname === "/health") {
      return Response.json({ status: "ok" });
    }

    const metadata = gate.metadataResponse(req, url);
    if (metadata) return metadata;

    if (url.pathname !== "/mcp") {
      return Response.json({ error: "not found" }, { status: 404 });
    }

    try {
      await gate.authenticate(req);
    } catch (err) {
      if (err instanceof McpAuthError) return gate.refusal(err);
      throw err;
    }
    return handler.fetch(req);
  }

  return { fetch };
}
