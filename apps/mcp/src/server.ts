import { createMcpHandler, isLegacyRequest, McpServer, WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/server";
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
    legacy: "reject",
    onerror: (err) => console.error("mcp:", err.message),
  });

  /**
   * 2025 clients get one JSON body, as they did before SDK v2: the SDK's own
   * legacy path answers over SSE, which breaks callers that parse the body
   * as JSON (the hum band, deploy/smoke.sh).
   */
  async function legacy(req: Request, parsedBody: unknown): Promise<Response> {
    if (req.method !== "POST") {
      return Response.json({ jsonrpc: "2.0", error: { code: -32000, message: "Method not allowed." }, id: null }, { status: 405 });
    }
    const server = buildServer(pool);
    const transport = new WebStandardStreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    await server.connect(transport);
    try {
      return await transport.handleRequest(req, { parsedBody });
    } finally {
      void transport.close().catch(() => {});
      void server.close().catch(() => {});
    }
  }

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
    let parsedBody: unknown;
    if (req.method === "POST") {
      try {
        parsedBody = await req.json();
      } catch {
        return Response.json({ jsonrpc: "2.0", error: { code: -32700, message: "Parse error" }, id: null }, { status: 400 });
      }
    }
    if (await isLegacyRequest(req, parsedBody)) return legacy(req, parsedBody);
    return handler.fetch(req, { parsedBody });
  }

  return { fetch };
}
