import { getPool } from "@sensorium/db";
import { mcpAuthConfigFromEnv } from "./auth.js";
import { createMcpApp } from "./server.js";

const auth = mcpAuthConfigFromEnv();
const port = Number(process.env.MCP_PORT ?? 8788);
// The MCP server only reads. Its sessions are read-only in the database too, so a bug
// cannot write, and a query an agent sends over a wide window stops after 15 seconds
// instead of holding a connection ingest needs.
const pool = getPool({
  options: "-c default_transaction_read_only=on",
  statement_timeout: 15_000,
  connectionTimeoutMillis: 5_000,
});
const app = createMcpApp(pool, auth);

Bun.serve({ port, fetch: app.fetch });

console.log(
  `sensorium mcp listening on :${port} (${[auth.staticToken && "static token", auth.oidc && `OIDC ${auth.oidc.issuer}`].filter(Boolean).join(" + ")})`,
);
