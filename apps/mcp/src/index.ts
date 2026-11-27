import { getPool } from "@sensorium/db";
import { mcpAuthConfigFromEnv } from "./auth.js";
import { createMcpApp } from "./server.js";

const auth = mcpAuthConfigFromEnv();
const port = Number(process.env.MCP_PORT ?? 8788);
const pool = getPool();
const app = createMcpApp(pool, auth);

Bun.serve({ port, fetch: app.fetch });

console.log(
  `sensorium mcp listening on :${port} (${[auth.staticToken && "static token", auth.oidc && `OIDC ${auth.oidc.issuer}`].filter(Boolean).join(" + ")})`,
);
