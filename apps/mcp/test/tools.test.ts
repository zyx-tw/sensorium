import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createPool, createProject, runMigrations, type Pool } from "@sensorium/db";
import { createMcpApp } from "../src/server.js";

const DATABASE_URL = process.env.DATABASE_URL;

async function call(app: ReturnType<typeof createMcpApp>, method: string, params: unknown = {}) {
  const res = await app.fetch(
    new Request("http://x/mcp", {
      method: "POST",
      headers: { authorization: "Bearer t", "content-type": "application/json", accept: "application/json, text/event-stream" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    }),
  );
  const text = await res.text();
  const json = text.startsWith("{") ? text : text.split("\n").find((l) => l.startsWith("data: "))!.slice(6);
  return JSON.parse(json) as { result: Record<string, unknown> };
}

describe.skipIf(!DATABASE_URL)("mcp tools (integration, requires DATABASE_URL)", () => {
  let pool: Pool;
  let readOnly: Pool;
  let app: ReturnType<typeof createMcpApp>;
  const project = `test-mcp-${Date.now()}`;

  beforeAll(async () => {
    pool = createPool({ connectionString: DATABASE_URL });
    await runMigrations(pool);
    await createProject(pool, project, `token-${project}`);
    // The same session settings apps/mcp/src/index.ts uses.
    readOnly = createPool({ connectionString: DATABASE_URL, options: "-c default_transaction_read_only=on", statement_timeout: 15_000 });
    app = createMcpApp(readOnly, { staticToken: "t" });
  });

  afterAll(async () => {
    await pool.query("delete from logs where project = $1", [project]);
    await pool.query("delete from projects where name = $1", [project]);
    await readOnly.end();
    await pool.end();
  });

  test("every tool is annotated read-only and declares an output schema", async () => {
    const { result } = await call(app, "tools/list");
    const tools = result.tools as { name: string; annotations?: Record<string, boolean>; outputSchema?: unknown }[];
    expect(tools.length).toBe(10);
    for (const tool of tools) {
      expect(tool.annotations).toMatchObject({ readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false });
      expect(tool.outputSchema).toBeDefined();
    }
  });

  test("list_projects returns structured content with the project", async () => {
    const { result } = await call(app, "tools/call", { name: "list_projects", arguments: {} });
    const structured = result.structuredContent as { result: { name: string }[] };
    expect(structured.result.some((p) => p.name === project)).toBe(true);
    // The text content stays the bare data for callers that parse it.
    const text = (result.content as { text: string }[])[0]!.text;
    expect(JSON.parse(text)).toEqual(structured.result);
  });

  test("storage_stats reports each partitioned table and the silent project", async () => {
    const { result } = await call(app, "tools/call", { name: "storage_stats", arguments: {} });
    const stats = (result.structuredContent as { result: { tables: { table: string; partitions: number }[]; silentProjects: { name: string }[] } }).result;
    expect(stats.tables.map((t) => t.table)).toEqual(["metric_points", "spans", "logs"]);
    expect(stats.tables[0]!.partitions).toBeGreaterThan(0);
    expect(stats.silentProjects.some((p) => p.name === project)).toBe(true);
  });

  test("a list cut at its limit says so", async () => {
    await pool.query(`insert into logs (project, ts, body) values ($1, now(), 'a'), ($1, now(), 'b')`, [project]);
    const since = new Date(Date.now() - 60_000).toISOString();
    const { result } = await call(app, "tools/call", { name: "query_logs", arguments: { project, since, limit: 1 } });
    expect((result.structuredContent as { truncated: boolean }).truncated).toBe(true);
  });

  test("the MCP database session refuses writes", async () => {
    await expect(readOnly.query("insert into projects (name, ingest_token_hash) values ('x', 'y')")).rejects.toThrow(/read-only/);
  });
});
