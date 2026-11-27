import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { SpanRow } from "@sensorium/core";
import { Pool } from "pg";
import { agentUsage } from "../src/agent-usage.js";
import { runMigrations } from "../src/migrate.js";
import { createProject, insertSpans } from "../src/queries.js";

const DATABASE_URL = process.env.DATABASE_URL;

describe.skipIf(!DATABASE_URL)("agentUsage (integration, requires DATABASE_URL)", () => {
  let pool: Pool;
  const project = `test-agent-${Date.now()}`;
  const now = Date.now();
  let n = 0;
  const span = (attributes: Record<string, unknown>, over: Partial<SpanRow> = {}): SpanRow => ({
    project,
    traceId: "t",
    spanId: `s${n++}`,
    parentSpanId: null,
    name: "llm",
    kind: "client",
    startTs: new Date(now - 60_000),
    endTs: new Date(now - 59_000),
    durationMs: 1000,
    statusCode: null,
    httpStatusCode: null,
    resource: {},
    attributes,
    ...over,
  });

  beforeAll(async () => {
    pool = new Pool({ connectionString: DATABASE_URL });
    await runMigrations(pool);
    await createProject(pool, project, `token-${project}`);
    await insertSpans(pool, [
      // Current names, tokens as numbers.
      span({ "gen_ai.operation.name": "chat", "gen_ai.provider.name": "anthropic", "gen_ai.request.model": "claude-opus-5-5", "gen_ai.usage.input_tokens": 100, "gen_ai.usage.output_tokens": 20 }),
      span({ "gen_ai.operation.name": "chat", "gen_ai.provider.name": "anthropic", "gen_ai.request.model": "claude-opus-5-5", "gen_ai.usage.input_tokens": "50", "gen_ai.usage.output_tokens": 5, "error.type": "overloaded" }),
      // Older names.
      span({ "gen_ai.system": "openai", "gen_ai.request.model": "gpt-x", "gen_ai.usage.prompt_tokens": 7, "gen_ai.usage.completion_tokens": 3 }),
      // Tool calls.
      span({ "gen_ai.operation.name": "execute_tool", "gen_ai.tool.name": "search" }, { durationMs: 200, kind: "internal" }),
      span({ "gen_ai.operation.name": "execute_tool", "gen_ai.tool.name": "search" }, { durationMs: 400, kind: "internal", statusCode: "error" }),
      // Not a GenAI span.
      span({ "http.route": "/x" }, { kind: "server" }),
    ]);
  });

  afterAll(async () => {
    await pool.query("delete from spans where project = $1", [project]);
    await pool.query("delete from projects where name = $1", [project]);
    await pool.end();
  });

  test("tokens, calls and errors per model, with the older attribute names as fallback", async () => {
    const usage = await agentUsage(pool, { project, since: new Date(now - 3_600_000) });
    const opus = usage.models.find((m) => m.model === "claude-opus-5-5")!;
    expect(opus).toMatchObject({ provider: "anthropic", operation: "chat", calls: 2, errors: 1, inputTokens: 150, outputTokens: 25 });
    const gpt = usage.models.find((m) => m.model === "gpt-x")!;
    expect(gpt).toMatchObject({ provider: "openai", calls: 1, inputTokens: 7, outputTokens: 3 });
    expect(usage.models.some((m) => m.operation === "execute_tool")).toBe(false);
  });

  test("tool calls with errors and latency", async () => {
    const usage = await agentUsage(pool, { project, since: new Date(now - 3_600_000) });
    expect(usage.tools).toEqual([{ tool: "search", calls: 2, errors: 1, avgDurationMs: 300, maxDurationMs: 400 }]);
    expect(usage.errorTypes).toEqual(expect.arrayContaining([{ type: "overloaded", count: 1 }, { type: "error", count: 1 }]));
  });

  test("today's spans partition has the gen_ai partial index, and the query uses it", async () => {
    const today = new Date().toISOString().slice(0, 10).replaceAll("-", "");
    const { rows } = await pool.query(
      "select indexdef from pg_indexes where tablename = $1 and indexdef like '%gen_ai.operation.name%'",
      [`spans_${today}`],
    );
    expect(rows.length).toBe(1);
    const client = await pool.connect();
    try {
      await client.query("set enable_seqscan = off");
      const { rows: plan } = await client.query(
        `explain select count(*) from spans where project = $1 and start_ts >= now() - interval '1 hour'
           and (attributes ? 'gen_ai.operation.name' or attributes ? 'gen_ai.request.model'
                or attributes ? 'gen_ai.provider.name' or attributes ? 'gen_ai.system')`,
        [project],
      );
      expect(plan.map((r) => r["QUERY PLAN"]).join("\n")).toContain("gen_ai_idx");
    } finally {
      await client.query("reset enable_seqscan");
      client.release();
    }
  });

  test("a window with no GenAI spans is empty", async () => {
    const usage = await agentUsage(pool, { project, since: new Date(now + 60_000) });
    expect(usage).toEqual({ models: [], tools: [], errorTypes: [] });
  });
});
