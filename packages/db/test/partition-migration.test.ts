import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";
import { runMigrations } from "../src/migrate.js";
import { createProject, insertSpans } from "../src/queries.js";
import { runRetention, DEFAULT_RETENTION } from "../src/retention.js";

// Builds a database at migration 0005 with data and a read-only role, then
// applies 0006 and checks nothing was lost. Needs a DATABASE_URL whose user may
// create databases and roles (the CI and sandbox Postgres users are superusers).
const DATABASE_URL = process.env.DATABASE_URL;

describe.skipIf(!DATABASE_URL)("0006 partitions spans and logs (integration, requires DATABASE_URL)", () => {
  const dbName = `sens_mig_${Date.now()}`;
  let admin: Pool;
  let pool: Pool;
  const day = 86_400_000;
  const now = Date.now();
  let goneId = 0;

  beforeAll(async () => {
    admin = new Pool({ connectionString: DATABASE_URL });
    await admin.query(`create database ${dbName}`);
    const url = new URL(DATABASE_URL!);
    url.pathname = `/${dbName}`;
    pool = new Pool({ connectionString: url.toString() });
    await runMigrations(pool, { until: "0005_spans_start_index.sql" });
    await createProject(pool, "p", "token-p");
    await pool.query(`do $$ begin create role sens_mig_ro; exception when duplicate_object then null; end $$`);
    await pool.query("grant select on spans, logs to sens_mig_ro");
    await pool.query(`do $$ begin create role sens_mig_rw; exception when duplicate_object then null; end $$`);
    await pool.query("grant select, insert on logs to sens_mig_rw");
    // A producer clock in 2100 must not make the migration build thousands of partitions.
    await pool.query(
      `insert into spans (project, trace_id, span_id, name, kind, start_ts, end_ts, duration_ms)
       values ('p', 'future', 'f', 'n', 'server', '2100-01-01', '2100-01-01', 1)`,
    );
    // 3 spans over 3 days (one 40 days old), 2 logs.
    for (const [i, ago] of [0, 1, 40].entries()) {
      const t = new Date(now - ago * day).toISOString();
      await pool.query(
        `insert into spans (project, trace_id, span_id, name, kind, start_ts, end_ts, duration_ms, http_status_code)
         values ('p', $1, $2, 'n', 'server', $3, $3, 1, 200)`,
        [`t${i}`, `s${i}`, t],
      );
    }
    await pool.query(`insert into logs (project, ts, body) values ('p', now(), 'a'), ('p', now() - interval '2 days', 'b')`);
    // The newest log is gone before the migration: its id must not come back.
    const { rows: gone } = await pool.query(`insert into logs (project, ts, body) values ('p', now(), 'gone') returning id::int`);
    goneId = gone[0].id;
    await pool.query("delete from logs where id = $1", [goneId]);
    await runMigrations(pool);
  }, 30_000); // about 70 partitions per table: slow on a busy machine

  afterAll(async () => {
    await pool.end();
    await admin.query(`drop database ${dbName}`);
    await admin.query("drop role if exists sens_mig_ro");
    await admin.query("drop role if exists sens_mig_rw");
    await admin.end();
  });

  test("every row is copied into a partitioned table", async () => {
    const { rows } = await pool.query(
      `select (select count(*)::int from spans) spans, (select count(*)::int from logs) logs,
              (select relkind from pg_class where relname = 'spans') spans_kind,
              (select relkind from pg_class where relname = 'logs') logs_kind,
              (select count(*)::int from spans_default) in_default`,
    );
    expect(rows[0]).toEqual({ spans: 4, logs: 2, spans_kind: "p", logs_kind: "p", in_default: 1 });
  });

  test("partitions cover a bounded window, not the far-future row's day", async () => {
    const { rows } = await pool.query(
      `select count(*)::int n from pg_inherits where inhparent = 'spans'::regclass`,
    );
    // 60 days back at most, a week ahead, plus the default partition.
    expect(rows[0].n).toBeLessThanOrEqual(69);
  });

  test("a role that could insert logs still can (the new id sequence is granted)", async () => {
    const client = await pool.connect();
    try {
      await client.query("set role sens_mig_rw");
      await client.query(`insert into logs (project, ts, body) values ('p', now(), 'as rw')`);
    } finally {
      await client.query("reset role");
      client.release();
    }
  });

  test("the read-only role can still read both tables", async () => {
    const { rows } = await pool.query(
      `select has_table_privilege('sens_mig_ro', 'spans', 'select') s, has_table_privilege('sens_mig_ro', 'logs', 'select') l`,
    );
    expect(rows[0]).toEqual({ s: true, l: true });
  });

  test("log ids continue after the old sequence, past deleted rows", async () => {
    const { rows } = await pool.query(`insert into logs (project, ts, body) values ('p', now(), 'c') returning id::int`);
    expect(rows[0].id).toBeGreaterThan(goneId);
    const { rows: reused } = await pool.query("select count(*)::int n from logs where id = $1", [goneId]);
    expect(reused[0].n).toBe(0);
  });

  test("a retried span upserts instead of duplicating", async () => {
    const { rows } = await pool.query("select * from spans where trace_id = 't0'");
    const r = rows[0];
    const span = {
      project: "p",
      traceId: "t0",
      spanId: "s0",
      parentSpanId: null,
      name: "renamed",
      kind: "server" as const,
      startTs: r.start_ts,
      endTs: r.end_ts,
      durationMs: 1,
      statusCode: null,
      httpStatusCode: 200,
      resource: {},
      attributes: {},
    };
    await insertSpans(pool, [span]);
    const { rows: after } = await pool.query("select name from spans where trace_id = 't0'");
    expect(after).toEqual([{ name: "renamed" }]);
  });

  test("retention drops expired days for spans and logs by partition", async () => {
    const report = await runRetention(pool, { ...DEFAULT_RETENTION, spanDays: 30, logDays: 30 }, new Date(now));
    expect(report.spans.partitionsDropped.length).toBeGreaterThan(0);
    const { rows } = await pool.query(
      "select count(*)::int n, (select count(*)::int from spans_default) d, (select count(*)::int from spans_21000101) f from spans",
    );
    // The 40-day-old span is gone; the 2100 one moved out of default into its own day.
    expect(rows[0]).toEqual({ n: 3, d: 0, f: 1 });
    expect(report.spans.defaultRowsRescued).toBe(1);
    expect(report.logs.partitionsCreated).toEqual([]);
  });
});
