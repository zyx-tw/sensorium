import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";
import { runMigrations } from "../src/migrate.js";

// A migration that waits on a table lock gives up after lock_timeout and tries
// again, instead of queueing every later reader and writer behind it.
const DATABASE_URL = process.env.DATABASE_URL;

describe.skipIf(!DATABASE_URL)("migration lock timeout (integration, requires DATABASE_URL)", () => {
  const dbName = `sens_lock_${Date.now()}`;
  let admin: Pool;
  let pool: Pool;

  beforeAll(async () => {
    admin = new Pool({ connectionString: DATABASE_URL });
    await admin.query(`create database ${dbName}`);
    const url = new URL(DATABASE_URL!);
    url.pathname = `/${dbName}`;
    pool = new Pool({ connectionString: url.toString() });
    await runMigrations(pool, { until: "0008_metric_series.sql" });
  }, 30_000);

  afterAll(async () => {
    await pool.end();
    await admin.query(`drop database ${dbName}`);
    await admin.end();
  });

  // 0009 creates an index on spans, which waits for any lock that conflicts with it.
  const holdSpans = async () => {
    const holder = await pool.connect();
    await holder.query("begin");
    await holder.query("lock table spans in access exclusive mode");
    return holder;
  };

  test("gives up with a clear error when the lock never frees", async () => {
    const holder = await holdSpans();
    try {
      const started = Date.now();
      await expect(runMigrations(pool, { lockTimeoutMs: 100, lockAttempts: 2 })).rejects.toThrow(/0009.*lock timeout/);
      // 2 tries of 100 ms plus one 500 ms backoff, not an unbounded wait.
      expect(Date.now() - started).toBeLessThan(5_000);
    } finally {
      await holder.query("rollback");
      holder.release();
    }
    const { rows } = await pool.query("select count(*)::int n from schema_migrations where filename like '0009%'");
    expect(rows[0].n).toBe(0);
  });

  test("retries and applies once the lock frees", async () => {
    const holder = await holdSpans();
    setTimeout(() => {
      void holder.query("rollback").then(() => holder.release());
    }, 300);
    const { applied } = await runMigrations(pool, { lockTimeoutMs: 100, lockAttempts: 10 });
    expect(applied).toContain("0009_spans_gen_ai_index.sql");
  });
});
