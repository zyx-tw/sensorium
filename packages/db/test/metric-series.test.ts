import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { MetricPointRow } from "@sensorium/core";
import { Pool } from "pg";
import { insertMetricPoints, oldestDirtyBucket, queryMetricBuckets, rollUpMetrics, rolledUntil } from "../src/metrics.js";
import { runMigrations } from "../src/migrate.js";
import { createProject, listMetricNames, queryMetrics } from "../src/queries.js";
import { dropExpiredPartitions, utcDay } from "../src/retention.js";

// Builds a database at migration 0007 with metric rows and a read-only role,
// applies 0008, and checks old and new points read back the same way.
const DATABASE_URL = process.env.DATABASE_URL;

const point = (over: Partial<MetricPointRow>): MetricPointRow => ({
  project: "p",
  metricName: "cpu",
  ts: new Date(),
  kind: "gauge",
  value: 1,
  attributes: { host: "a", core: 0 },
  resource: { "service.name": "s" },
  ...over,
});

describe.skipIf(!DATABASE_URL)("0008 metric series (integration, requires DATABASE_URL)", () => {
  const dbName = `sens_series_${Date.now()}`;
  let admin: Pool;
  let pool: Pool;
  let url: URL;
  const cutover = new Date();

  beforeAll(async () => {
    admin = new Pool({ connectionString: DATABASE_URL });
    await admin.query(`create database ${dbName}`);
    url = new URL(DATABASE_URL!);
    url.pathname = `/${dbName}`;
    pool = new Pool({ connectionString: url.toString() });
    await runMigrations(pool, { until: "0007_project_last_rejected.sql" });
    await createProject(pool, "p", "token-p");
    await pool.query(`do $$ begin create role sens_series_ro; exception when duplicate_object then null; end $$`);
    await pool.query("grant select on metric_points to sens_series_ro");
    // Like golem_ro in production: column grants only on projects.
    await pool.query("grant select (name, last_seen_at, created_at) on projects to sens_series_ro");
    // Two old points, 2 hours before the cutover.
    await pool.query(
      `insert into metric_points (project, metric_name, ts, kind, value, attributes, resource)
       values ('p', 'cpu', now() - interval '2 hours', 'gauge', 10, '{"host":"a","core":0}', '{"service.name":"s"}'),
              ('p', 'cpu', now() - interval '2 hours' + interval '1 second', 'gauge', 20, '{"host":"a","core":0}', '{"service.name":"s"}')`,
    );
    await runMigrations(pool);
  }, 30_000);

  afterAll(async () => {
    await pool.end();
    await admin.query(`drop database ${dbName}`);
    await admin.query("drop role if exists sens_series_ro");
    await admin.end();
  });

  test("the migration seeds last_metric_at, and ingest keeps it", async () => {
    const { rows } = await pool.query("select last_metric_at is not null as seeded from projects where name = 'p'");
    expect(rows[0].seeded).toBe(true);
    await createProject(pool, "q", "token-q");
    await insertMetricPoints(pool, [point({ project: "q", metricName: "q.only" })]);
    const { rows: q } = await pool.query("select last_metric_at > now() - interval '1 minute' as fresh from projects where name = 'q'");
    expect(q[0].fresh).toBe(true);
  });

  test("the old points are still read through metric_points", async () => {
    const { rows } = await pool.query("select count(*)::int n from metric_points where project = 'p'");
    expect(rows[0].n).toBe(2);
    const { rows: legacy } = await pool.query("select count(*)::int n from metric_points_legacy");
    expect(legacy[0].n).toBe(2);
  });

  test("the read-only role can read the view", async () => {
    const client = await pool.connect();
    try {
      await client.query("set role sens_series_ro");
      const { rows } = await client.query("select count(*)::int n from metric_points where project = 'p'");
      expect(rows[0].n).toBe(2);
      // golem's continuous-project query.
      const { rows: active } = await client.query(
        "select name as project from projects where last_metric_at > now() - interval '1 day'",
      );
      expect(active.map((r) => r.project)).toContain("p");
    } finally {
      await client.query("reset role");
      client.release();
    }
  });

  test("attribute key order does not split a series", async () => {
    await insertMetricPoints(pool, [point({ value: 1, attributes: { host: "a", core: 0 } })]);
    // A fresh pool has an empty series cache, so this one resolves through the database.
    const other = new Pool({ connectionString: url.toString() });
    try {
      await insertMetricPoints(other, [point({ value: 2, attributes: { core: 0, host: "a" } })]);
    } finally {
      await other.end();
    }
    const { rows } = await pool.query("select count(*)::int n from metric_series where metric_name = 'cpu'");
    expect(rows[0].n).toBe(1);
  });

  test("two batches creating the same new series at once both succeed", async () => {
    const a = new Pool({ connectionString: url.toString() });
    const b = new Pool({ connectionString: url.toString() });
    try {
      const rows = Array.from({ length: 50 }, (_, i) => point({ metricName: "race", attributes: { i } }));
      await Promise.all([insertMetricPoints(a, rows), insertMetricPoints(b, [...rows].reverse())]);
    } finally {
      await a.end();
      await b.end();
    }
    const { rows } = await pool.query("select count(*)::int s, (select count(*)::int from metric_points where metric_name = 'race') p from metric_series where metric_name = 'race'");
    expect(rows[0]).toEqual({ s: 50, p: 100 });
  });

  test("new points read back with their series attributes", async () => {
    const result = await queryMetrics(pool, { project: "p", metricName: "cpu", since: new Date(Date.now() - 3 * 3_600_000) });
    expect(result.summary.count).toBe(4);
    expect(result.points[0]!.attributes).toEqual({ host: "a", core: 0 });
    const catalog = await listMetricNames(pool, { project: "p", since: new Date(Date.now() - 3 * 3_600_000) });
    expect(catalog.find((c) => c.metricName === "cpu")?.pointCount).toBe(4);
  });

  test("rollups fold finished buckets, recompute late ones, and only move forward", async () => {
    // Points in one bucket 30 to 20 minutes ago.
    const base = Math.floor((Date.now() - 30 * 60_000) / 300_000) * 300_000;
    await insertMetricPoints(pool, [
      point({ metricName: "roll", ts: new Date(base + 10_000), value: 5 }),
      point({ metricName: "roll", ts: new Date(base + 20_000), value: 1 }),
      point({ metricName: "roll", ts: new Date(base + 30_000), value: 3 }),
    ]);
    // The migration set rolled_until to its own time, after these points; pull it back as if ingest had run before.
    await pool.query("update metric_rollup_progress set rolled_until = $1", [new Date(base)]);
    const first = await rollUpMetrics(pool);
    expect(first.buckets).toBeGreaterThan(0);
    const bucket = async () =>
      (
        await pool.query(
          `select r.count, r.sum, r.min, r.max, r.last from metric_rollups r join metric_series s on s.id = r.series_id
            where s.metric_name = 'roll' and r.bucket = $1`,
          [new Date(base)],
        )
      ).rows[0];
    expect(await bucket()).toEqual({ count: 3, sum: 9, min: 1, max: 5, last: 3 });

    // A point that arrives late, within 15 minutes of the last rollup, is folded in on the next run.
    await pool.query("update metric_rollup_progress set rolled_until = $1", [new Date(base + 600_000)]);
    await insertMetricPoints(pool, [point({ metricName: "roll", ts: new Date(base + 40_000), value: 7 })]);
    await rollUpMetrics(pool);
    expect(await bucket()).toEqual({ count: 4, sum: 16, min: 1, max: 7, last: 7 });

    const before = (await rolledUntil(pool)).getTime();
    await rollUpMetrics(pool, new Date(Date.now() - 3_600_000));
    expect((await rolledUntil(pool)).getTime()).toBe(before);
  });

  test("buckets combine rollups, raw points after them, and legacy points", async () => {
    const series = await queryMetricBuckets(pool, { project: "p", metricName: "cpu", since: new Date(Date.now() - 3 * 3_600_000), stepSeconds: 3600 });
    expect(series).toHaveLength(1);
    const total = series[0]!.buckets.reduce((n, b) => n + b.count, 0);
    expect(total).toBe(4);
    // The legacy hour holds 10 and 20.
    const legacy = series[0]!.buckets.find((b) => b.max === 20)!;
    expect(legacy).toMatchObject({ count: 2, min: 10, avg: 15, last: 20 });
    await expect(queryMetricBuckets(pool, { project: "p", metricName: "cpu", since: new Date(), stepSeconds: 100 })).rejects.toThrow();
  });

  test("a point hours late is marked and folded in by the next rollup", async () => {
    // Rollups have moved past the point's bucket long ago.
    const ts = new Date(Date.now() - 2 * 3_600_000);
    await insertMetricPoints(pool, [point({ metricName: "late", ts, value: 42 })]);
    expect((await oldestDirtyBucket(pool))?.getTime()).toBeLessThanOrEqual(ts.getTime());
    await rollUpMetrics(pool);
    expect(await oldestDirtyBucket(pool)).toBeNull();
    const { rows } = await pool.query(
      "select r.count, r.last from metric_rollups r join metric_series s on s.id = r.series_id where s.metric_name = 'late'",
    );
    expect(rows).toEqual([{ count: 1, last: 42 }]);
  });

  test("an ingest from before 0008 still writes through the view, into the same series", async () => {
    await insertMetricPoints(pool, [point({ metricName: "compat", value: 1, attributes: { host: "a", core: 0 } })]);
    await pool.query(
      `insert into metric_points (project, metric_name, ts, kind, value, attributes, resource)
       values ('p', 'compat', now(), 'gauge', 2, '{"core":0,"host":"a"}', '{"service.name":"s"}'),
              ('p', 'compat', now() - interval '1 hour', 'gauge', 3, '{"core":0,"host":"a"}', '{"service.name":"s"}')`,
    );
    const { rows } = await pool.query(
      "select count(distinct s.id)::int series, count(*)::int points from metric_samples p join metric_series s on s.id = p.series_id where s.metric_name = 'compat'",
    );
    expect(rows[0]).toEqual({ series: 1, points: 3 });
    // The hour-old row is marked for the rollup like a late point from ingest.
    expect(await oldestDirtyBucket(pool)).not.toBeNull();
    await rollUpMetrics(pool);
  });

  test("a gauge and a sum with the same name and labels stay apart in buckets", async () => {
    await insertMetricPoints(pool, [point({ metricName: "mixed", kind: "gauge", value: 1 }), point({ metricName: "mixed", kind: "sum", value: 100 })]);
    const series = await queryMetricBuckets(pool, { project: "p", metricName: "mixed", since: new Date(Date.now() - 600_000), stepSeconds: 300 });
    expect(series.map((s) => [s.kind, s.buckets[0]!.max]).sort()).toEqual([["gauge", 1], ["sum", 100]]);
  });

  test("a samples partition is not dropped before it is rolled up", async () => {
    const today = utcDay(new Date());
    // Keep 0 days with a cutoff past today, but rollups complete only until yesterday.
    const kept = await dropExpiredPartitions(pool, 0, utcDay(today, 2), "metric_samples", utcDay(today, -1));
    expect(kept.some((n) => n.endsWith(today.toISOString().slice(0, 10).replaceAll("-", "")))).toBe(false);
    void cutover;
  });
});
