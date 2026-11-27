import { createHash } from "node:crypto";
import type { MetricPointRow } from "@sensorium/core";
import type { Pool } from "pg";

/**
 * Metric storage since migration 0008: a series (project, name, kind,
 * attributes, resource) is stored once in metric_series, and each point is
 * (series_id, ts, value) in metric_samples. metric_rollups keeps 5-minute
 * buckets per series longer than the raw samples. The metric_points view joins
 * them back, plus the pre-0008 rows in metric_points_legacy.
 */

const MAX_BIND_PARAMETERS = 65_535;
/** Series ids the ingest process remembers; past this the cache starts over. */
const SERIES_CACHE_LIMIT = 500_000;
export const ROLLUP_BUCKET_SECONDS = 300;
/** Buckets this far before the last rollup are computed again, for points that arrive late. */
const ROLLUP_LATE_MS = 15 * 60_000;
/** One rollup statement covers at most this much time. */
const ROLLUP_CHUNK_MS = 6 * 3_600_000;
const BUCKET_ORIGIN = "2000-01-01T00:00:00Z";

/** JSON with object keys sorted at every level, so key order never splits a series. */
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(",")}}`;
  }
  return JSON.stringify(value ?? null);
}

/** sha256 over the canonical [name, kind, attributes, resource]; unique per project in metric_series. */
export function seriesIdentity(row: Pick<MetricPointRow, "metricName" | "kind" | "attributes" | "resource">): Buffer {
  return createHash("sha256").update(canonical([row.metricName, row.kind, row.attributes, row.resource])).digest();
}

const seriesCaches = new WeakMap<Pool, Map<string, string>>();
const lastMetricWritten = new WeakMap<Pool, Map<string, number>>();
/** projects.last_metric_at is written at most this often per project. */
export const LAST_METRIC_INTERVAL_MS = 30_000;

/** Marks the projects as having sent metrics now, at most once per interval each. */
async function touchLastMetric(pool: Pool, projects: Iterable<string>): Promise<void> {
  let written = lastMetricWritten.get(pool);
  if (!written) {
    written = new Map();
    lastMetricWritten.set(pool, written);
  }
  const now = Date.now();
  const due = [...new Set(projects)].filter((p) => now - (written.get(p) ?? 0) >= LAST_METRIC_INTERVAL_MS);
  if (due.length === 0) return;
  for (const p of due) written.set(p, now);
  await pool.query("update projects set last_metric_at = now() where name = any($1::text[])", [due]).catch(() => {
    for (const p of due) written.delete(p);
  });
}

function cacheFor(pool: Pool): Map<string, string> {
  let cache = seriesCaches.get(pool);
  if (!cache) {
    cache = new Map();
    seriesCaches.set(pool, cache);
  }
  return cache;
}

/** Series ids for the rows, creating the series that do not exist yet. Keyed by project + identity hex. */
async function resolveSeries(pool: Pool, rows: MetricPointRow[], identities: Buffer[]): Promise<string[]> {
  const cache = cacheFor(pool);
  const keys = rows.map((row, i) => `${row.project}\u0000${identities[i]!.toString("hex")}`);
  const missing = new Map<string, number>();
  keys.forEach((key, i) => {
    if (!cache.has(key) && !missing.has(key)) missing.set(key, i);
  });

  if (missing.size > 0) {
    if (cache.size + missing.size > SERIES_CACHE_LIMIT) cache.clear();
    // Sorted, so two batches that create the same series lock them in the same order.
    const todo = [...missing.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    const perStatement = Math.floor(MAX_BIND_PARAMETERS / 6);
    for (let start = 0; start < todo.length; start += perStatement) {
      const chunk = todo.slice(start, start + perStatement);
      const values: unknown[] = [];
      const tuples = chunk.map(([, i], n) => {
        const row = rows[i]!;
        values.push(row.project, identities[i], row.metricName, row.kind, JSON.stringify(row.attributes), JSON.stringify(row.resource));
        const b = n * 6;
        return `($${b + 1}, $${b + 2}, $${b + 3}, $${b + 4}, $${b + 5}, $${b + 6})`;
      });
      await pool.query(
        `insert into metric_series (project, identity, metric_name, kind, attributes, resource)
         values ${tuples.join(", ")} on conflict (project, identity) do nothing`,
        values,
      );
      const { rows: found } = await pool.query<{ id: string; project: string; identity: Buffer }>(
        `select id, project, identity from metric_series
          where (project, identity) in (select * from unnest($1::text[], $2::bytea[]))`,
        [chunk.map(([, i]) => rows[i]!.project), chunk.map(([, i]) => identities[i])],
      );
      for (const r of found) cache.set(`${r.project}\u0000${r.identity.toString("hex")}`, r.id);
    }
  }

  return keys.map((key) => {
    const id = cache.get(key);
    if (id === undefined) throw new Error("metric series was not created");
    return id;
  });
}

export async function insertMetricPoints(pool: Pool, rows: MetricPointRow[]): Promise<number> {
  if (rows.length === 0) return 0;
  const ids = await resolveSeries(pool, rows, rows.map(seriesIdentity));
  const perStatement = Math.floor(MAX_BIND_PARAMETERS / 3);
  const statements: { text: string; values: unknown[] }[] = [];
  for (let start = 0; start < rows.length; start += perStatement) {
    const values: unknown[] = [];
    const tuples: string[] = [];
    for (let i = start; i < Math.min(rows.length, start + perStatement); i++) {
      const b = values.length;
      values.push(ids[i], rows[i]!.ts, rows[i]!.value);
      tuples.push(`($${b + 1}, $${b + 2}, $${b + 3})`);
    }
    statements.push({ text: `insert into metric_samples (series_id, ts, value) values ${tuples.join(", ")}`, values });
  }
  if (statements.length === 1) {
    await pool.query(statements[0]!.text, statements[0]!.values);
    await touchLastMetric(pool, rows.map((r) => r.project));
    return rows.length;
  }
  // One batch is all or nothing, so a retried export never stores half twice.
  const client = await pool.connect();
  try {
    await client.query("begin");
    for (const s of statements) await client.query(s.text, s.values);
    await client.query("commit");
  } catch (err) {
    await client.query("rollback").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
  await touchLastMetric(pool, rows.map((r) => r.project));
  return rows.length;
}

/** Every bucket before this instant is final in metric_rollups. */
export async function rolledUntil(pool: Pool): Promise<Date> {
  const { rows } = await pool.query<{ rolled_until: Date }>("select rolled_until from metric_rollup_progress");
  return rows[0]!.rolled_until;
}

function bucketFloor(ms: number): number {
  const step = ROLLUP_BUCKET_SECONDS * 1000;
  const origin = Date.parse(BUCKET_ORIGIN);
  return origin + Math.floor((ms - origin) / step) * step;
}

/**
 * Folds finished 5-minute buckets of metric_samples into metric_rollups.
 * The last few buckets before the previous run are computed again, so a point
 * that arrived up to 15 minutes late still counts. Returns the new rolled_until.
 */
export async function rollUpMetrics(pool: Pool, now = new Date()): Promise<{ from: Date; until: Date; buckets: number }> {
  const previous = (await rolledUntil(pool)).getTime();
  // A bucket is final once a whole bucket has passed since its end.
  const until = bucketFloor(now.getTime() - ROLLUP_BUCKET_SECONDS * 1000);
  const from = bucketFloor(previous - ROLLUP_LATE_MS);
  let buckets = 0;
  for (let start = from; start < until; start += ROLLUP_CHUNK_MS) {
    const end = Math.min(until, start + ROLLUP_CHUNK_MS);
    const res = await pool.query(
      `insert into metric_rollups (series_id, bucket, count, sum, min, max, last)
       select series_id, date_bin('${ROLLUP_BUCKET_SECONDS} seconds', ts, timestamptz '${BUCKET_ORIGIN}'),
              count(*), sum(value), min(value), max(value), (array_agg(value order by ts desc))[1]
         from metric_samples
        where ts >= $1 and ts < $2
        group by 1, 2
       on conflict (series_id, bucket) do update
         set count = excluded.count, sum = excluded.sum, min = excluded.min, max = excluded.max, last = excluded.last`,
      [new Date(start), new Date(end)],
    );
    buckets += res.rowCount ?? 0;
    // Progress moves forward only, chunk by chunk, so a failed run resumes where it stopped.
    await pool.query("update metric_rollup_progress set rolled_until = greatest(rolled_until, $1)", [new Date(end)]);
  }
  return { from: new Date(from), until: new Date(Math.max(until, previous)), buckets };
}

export interface MetricBucketParams {
  project: string;
  metricName: string;
  since: Date;
  until?: Date;
  /** Bucket width in seconds, a multiple of 300. */
  stepSeconds: number;
  limit?: number;
}

export interface MetricBucket {
  ts: Date;
  count: number;
  avg: number;
  min: number;
  max: number;
  /** The newest value in the bucket; use it for cumulative counters. */
  last: number;
}

export interface MetricBucketSeries {
  attributes: Record<string, unknown>;
  resource: Record<string, unknown>;
  /** Newest first. */
  buckets: MetricBucket[];
}

/**
 * Buckets of `stepSeconds` per series: from metric_rollups where they are
 * final, from raw points after that, and from metric_points_legacy for the
 * time before migration 0008. `limit` caps the buckets per series.
 */
export async function queryMetricBuckets(pool: Pool, params: MetricBucketParams): Promise<MetricBucketSeries[]> {
  if (!Number.isInteger(params.stepSeconds) || params.stepSeconds < ROLLUP_BUCKET_SECONDS || params.stepSeconds % ROLLUP_BUCKET_SECONDS !== 0) {
    throw new Error(`step must be a multiple of ${ROLLUP_BUCKET_SECONDS} seconds`);
  }
  const until = params.until ?? new Date();
  const limit = Math.min(params.limit ?? 288, 2000);
  const rolled = await rolledUntil(pool);
  const { rows: legacy } = await pool.query<{ ok: boolean }>("select to_regclass('metric_points_legacy') is not null as ok");
  const legacyBranch = legacy[0]!.ok
    ? `union all
       select attributes, resource, ts, 1, value, value, value, value
         from metric_points_legacy
        where project = $1 and metric_name = $2 and ts >= $3::timestamptz and ts < least($4::timestamptz, $5::timestamptz)`
    : "";
  const { rows } = await pool.query<{
    attributes: Record<string, unknown>;
    resource: Record<string, unknown>;
    ts: Date;
    count: string;
    sum: number;
    min: number;
    max: number;
    last: number;
  }>(
    `with pts (attributes, resource, ts, count, sum, min, max, last) as (
       select s.attributes, s.resource, r.bucket, r.count, r.sum, r.min, r.max, r.last
         from metric_rollups r
         join metric_series s on s.id = r.series_id
        where s.project = $1 and s.metric_name = $2 and r.bucket >= $3::timestamptz and r.bucket < least($4::timestamptz, $5::timestamptz)
       union all
       select s.attributes, s.resource, p.ts, 1, p.value, p.value, p.value, p.value
         from metric_samples p
         join metric_series s on s.id = p.series_id
        where s.project = $1 and s.metric_name = $2 and p.ts >= greatest($3::timestamptz, $5::timestamptz) and p.ts < $4::timestamptz
       ${legacyBranch}
     ), binned as (
       select attributes, resource, date_bin(make_interval(secs => $6::int), ts, timestamptz '${BUCKET_ORIGIN}') as ts,
              sum(count) as count, sum(sum) as sum, min(min) as min, max(max) as max,
              (array_agg(last order by ts desc))[1] as last
         from pts
        group by 1, 2, 3
     )
     select * from (
       select *, row_number() over (partition by attributes, resource order by ts desc) as n from binned
     ) b where n <= $7::int
     order by attributes::text, resource::text, ts desc`,
    [params.project, params.metricName, params.since, until, rolled, params.stepSeconds, limit],
  );
  const series = new Map<string, MetricBucketSeries>();
  for (const r of rows) {
    const key = `${JSON.stringify(r.attributes)}\u0000${JSON.stringify(r.resource)}`;
    let s = series.get(key);
    if (!s) {
      s = { attributes: r.attributes, resource: r.resource, buckets: [] };
      series.set(key, s);
    }
    const count = Number(r.count);
    s.buckets.push({ ts: r.ts, count, avg: r.sum / count, min: r.min, max: r.max, last: r.last });
  }
  return [...series.values()];
}
