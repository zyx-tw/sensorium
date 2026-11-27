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

/** JSON with object keys sorted at every level: the cache key, so key order never misses the cache. */
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

function cacheKey(row: MetricPointRow): string {
  return canonical([row.project, row.metricName, row.kind, row.attributes, row.resource]);
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

/**
 * Series ids for the rows, creating the series that do not exist yet. The
 * database computes the identity (metric_series_identity in migration 0008), so
 * ingest and the compatibility trigger agree on it. Ids for this batch live in
 * a map of the call's own, so a cache cleared by another request cannot lose them.
 */
async function resolveSeries(pool: Pool, rows: MetricPointRow[]): Promise<string[]> {
  const cache = cacheFor(pool);
  const keys = rows.map(cacheKey);
  const resolved = new Map<string, string>();
  const missing = new Map<string, number>();
  keys.forEach((key, i) => {
    const id = cache.get(key);
    if (id !== undefined) resolved.set(key, id);
    else if (!missing.has(key)) missing.set(key, i);
  });

  if (missing.size > 0) {
    if (cache.size + missing.size > SERIES_CACHE_LIMIT) cache.clear();
    // Sorted, so two batches that create the same series lock them in the same order.
    const todo = [...missing.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    const perStatement = Math.floor(MAX_BIND_PARAMETERS / 5);
    for (let start = 0; start < todo.length; start += perStatement) {
      const chunk = todo.slice(start, start + perStatement);
      const columns = {
        project: chunk.map(([, i]) => rows[i]!.project),
        name: chunk.map(([, i]) => rows[i]!.metricName),
        kind: chunk.map(([, i]) => rows[i]!.kind),
        attributes: chunk.map(([, i]) => JSON.stringify(rows[i]!.attributes ?? {})),
        resource: chunk.map(([, i]) => JSON.stringify(rows[i]!.resource ?? {})),
      };
      // One statement: insert the new series, then read every id back in input order.
      const { rows: found } = await pool.query<{ n: number; id: string }>(
        `with input as (
           select n::int, project, name, kind, attributes::jsonb as attributes, resource::jsonb as resource,
                  metric_series_identity(name, kind, attributes::jsonb, resource::jsonb) as identity
             from unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[]) with ordinality
                  as t(project, name, kind, attributes, resource, n)
         ), created as (
           insert into metric_series (project, identity, metric_name, kind, attributes, resource)
           select project, identity, name, kind, attributes, resource from input order by identity
           on conflict (project, identity) do nothing
           returning id, project, identity
         )
         select i.n, coalesce(c.id, s.id) as id
           from input i
           left join created c on c.project = i.project and c.identity = i.identity
           left join metric_series s on s.project = i.project and s.identity = i.identity`,
        [columns.project, columns.name, columns.kind, columns.attributes, columns.resource],
      );
      const late: number[] = [];
      for (const r of found) {
        if (r.id === null) late.push(r.n);
        else {
          const key = chunk[r.n - 1]![0];
          resolved.set(key, r.id);
          cache.set(key, r.id);
        }
      }
      // A series another batch committed while this statement ran is not in its
      // snapshot: the insert skipped it and the join could not see it. A new
      // statement does.
      if (late.length > 0) {
        const pick = <T>(xs: T[]) => late.map((n) => xs[n - 1]!);
        const { rows: again } = await pool.query<{ n: number; id: string }>(
          `select t.n::int, s.id
             from unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::int[])
                  as t(project, name, kind, attributes, resource, n)
             join metric_series s on s.project = t.project
              and s.identity = metric_series_identity(t.name, t.kind, t.attributes::jsonb, t.resource::jsonb)`,
          [pick(columns.project), pick(columns.name), pick(columns.kind), pick(columns.attributes), pick(columns.resource), late],
        );
        for (const r of again) {
          const key = chunk[r.n - 1]![0];
          resolved.set(key, r.id);
          cache.set(key, r.id);
        }
      }
    }
  }

  return keys.map((key) => {
    const id = resolved.get(key);
    if (id === undefined) throw new Error("metric series was not created");
    return id;
  });
}

/**
 * A point this much older than now may land in a bucket the last rollup
 * finished. The next rollup starts at least 20 minutes back (rolled_until trails
 * now by 5, and it recomputes 15 more), so 15 leaves a margin.
 */
const LATE_MS = 15 * 60_000;

/** Buckets of points old enough that the next rollup would not reach them on its own. */
function lateBuckets(rows: MetricPointRow[], now: number): Date[] {
  const buckets = new Set<number>();
  for (const row of rows) {
    const t = row.ts.getTime();
    if (t < now - LATE_MS) buckets.add(bucketFloor(t));
  }
  return [...buckets].sort((a, b) => a - b).map((b) => new Date(b));
}

export async function insertMetricPoints(pool: Pool, rows: MetricPointRow[]): Promise<number> {
  if (rows.length === 0) return 0;
  const ids = await resolveSeries(pool, rows);
  const late = lateBuckets(rows, Date.now());
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
  if (late.length > 0) {
    statements.push({
      // "do update", not "do nothing": the row lock on an existing mark makes a
      // concurrent rollUpDirty wait for this batch to commit before it deletes
      // the mark and recomputes, so the recompute sees these samples.
      text:
        "insert into metric_rollup_dirty (bucket) select unnest($1::timestamptz[]) on conflict (bucket) do update set bucket = excluded.bucket",
      values: [late],
    });
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
 * that arrived up to 15 minutes late still counts, and buckets marked by later
 * points are recomputed. samplesFrom is where retention keeps raw samples from:
 * marks before it are dropped. Returns the new rolled_until.
 */
export async function rollUpMetrics(
  pool: Pool,
  now = new Date(),
  samplesFrom?: Date,
): Promise<{ from: Date; until: Date; buckets: number }> {
  const previous = (await rolledUntil(pool)).getTime();
  // A bucket is final once a whole bucket has passed since its end.
  const until = bucketFloor(now.getTime() - ROLLUP_BUCKET_SECONDS * 1000);
  const from = bucketFloor(previous - ROLLUP_LATE_MS);
  let buckets = await rollUpDirty(pool, samplesFrom);
  for (let start = from; start < until; start += ROLLUP_CHUNK_MS) {
    const end = Math.min(until, start + ROLLUP_CHUNK_MS);
    const res = await pool.query(`${ROLLUP_SQL} where ts >= $1 and ts < $2 ${ROLLUP_UPSERT}`, [new Date(start), new Date(end)]);
    buckets += res.rowCount ?? 0;
    // Progress moves forward only, chunk by chunk, so a failed run resumes where it stopped.
    await pool.query("update metric_rollup_progress set rolled_until = greatest(rolled_until, $1)", [new Date(end)]);
  }
  return { from: new Date(from), until: new Date(Math.max(until, previous)), buckets };
}

const ROLLUP_SQL = `insert into metric_rollups (series_id, bucket, count, sum, min, max, last)
  select series_id, date_bin('${ROLLUP_BUCKET_SECONDS} seconds', ts, timestamptz '${BUCKET_ORIGIN}'),
         count(*), sum(value), min(value), max(value), (array_agg(value order by ts desc))[1]
    from metric_samples`;
const ROLLUP_UPSERT = `group by 1, 2
  on conflict (series_id, bucket) do update
    set count = excluded.count, sum = excluded.sum, min = excluded.min, max = excluded.max, last = excluded.last`;

/** Recomputes the buckets that got late points, 200 at a time; each batch clears its marks in the same transaction. */
async function rollUpDirty(pool: Pool, samplesFrom?: Date): Promise<number> {
  let buckets = 0;
  // Before samplesFrom, retention has dropped the raw samples. Recomputing such a
  // bucket from the few late points left would overwrite a complete rollup, so
  // its mark is dropped and the rollup stays as it was.
  if (samplesFrom) await pool.query("delete from metric_rollup_dirty where bucket < $1", [samplesFrom]);
  for (;;) {
    const client = await pool.connect();
    try {
      await client.query("begin");
      const { rows } = await client.query<{ bucket: Date }>(
        // Locked in bucket order, the same order ingest marks them in, so the two never deadlock.
        `delete from metric_rollup_dirty where bucket in (
           select bucket from metric_rollup_dirty order by bucket limit 200 for update
         )
         returning bucket`,
      );
      if (rows.length === 0) {
        await client.query("commit");
        return buckets;
      }
      const list = rows.map((r) => r.bucket);
      const res = await client.query(
        `${ROLLUP_SQL}
          where ts >= (select min(b) from unnest($1::timestamptz[]) b)
            and ts < (select max(b) from unnest($1::timestamptz[]) b) + interval '${ROLLUP_BUCKET_SECONDS} seconds'
            and date_bin('${ROLLUP_BUCKET_SECONDS} seconds', ts, timestamptz '${BUCKET_ORIGIN}') = any($1::timestamptz[])
          ${ROLLUP_UPSERT}`,
        [list],
      );
      await client.query("commit");
      buckets += res.rowCount ?? 0;
    } catch (err) {
      await client.query("rollback").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }
}

/** The earliest bucket still waiting for a recompute, if any: retention keeps its day. */
export async function oldestDirtyBucket(pool: Pool): Promise<Date | null> {
  const { rows } = await pool.query<{ bucket: Date | null }>("select min(bucket) as bucket from metric_rollup_dirty");
  return rows[0]?.bucket ?? null;
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
  kind: MetricPointRow["kind"];
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
       select kind, attributes, resource, ts, 1, value, value, value, value
         from metric_points_legacy
        where project = $1 and metric_name = $2 and ts >= $3::timestamptz and ts < least($4::timestamptz, $5::timestamptz)`
    : "";
  const { rows } = await pool.query<{
    kind: MetricPointRow["kind"];
    attributes: Record<string, unknown>;
    resource: Record<string, unknown>;
    ts: Date;
    count: string;
    sum: number;
    min: number;
    max: number;
    last: number;
  }>(
    `with pts (kind, attributes, resource, ts, count, sum, min, max, last) as (
       select s.kind, s.attributes, s.resource, r.bucket, r.count, r.sum, r.min, r.max, r.last
         from metric_rollups r
         join metric_series s on s.id = r.series_id
        where s.project = $1 and s.metric_name = $2 and r.bucket >= $3::timestamptz and r.bucket < least($4::timestamptz, $5::timestamptz)
       union all
       select s.kind, s.attributes, s.resource, p.ts, 1, p.value, p.value, p.value, p.value
         from metric_samples p
         join metric_series s on s.id = p.series_id
        where s.project = $1 and s.metric_name = $2 and p.ts >= greatest($3::timestamptz, $5::timestamptz) and p.ts < $4::timestamptz
       ${legacyBranch}
     ), binned as (
       select kind, attributes, resource, date_bin(make_interval(secs => $6::int), ts, timestamptz '${BUCKET_ORIGIN}') as ts,
              sum(count) as count, sum(sum) as sum, min(min) as min, max(max) as max,
              (array_agg(last order by ts desc))[1] as last
         from pts
        group by 1, 2, 3, 4
     )
     select * from (
       select *, row_number() over (partition by kind, attributes, resource order by ts desc) as n from binned
     ) b where n <= $7::int
     order by kind, attributes::text, resource::text, ts desc`,
    [params.project, params.metricName, params.since, until, rolled, params.stepSeconds, limit],
  );
  const series = new Map<string, MetricBucketSeries>();
  for (const r of rows) {
    const key = `${r.kind}\u0000${JSON.stringify(r.attributes)}\u0000${JSON.stringify(r.resource)}`;
    let s = series.get(key);
    if (!s) {
      s = { kind: r.kind, attributes: r.attributes, resource: r.resource, buckets: [] };
      series.set(key, s);
    }
    const count = Number(r.count);
    s.buckets.push({ ts: r.ts, count, avg: r.sum / count, min: r.min, max: r.max, last: r.last });
  }
  return [...series.values()];
}
