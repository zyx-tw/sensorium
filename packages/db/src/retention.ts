import type { Pool } from "pg";
import { oldestDirtyBucket, rollUpMetrics, rolledUntil } from "./metrics.js";

/**
 * Age-based expiry for the three signal tables.
 *
 * All three are partitioned by UTC day (metric_points in migration 0004, spans
 * and logs in 0006), so expiry is a DROP TABLE per day: constant time, and the
 * space comes back immediately. A DELETE would leave the bytes behind.
 *
 * Meant to run daily from a timer. Every step is idempotent, so a missed run
 * costs disk, not correctness, and the next run catches up.
 */

/**
 * A table partitioned by UTC day into <prefix>_YYYYMMDD, with <prefix>_default
 * as a backstop. The prefix is the table name, except for metric_points_legacy:
 * migration 0008 renamed the old metric_points, and its partitions kept their names.
 */
export type PartitionedTable = "metric_samples" | "metric_rollups" | "metric_points_legacy" | "spans" | "logs";

/** Tables that take new rows, so retention keeps partitions ahead of them. */
export const PARTITIONED_TABLES: readonly PartitionedTable[] = ["metric_samples", "metric_rollups", "spans", "logs"];

export function partitionPrefix(table: PartitionedTable): string {
  return table === "metric_points_legacy" ? "metric_points" : table;
}

export function partitionColumn(table: PartitionedTable): string {
  if (table === "spans") return "start_ts";
  if (table === "metric_rollups") return "bucket";
  return "ts";
}

/** <prefix>_YYYYMMDD; the default partition deliberately does not match. */
function partitionRe(table: PartitionedTable): RegExp {
  return new RegExp(`^${partitionPrefix(table)}_(\\d{4})(\\d{2})(\\d{2})$`);
}

export interface RetentionConfig {
  /** Days of raw metric samples to keep. */
  metricDays: number;
  /** Days of 5-minute metric rollups to keep. */
  rollupDays: number;
  spanDays: number;
  logDays: number;
  /** How many days of partitions to keep pre-created ahead of now. */
  aheadDays: number;
}

export const DEFAULT_RETENTION: RetentionConfig = {
  metricDays: 14,
  rollupDays: 90,
  spanDays: 30,
  logDays: 30,
  aheadDays: 7,
};

/** Reads the config from env, falling back to DEFAULT_RETENTION per field. */
export function retentionConfigFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): RetentionConfig {
  const num = (key: string, fallback: number): number => {
    const raw = env[key];
    if (raw === undefined || raw === "") return fallback;
    const parsed = Number(raw);
    if (!Number.isInteger(parsed) || parsed < 1) {
      throw new Error(
        `${key} must be a positive integer, got ${JSON.stringify(raw)}`,
      );
    }
    return parsed;
  };
  return {
    metricDays: num(
      "SENSORIUM_RETENTION_METRIC_DAYS",
      DEFAULT_RETENTION.metricDays,
    ),
    rollupDays: num("SENSORIUM_RETENTION_ROLLUP_DAYS", DEFAULT_RETENTION.rollupDays),
    spanDays: num("SENSORIUM_RETENTION_SPAN_DAYS", DEFAULT_RETENTION.spanDays),
    logDays: num("SENSORIUM_RETENTION_LOG_DAYS", DEFAULT_RETENTION.logDays),
    aheadDays: num(
      "SENSORIUM_RETENTION_AHEAD_DAYS",
      DEFAULT_RETENTION.aheadDays,
    ),
  };
}

/** UTC midnight of the day `offsetDays` from `from`. */
export function utcDay(from: Date, offsetDays = 0): Date {
  return new Date(
    Date.UTC(
      from.getUTCFullYear(),
      from.getUTCMonth(),
      from.getUTCDate() + offsetDays,
      0,
      0,
      0,
      0,
    ),
  );
}

export function partitionName(day: Date, table: PartitionedTable = "metric_samples"): string {
  const y = day.getUTCFullYear().toString().padStart(4, "0");
  const m = (day.getUTCMonth() + 1).toString().padStart(2, "0");
  const d = day.getUTCDate().toString().padStart(2, "0");
  return `${partitionPrefix(table)}_${y}${m}${d}`;
}

/** Inverse of partitionName; null for anything that is not a daily partition. */
export function partitionDay(name: string, table: PartitionedTable = "metric_samples"): Date | null {
  const m = partitionRe(table).exec(name);
  if (!m) return null;
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
}

/** [start, end) bounds of a day partition, as the SQL literals the DDL wants. */
export function partitionBounds(day: Date): { from: string; to: string } {
  return { from: day.toISOString(), to: utcDay(day, 1).toISOString() };
}

const ISO_UTC_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

/**
 * Quotes an ISO timestamp for inlining into DDL.
 *
 * `CREATE TABLE ... PARTITION OF ... FOR VALUES` is DDL, and Postgres does not
 * accept bind parameters there — passing $1/$2 fails with "bind message
 * supplies 2 parameters, but prepared statement requires 0". These values come
 * from Date.toISOString(), so the assertion is a guard against a future caller
 * rather than against today's input.
 */
function tsLiteral(iso: string): string {
  if (!ISO_UTC_RE.test(iso)) {
    throw new Error(
      `refusing to inline a non-ISO timestamp into DDL: ${JSON.stringify(iso)}`,
    );
  }
  return `'${iso}'`;
}

async function listDayPartitions(pool: Pool, table: PartitionedTable): Promise<string[]> {
  const { rows } = await pool.query<{ relname: string }>(
    `select c.relname
       from pg_inherits i
       join pg_class c on c.oid = i.inhrelid
       join pg_class p on p.oid = i.inhparent
      where p.relname = $1
      order by c.relname`,
    [table],
  );
  const re = partitionRe(table);
  return rows.map((r) => r.relname).filter((n) => re.test(n));
}

/** Creates today's partition and the next `aheadDays`. Idempotent. */
export async function ensurePartitions(
  pool: Pool,
  aheadDays: number,
  now = new Date(),
  table: PartitionedTable = "metric_samples",
): Promise<string[]> {
  // Checked against the catalog rather than leaning on `if not exists`, whose
  // result still reports CREATE for a table that was already there — the
  // report would then claim work it did not do on every single run.
  const existing = new Set(await listDayPartitions(pool, table));
  const created: string[] = [];
  for (let offset = 0; offset <= aheadDays; offset++) {
    const day = utcDay(now, offset);
    const name = partitionName(day, table);
    if (existing.has(name)) continue;
    const { from, to } = partitionBounds(day);
    await pool.query(
      `create table if not exists ${name}
         partition of ${table} for values from (${tsLiteral(from)}) to (${tsLiteral(to)})`,
    );
    created.push(name);
  }
  return created;
}

/** Drops partitions whose whole day is older than the cutoff. */
export async function dropExpiredPartitions(
  pool: Pool,
  keepDays: number,
  now = new Date(),
  table: PartitionedTable = "metric_samples",
  /** Never drop a partition that ends after this instant (samples not rolled up yet). */
  keepAfter?: Date,
): Promise<string[]> {
  const cutoff = utcDay(now, -keepDays);
  const dropped: string[] = [];
  for (const name of await listDayPartitions(pool, table)) {
    const day = partitionDay(name, table);
    // A partition covers [day, day+1), so it is only fully expired once its
    // end is at or before the cutoff. Never drop a day still partly in window.
    if (!day || utcDay(day, 1) > cutoff) continue;
    if (keepAfter && utcDay(day, 1) > keepAfter) continue;
    await pool.query(`drop table ${name}`);
    dropped.push(name);
  }
  return dropped;
}

/**
 * Moves anything that landed in the default partition into a real one.
 *
 * Reachable when a day arrived with no partition pre-created: the timer was
 * down for longer than aheadDays, or a producer sent a timestamp outside the
 * window (a skewed clock). Each day up to aheadDays from now gets one
 * partition, and rows past retention are deleted instead. Rows further ahead (a
 * clock in 2100) stay in default: a partition for them would outlive every
 * retention run. The detach is what makes
 * the re-insert legal: while attached, a row cannot move out of default into
 * an overlapping partition.
 */
export async function drainDefaultPartition(
  pool: Pool,
  table: PartitionedTable = "metric_samples",
  keepDays?: number,
  now = new Date(),
  aheadDays = DEFAULT_RETENTION.aheadDays,
): Promise<number> {
  const defaultPartition = `${partitionPrefix(table)}_default`;
  const column = partitionColumn(table);
  // Rows already past retention are deleted rather than given a partition of their own.
  if (keepDays !== undefined) {
    await pool.query(`delete from ${defaultPartition} where ${column} < $1`, [utcDay(now, -keepDays).toISOString()]);
  }
  const horizon = utcDay(now, aheadDays + 1).toISOString();
  const { rows } = await pool.query<{ n: string }>(
    `select count(*) as n from ${defaultPartition} where ${column} < $1`,
    [horizon],
  );
  const pending = Number(rows[0]?.n ?? 0);
  if (pending === 0) return 0;

  // The day as text: a date column would come back as local midnight in the
  // process timezone, a day off when that is not UTC.
  const { rows: dayRows } = await pool.query<{ day: string }>(
    `select distinct to_char(${column} at time zone 'UTC', 'YYYY-MM-DD') as day
       from ${defaultPartition} where ${column} < $1 order by day`,
    [horizon],
  );

  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query(
      `alter table ${table} detach partition ${defaultPartition}`,
    );
    for (const { day } of dayRows) {
      const utc = new Date(`${day}T00:00:00Z`);
      const { from, to } = partitionBounds(utc);
      await client.query(
        `create table if not exists ${partitionName(utc, table)}
           partition of ${table} for values from (${tsLiteral(from)}) to (${tsLiteral(to)})`,
      );
    }
    // The detached default keeps the parent's column order, so select * lines up.
    await client.query(`insert into ${table} select * from ${defaultPartition} where ${column} < $1`, [horizon]);
    await client.query(`delete from ${defaultPartition} where ${column} < $1`, [horizon]);
    await client.query(
      `alter table ${table} attach partition ${defaultPartition} default`,
    );
    await client.query("commit");
  } catch (err) {
    await client.query("rollback");
    throw err;
  } finally {
    client.release();
  }
  return pending;
}

export interface TableRetention {
  partitionsCreated: string[];
  partitionsDropped: string[];
  defaultRowsRescued: number;
}

export interface RetentionReport {
  tables: Partial<Record<PartitionedTable, TableRetention>>;
  rollup: { from: Date; until: Date; buckets: number };
}

export function keepDaysFor(table: PartitionedTable, config: RetentionConfig): number {
  if (table === "spans") return config.spanDays;
  if (table === "logs") return config.logDays;
  if (table === "metric_rollups") return config.rollupDays;
  return config.metricDays;
}

export async function runRetention(
  pool: Pool,
  config: RetentionConfig = DEFAULT_RETENTION,
  now = new Date(),
): Promise<RetentionReport> {
  // Rollups first: a samples partition is dropped only once it is rolled up.
  // Raw samples before this day are gone or about to be: dropExpiredPartitions uses the same cutoff.
  const rollup = await rollUpMetrics(pool, now, utcDay(now, -config.metricDays));
  // Samples are kept until they are rolled up: up to rolled_until, and before any bucket still marked dirty.
  const dirty = await oldestDirtyBucket(pool);
  const rolledAt = await rolledUntil(pool);
  const rolled = dirty && dirty < rolledAt ? dirty : rolledAt;
  const tables: RetentionReport["tables"] = {};
  for (const table of PARTITIONED_TABLES) {
    // Order matters: rescue before dropping, so a rescued row from an expired
    // day is dropped in the same run instead of lingering a day longer.
    const defaultRowsRescued = await drainDefaultPartition(pool, table, keepDaysFor(table, config), now, config.aheadDays);
    const partitionsCreated = await ensurePartitions(pool, config.aheadDays, now, table);
    const partitionsDropped = await dropExpiredPartitions(
      pool,
      keepDaysFor(table, config),
      now,
      table,
      table === "metric_samples" ? rolled : undefined,
    );
    tables[table] = { partitionsCreated, partitionsDropped, defaultRowsRescued };
  }
  // The pre-0008 metric table takes no new rows: its days only expire.
  const { rows } = await pool.query<{ ok: boolean }>("select to_regclass('metric_points_legacy') is not null as ok");
  if (rows[0]!.ok) {
    const partitionsDropped = await dropExpiredPartitions(pool, config.metricDays, now, "metric_points_legacy");
    tables.metric_points_legacy = { partitionsCreated: [], partitionsDropped, defaultRowsRescued: 0 };
  }
  return { tables, rollup };
}
