import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Pool, PoolClient } from "pg";

const MIGRATIONS_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "migrations");

/** pg_advisory_lock key held while migrations run ("sensoriu" as ASCII). */
const MIGRATION_LOCK = 0x73656e736f726975n.toString();

const BOOTSTRAP_SQL = `
create table if not exists schema_migrations (
  filename text primary key,
  applied_at timestamptz not null default now()
);
`;

/** Applies every *.sql file in migrations/ that isn't already recorded in schema_migrations. Safe to re-run. */
export async function runMigrations(
  pool: Pool,
  { until }: { until?: string } = {},
): Promise<{ applied: string[]; skipped: string[] }> {
  // One migrator at a time per database: two processes (or two test files)
  // starting together would otherwise both apply the same file.
  const lock = await pool.connect();
  try {
    await lock.query("select pg_advisory_lock($1)", [MIGRATION_LOCK]);
    return await migrateLocked(lock, until);
  } finally {
    await lock.query("select pg_advisory_unlock($1)", [MIGRATION_LOCK]).catch(() => {});
    lock.release();
  }
}

async function migrateLocked(client: PoolClient, until: string | undefined): Promise<{ applied: string[]; skipped: string[] }> {
  await client.query(BOOTSTRAP_SQL);

  // `until` (inclusive) stops early, so a test can build an older schema and then migrate it.
  const files = (await readdir(MIGRATIONS_DIR))
    .filter((f) => f.endsWith(".sql") && (until === undefined || f <= until))
    .sort();
  const { rows } = await client.query<{ filename: string }>("select filename from schema_migrations");
  const applied = new Set(rows.map((r) => r.filename));

  const newlyApplied: string[] = [];
  const skipped: string[] = [];

  for (const file of files) {
    if (applied.has(file)) {
      skipped.push(file);
      continue;
    }
    const sql = await readFile(path.join(MIGRATIONS_DIR, file), "utf8");
    try {
      await client.query("begin");
      await client.query(sql);
      await client.query("insert into schema_migrations (filename) values ($1)", [file]);
      await client.query("commit");
    } catch (err) {
      await client.query("rollback");
      throw new Error(`migration ${file} failed: ${(err as Error).message}`, { cause: err });
    }
    newlyApplied.push(file);
  }

  return { applied: newlyApplied, skipped };
}
