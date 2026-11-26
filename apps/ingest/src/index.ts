import { getPool } from "@sensorium/db";
import { parseMetricDrop } from "./metric-drop.js";
import { createIngestApp, ingestOptionsFromEnv } from "./server.js";

const port = Number(process.env.PORT ?? 8787);
// A database that hangs (recovery, a full disk, a paused host) must still answer the
// exporter within its own timeout, so the export fails with a retryable 503.
const pool = getPool({ connectionTimeoutMillis: 5_000, query_timeout: 8_000 });
const limits = ingestOptionsFromEnv();
const app = createIngestApp(pool, {
  landing: process.env.SENSORIUM_LANDING === "1",
  dropMetric: parseMetricDrop(process.env.SENSORIUM_METRIC_DROP),
  ...limits,
});

// A gzip body is checked against the cap again after decompression (server.ts).
Bun.serve({ port, fetch: app.fetch, maxRequestBodySize: limits.maxBodyBytes });

console.log(`sensorium ingest listening on :${port}`);
