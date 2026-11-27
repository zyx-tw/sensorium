import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { buildSpanTree } from "@sensorium/core";
import {
  errorSummary,
  listMetricNames,
  listProjects,
  listTraces,
  queryLogs,
  queryMetrics,
  queryTraceSpans,
  searchLogs,
  storageStats,
  topSources,
  type Pool,
} from "@sensorium/db";
import { z } from "zod";

/**
 * Every tool reads telemetry and changes nothing, so all of them carry the same hints:
 * a client may run them without asking, and calling one twice gives the same answer.
 */
const READ_ONLY = { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false } as const;

/**
 * Tool results are `{ result, truncated? }`: `result` is what the tool reads, and
 * `truncated` is true when a list stopped at its limit, so a caller knows to narrow the
 * window or raise the limit. The text content is the same JSON, compact.
 */
const OUTPUT = z.object({
  result: z.unknown(),
  truncated: z.boolean().optional().describe("true when the list stopped at its limit"),
});

function result(data: unknown, limit?: number): CallToolResult & { structuredContent: z.infer<typeof OUTPUT> } {
  const structuredContent: z.infer<typeof OUTPUT> = { result: data };
  if (limit !== undefined && Array.isArray(data)) structuredContent.truncated = data.length >= limit;
  // The text stays the bare data, as before structured output: callers such as the hum band parse it.
  return { content: [{ type: "text", text: JSON.stringify(data) }], structuredContent };
}

/** Registers the read-only observability tools. All reads are cross-project: every authenticated reader sees every project. */
export function registerTools(server: McpServer, pool: Pool): void {
  server.registerTool(
    "list_projects",
    {
      title: "List projects",
      description:
        "Lists every registered project with its creation and last-seen time (last ingest, accurate to about 30 seconds).",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
    },
    async () => result(await listProjects(pool)),
  );

  server.registerTool(
    "storage_stats",
    {
      title: "Storage stats",
      description:
        "Health of sensorium itself: database size, size and partition range of each table (oldest and newest day " +
        "kept), rows waiting in default partitions, and the projects that have stopped sending. Check it when " +
        "data looks missing: a full disk or a stopped retention sweep shows here first.",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
    },
    async () => result(await storageStats(pool)),
  );

  server.registerTool(
    "query_logs",
    {
      title: "Query logs",
      description:
        "Queries logs for a project within a time window, optionally filtered by severity, trace_id, a body substring, " +
        "source IP (client.address), or route (http.route). Returns full attributes per row, including " +
        "client.address/geo.country/geo.city/geo.region/http.route when the producer sets them.",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
      inputSchema: z.object({
              project: z.string(),
              since: z.string().datetime().describe("ISO 8601 timestamp, inclusive lower bound"),
              until: z.string().datetime().optional(),
              severity: z.string().optional(),
              contains: z.string().optional().describe("substring to match against log body (case-insensitive)"),
              traceId: z.string().optional(),
              ip: z.string().optional().describe("exact match against the client.address attribute"),
              route: z.string().optional().describe("exact match against the http.route attribute"),
              limit: z.number().int().positive().max(1000).optional(),
            }),
    },
    async ({ project, since, until, severity, contains, traceId, ip, route, limit }) => {
      const rows = await queryLogs(pool, {
        project,
        since: new Date(since),
        until: until ? new Date(until) : undefined,
        severity,
        contains,
        traceId,
        ip,
        route,
        limit,
      });
      return result(rows, Math.min(limit ?? 100, 1000));
    },
  );

  server.registerTool(
    "query_traces",
    {
      title: "Query trace",
      description: "Fetches every span for a trace_id in a project and assembles them into a parent/child tree.",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
      inputSchema: z.object({
              project: z.string(),
              traceId: z.string(),
            }),
    },
    async ({ project, traceId }) => {
      const spans = await queryTraceSpans(pool, { project, traceId });
      return result(buildSpanTree(spans));
    },
  );

  server.registerTool(
    "list_traces",
    {
      title: "List recent traces",
      description:
        "Browses the most recent traces for a project without needing a traceId up front — one row per " +
        "trace_id, newest first: name/http.route/http_status_code from the trace's inbound request span, " +
        "plus client.address/geo.country/geo.city/geo.region backfilled from ANY span in the same trace " +
        "(some producers, e.g. Vercel, put client.address/geo.* on a sibling span — like a root layout " +
        "render — rather than the request span itself, so this joins across the trace to fill them in). " +
        "Excludes outbound spans (this service's own fetch/db calls, e.g. calls to Supabase) from the " +
        "representative pick so it only shows requests this service actually served. Feed a traceId from " +
        "here into query_traces for the full span tree of one request.",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
      inputSchema: z.object({
              project: z.string(),
              windowMinutes: z.number().int().positive().max(7 * 24 * 60).default(60),
              limit: z.number().int().positive().max(1000).default(30),
            }),
    },
    async ({ project, windowMinutes, limit }) =>
      result(await listTraces(pool, { project, windowMinutes, limit })),
  );

  server.registerTool(
    "error_summary",
    {
      title: "Error summary",
      description:
        "Counts ERROR/FATAL logs and error-status spans for a project over a recent time window, with the top repeated log messages.",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
      inputSchema: z.object({
              project: z.string(),
              windowMinutes: z.number().int().positive().max(7 * 24 * 60).default(60),
            }),
    },
    async ({ project, windowMinutes }) => result(await errorSummary(pool, { project, windowMinutes })),
  );

  server.registerTool(
    "top_sources",
    {
      title: "Top request sources",
      description:
        "Attack/traffic attribution: aggregates spans AND logs in a project over a recent time window by " +
        "source IP (client.address) — some producers (e.g. Vercel) attach client.address/geo.* to a log " +
        "record (429/401/error) rather than the span, so both are read. Returns each IP's geo " +
        "(country/city/region — most recent sighting across either signal) and total request/event count " +
        "from that same reading. Its top 5 routes (http.route) and error count are a trace-level join: on " +
        "Vercel, client.address/geo.* often land on a root layout span with no route, while the route/status " +
        "sit on a sibling request span in the SAME trace — so both are pulled from any inbound span across " +
        "every trace the IP touched, not just the row that happened to carry the IP. Sorted by request count " +
        "descending. Also returns a by-country rollup for 'which countries are hitting us' at a glance.",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
      inputSchema: z.object({
              project: z.string(),
              windowMinutes: z.number().int().positive().max(7 * 24 * 60).default(60),
              limit: z.number().int().positive().max(1000).default(20).describe("max number of source IPs to return"),
            }),
    },
    async ({ project, windowMinutes, limit }) => result(await topSources(pool, { project, windowMinutes, limit })),
  );

  server.registerTool(
    "query_metrics",
    {
      title: "Query metrics",
      description:
        "Reads OTLP metrics for a project within a time window. Without metricName: catalogs every metric " +
        "seen in the window (kind, point count, first/last seen, last value) — start here to discover what a " +
        "producer reports. With metricName: whole-window min/max/avg/count plus raw points, newest first, up " +
        "to limit (summary ignores limit). A metric with several series (e.g. hostmetrics' per-cpu/per-state " +
        "points) interleaves them — the labels live in each point's attributes, so group client-side for " +
        "per-series numbers. A histogram or summary is stored as its sum under the metric name plus <name>.count, .min, .max and .quantile.",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
      inputSchema: z.object({
              project: z.string(),
              metricName: z.string().optional().describe("exact metric name; omit to list which metrics exist"),
              since: z.string().datetime().describe("ISO 8601 timestamp, inclusive lower bound"),
              until: z.string().datetime().optional(),
              limit: z
                .number()
                .int()
                .positive()
                .max(1000)
                .optional()
                .describe("max points returned when metricName is set (default 100)"),
            }),
    },
    async ({ project, metricName, since, until, limit }) => {
      const window = { project, since: new Date(since), until: until ? new Date(until) : undefined };
      if (!metricName) return result(await listMetricNames(pool, window));
      return result(await queryMetrics(pool, { ...window, metricName, limit }));
    },
  );

  server.registerTool(
    "search",
    {
      title: "Search logs",
      description: "v0 substring search (ILIKE) over log body and attributes for a project since a given time.",
      annotations: READ_ONLY,
      outputSchema: OUTPUT,
      inputSchema: z.object({
              project: z.string(),
              q: z.string(),
              since: z.string().datetime(),
              limit: z.number().int().positive().max(1000).optional(),
            }),
    },
    async ({ project, q, since, limit }) =>
      result(await searchLogs(pool, { project, q, since: new Date(since), limit }), Math.min(limit ?? 100, 1000)),
  );
}
