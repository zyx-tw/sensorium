import { gunzipSync } from "node:zlib";
import {
  mapOtlpLogsToRows,
  mapOtlpMetricsToRows,
  mapOtlpTracesToRows,
  type OtlpLogsPayload,
  type OtlpMetricsPayload,
  type OtlpTracesPayload,
} from "@sensorium/core";
import {
  databaseReady,
  insertLogs,
  insertMetricPoints,
  insertSpans,
  recordProjectRejection,
  touchProjectLastSeen,
  type Pool,
} from "@sensorium/db";
import { createIngestAuth, type IngestAuth } from "./auth.js";
import { serveLanding } from "./landing.js";
import { decodeLogsRequestProtobuf, decodeMetricsRequestProtobuf, decodeTraceRequestProtobuf } from "./otlp-protobuf.js";
import { failure, JSON_CONTENT_TYPE, PROTOBUF_CONTENT_TYPE, success, type Format } from "./otlp-response.js";

/** Default cap on a decompressed request body, the size the OTLP spec recommends. */
export const DEFAULT_MAX_BODY_BYTES = 64 * 1024 * 1024;
/** Default number of exports handled at once before new ones get a 429. */
export const DEFAULT_MAX_IN_FLIGHT = 32;
/** projects.last_seen_at is written at most this often per project. */
export const LAST_SEEN_INTERVAL_MS = 30_000;

export interface IngestOptions {
  /** Also serve the zyx.tw landing page at / (see ./landing.ts). */
  landing?: boolean;
  /** Discards matching metric points before insert (see ./metric-drop.ts). */
  dropMetric?: (metricName: string) => boolean;
  maxBodyBytes?: number;
  maxInFlight?: number;
  auth?: IngestAuth;
  now?: () => number;
}

/** The request body could not be read as an OTLP export: never retry it. */
class BadRequest extends Error {
  constructor(
    message: string,
    readonly status = 400,
  ) {
    super(message);
  }
}

/**
 * Whether a database error means the data itself is bad (retrying cannot help) rather
 * than the database being unavailable. SQLSTATE class 22 is a data exception and 23 an
 * integrity violation; everything else (disk full, recovery mode, lost connection,
 * timeouts) is worth retrying.
 */
export function isPermanentDbError(err: unknown): boolean {
  const code = (err as { code?: unknown } | null)?.code;
  return typeof code === "string" && /^2[23][0-9A-Z]{3}$/.test(code);
}

function parseOptionalInt(value: string | undefined, fallback: number): number {
  if (value === undefined || value === "") return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n <= 0) throw new Error(`expected a positive integer, got "${value}"`);
  return n;
}

export function ingestOptionsFromEnv(env: Record<string, string | undefined> = process.env) {
  return {
    maxBodyBytes: parseOptionalInt(env.SENSORIUM_INGEST_MAX_BODY_BYTES, DEFAULT_MAX_BODY_BYTES),
    maxInFlight: parseOptionalInt(env.SENSORIUM_INGEST_MAX_IN_FLIGHT, DEFAULT_MAX_IN_FLIGHT),
  };
}

/**
 * Builds the OTLP/HTTP receiver as a Web Standard fetch handler (works with Bun.serve directly).
 *
 * Status codes follow the OTLP/HTTP spec: exporters retry only 429, 502, 503 and 504, so a
 * database outage answers 503 (the batch comes back later) and only a body that can never
 * be stored answers 400.
 */
export function createIngestApp(pool: Pool, options: IngestOptions = {}) {
  const {
    landing = false,
    dropMetric = () => false,
    maxBodyBytes = DEFAULT_MAX_BODY_BYTES,
    maxInFlight = DEFAULT_MAX_IN_FLIGHT,
    now = Date.now,
  } = options;
  const auth = options.auth ?? createIngestAuth(pool, now);
  const lastSeenWritten = new Map<string, number>();
  const lastRejectionWritten = new Map<string, number>();
  let inFlight = 0;

  async function readBody(req: Request): Promise<Uint8Array> {
    const encoding = (req.headers.get("content-encoding") ?? "identity").trim().toLowerCase();
    if (encoding !== "identity" && encoding !== "gzip") {
      throw new BadRequest(`unsupported content-encoding "${encoding}"; expected gzip or none`, 415);
    }
    const raw = new Uint8Array(await req.arrayBuffer());
    if (raw.byteLength > maxBodyBytes) throw new BadRequest("request body too large", 413);
    if (encoding === "identity") return raw;
    try {
      return gunzipSync(raw, { maxOutputLength: maxBodyBytes });
    } catch (err) {
      if ((err as { code?: string }).code === "ERR_BUFFER_TOO_LARGE") {
        throw new BadRequest("decompressed request body too large", 413);
      }
      throw new BadRequest("request body is not valid gzip");
    }
  }

  function decode<T>(body: Uint8Array, format: Format, protobuf: (bytes: Uint8Array) => T): T {
    try {
      return format === "protobuf" ? protobuf(body) : (JSON.parse(new TextDecoder().decode(body)) as T);
    } catch {
      throw new BadRequest(`request body is not a valid OTLP ${format} export`);
    }
  }

  async function touchLastSeen(project: string): Promise<void> {
    const last = lastSeenWritten.get(project) ?? 0;
    if (now() - last < LAST_SEEN_INTERVAL_MS) return;
    lastSeenWritten.set(project, now());
    await touchProjectLastSeen(pool, project).catch((err: unknown) => {
      lastSeenWritten.delete(project);
      console.error("ingest: last_seen update failed:", err);
    });
  }

  /** Notes a refused export on the project (at most once per interval), so storage_stats can show it. */
  async function noteRejection(project: string, status: number, reason: string): Promise<void> {
    const last = lastRejectionWritten.get(project) ?? 0;
    if (now() - last < LAST_SEEN_INTERVAL_MS) return;
    lastRejectionWritten.set(project, now());
    await recordProjectRejection(pool, project, status, reason).catch((err: unknown) => {
      lastRejectionWritten.delete(project);
      console.error("ingest: recording a rejection failed:", err);
    });
  }

  /**
   * Runs one export: content type, auth, body, mapping (any failure there is the
   * client's: 4xx) and then the insert (a failure there is retryable unless the
   * database says the data is invalid).
   */
  async function handleExport(req: Request, store: (project: string, body: Uint8Array, format: Format) => Promise<number>) {
    // Content-Type dictates the wire format; anything else (including missing) is
    // rejected outright rather than guessed: silently trying to protobuf-decode an
    // unrecognized body risks parsing garbage into rows without ever throwing.
    const contentType = (req.headers.get("content-type") ?? "").split(";")[0]!.trim().toLowerCase();
    if (contentType !== JSON_CONTENT_TYPE && contentType !== PROTOBUF_CONTENT_TYPE) {
      return failure(
        "json",
        415,
        `unsupported content-type "${contentType || "(missing)"}"; expected ${JSON_CONTENT_TYPE} or ${PROTOBUF_CONTENT_TYPE}`,
      );
    }
    const format: Format = contentType === PROTOBUF_CONTENT_TYPE ? "protobuf" : "json";

    if (inFlight >= maxInFlight) return failure(format, 429, "too many exports in flight");
    inFlight++;
    try {
      // Token -> project binding applies identically regardless of wire format.
      let project: string | null;
      try {
        project = await auth.authenticate(req);
      } catch (err) {
        console.error("ingest: token lookup failed:", err);
        return failure(format, 503, "storage unavailable");
      }
      if (!project) return failure(format, 401, "unauthorized");

      let inserted: number;
      try {
        inserted = await store(project, await readBody(req), format);
      } catch (err) {
        if (err instanceof BadRequest) {
          await noteRejection(project, err.status, err.message);
          return failure(format, err.status, err.message);
        }
        if (isPermanentDbError(err)) {
          console.error("ingest: rejected by the database:", err);
          await noteRejection(project, 400, `the database rejected the data (${(err as { code: string }).code})`);
          return failure(format, 400, "the database rejected the data");
        }
        console.error("ingest: storage error:", err);
        return failure(format, 503, "storage unavailable");
      }
      await touchLastSeen(project);
      return success(format, { ok: true, inserted });
    } finally {
      inFlight--;
    }
  }

  /** Wraps a mapping step so a payload it cannot read is a 400, not a 503. */
  function mapped<T>(map: () => T): T {
    try {
      return map();
    } catch (err) {
      throw new BadRequest(`request body is not a valid OTLP export: ${(err as Error).message}`);
    }
  }

  async function fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);

    if (landing) {
      const page = serveLanding(req, url);
      if (page) return page;
    }

    if (req.method === "GET" && url.pathname === "/health") {
      return Response.json({ status: "ok" });
    }

    // Unlike /health, /ready fails while the database does not answer: the state in which
    // every export gets a 503. Compose healthchecks use it.
    if (req.method === "GET" && url.pathname === "/ready") {
      return (await databaseReady(pool))
        ? Response.json({ status: "ready" })
        : Response.json({ status: "database unavailable" }, { status: 503 });
    }

    if (req.method === "POST" && url.pathname === "/v1/logs") {
      return handleExport(req, (project, body, format) => {
        const payload = decode<OtlpLogsPayload>(body, format, decodeLogsRequestProtobuf);
        return insertLogs(pool, mapped(() => mapOtlpLogsToRows(project, payload)));
      });
    }

    if (req.method === "POST" && url.pathname === "/v1/traces") {
      return handleExport(req, (project, body, format) => {
        const payload = decode<OtlpTracesPayload>(body, format, decodeTraceRequestProtobuf);
        return insertSpans(pool, mapped(() => mapOtlpTracesToRows(project, payload)));
      });
    }

    if (req.method === "POST" && url.pathname === "/v1/metrics") {
      return handleExport(req, (project, body, format) => {
        const payload = decode<OtlpMetricsPayload>(body, format, decodeMetricsRequestProtobuf);
        const rows = mapped(() => mapOtlpMetricsToRows(project, payload)).filter((row) => !dropMetric(row.metricName));
        return insertMetricPoints(pool, rows);
      });
    }

    return Response.json({ error: "not found" }, { status: 404 });
  }

  return { fetch };
}
