import {
  spanKindFromProto,
  statusCodeFromProto,
  type OtlpAnyValue,
  type OtlpKeyValue,
  type OtlpLogsPayload,
  type OtlpMetricsPayload,
  type OtlpNumberDataPoint,
  type OtlpResource,
  type OtlpTracesPayload,
} from "./otlp-json.js";
import { isInboundSpan, type LogRow, type MetricPointRow, type SpanRow } from "./rows.js";

/** Nanosecond epoch string (int64-as-string in OTLP/JSON) → Date, without losing precision to Number(). */
export function nanosToDate(nanos: string | number): Date {
  const n = typeof nanos === "number" ? BigInt(Math.trunc(nanos)) : BigInt(nanos);
  return new Date(Number(n / 1_000_000n));
}

/**
 * The first timestamp that is set. OTLP uses 0 for "unknown", and protobuf omits a zero
 * field, so both 0 and a missing value fall through to the next candidate; with none set,
 * the receive time is used so the row still lands inside retention.
 */
export function firstTime(candidates: Array<string | number | undefined>, receivedAt: Date): Date {
  for (const nanos of candidates) {
    if (nanos === undefined || nanos === null || nanos === "" || nanos === 0 || nanos === "0") continue;
    try {
      return nanosToDate(nanos);
    } catch {
      continue;
    }
  }
  return receivedAt;
}

/** SeverityNumber ranges (logs.proto): 1-4 TRACE, 5-8 DEBUG, 9-12 INFO, 13-16 WARN, 17-20 ERROR, 21-24 FATAL. */
const SEVERITY_NAMES = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"] as const;

/**
 * The stored severity. severityNumber is OTLP's normalized level, so when it is set its
 * short name wins (a "warning" or "Error" text then reads WARN or ERROR); otherwise the
 * producer's text is kept as sent.
 */
export function severityOf(severityNumber: number | undefined, severityText: string | undefined): string | null {
  if (severityNumber !== undefined && severityNumber >= 1 && severityNumber <= 24) {
    return SEVERITY_NAMES[Math.floor((severityNumber - 1) / 4)]!;
  }
  return severityText ? severityText : null;
}

function anyValueToJs(value: OtlpAnyValue | undefined): unknown {
  if (value === undefined) return null;
  if (value.stringValue !== undefined) return value.stringValue;
  if (value.boolValue !== undefined) return value.boolValue;
  if (value.intValue !== undefined) return typeof value.intValue === "string" ? Number(value.intValue) : value.intValue;
  if (value.doubleValue !== undefined) return value.doubleValue;
  if (value.bytesValue !== undefined) return value.bytesValue;
  if (value.arrayValue !== undefined) return value.arrayValue.values.map(anyValueToJs);
  if (value.kvlistValue !== undefined) return keyValuesToObject(value.kvlistValue.values);
  return null;
}

export function keyValuesToObject(kvs: OtlpKeyValue[] | undefined): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const kv of kvs ?? []) {
    out[kv.key] = anyValueToJs(kv.value);
  }
  return out;
}

function resourceToObject(resource: OtlpResource | undefined): Record<string, unknown> {
  return keyValuesToObject(resource?.attributes);
}

/** service.namespace as declared by the client — informational only, never trusted for project scoping. */
export function declaredNamespace(resource: OtlpResource | undefined): string | undefined {
  const attrs = keyValuesToObject(resource?.attributes);
  const ns = attrs["service.namespace"];
  return typeof ns === "string" ? ns : undefined;
}

/**
 * Reads the span's HTTP response status code, preferring the current stable semconv
 * name over the legacy one a lot of instrumentation still emits.
 */
export function httpStatusCodeFromAttributes(attributes: Record<string, unknown>): number | null {
  const raw = attributes["http.response.status_code"] ?? attributes["http.status_code"];
  if (typeof raw === "number") return Number.isFinite(raw) ? raw : null;
  if (typeof raw === "string") {
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/**
 * Maps an OTLP/JSON ExportLogsServiceRequest to our log rows.
 * `project` is caller-supplied (resolved from the ingest bearer token), NOT
 * read from resource attributes — token binding wins over client claims.
 */
export function mapOtlpLogsToRows(project: string, payload: OtlpLogsPayload, receivedAt = new Date()): LogRow[] {
  const rows: LogRow[] = [];
  for (const rl of payload.resourceLogs ?? []) {
    const resource = resourceToObject(rl.resource);
    for (const sl of rl.scopeLogs ?? []) {
      for (const rec of sl.logRecords ?? []) {
        rows.push({
          project,
          ts: firstTime([rec.timeUnixNano, rec.observedTimeUnixNano], receivedAt),
          severity: severityOf(rec.severityNumber, rec.severityText),
          body:
            rec.body?.stringValue ??
            (rec.body !== undefined ? JSON.stringify(anyValueToJs(rec.body)) : null),
          traceId: rec.traceId ?? null,
          spanId: rec.spanId ?? null,
          resource,
          attributes: keyValuesToObject(rec.attributes),
        });
      }
    }
  }
  return rows;
}

/** Maps an OTLP/JSON ExportTraceServiceRequest to our span rows. */
export function mapOtlpTracesToRows(project: string, payload: OtlpTracesPayload, receivedAt = new Date()): SpanRow[] {
  const rows: SpanRow[] = [];
  for (const rs of payload.resourceSpans ?? []) {
    const resource = resourceToObject(rs.resource);
    for (const ss of rs.scopeSpans ?? []) {
      for (const span of ss.spans ?? []) {
        const startTs = firstTime([span.startTimeUnixNano, span.endTimeUnixNano], receivedAt);
        const endTs = firstTime([span.endTimeUnixNano], startTs);
        const attributes = keyValuesToObject(span.attributes);
        const kind = spanKindFromProto(span.kind);
        rows.push({
          project,
          traceId: span.traceId,
          spanId: span.spanId,
          parentSpanId: span.parentSpanId ?? null,
          name: span.name,
          kind,
          startTs,
          endTs,
          durationMs: Math.max(0, endTs.getTime() - startTs.getTime()),
          statusCode: statusCodeFromProto(span.status?.code),
          httpStatusCode: isInboundSpan({ kind, attributes }) ? httpStatusCodeFromAttributes(attributes) : null,
          resource,
          attributes,
        });
      }
    }
  }
  return rows;
}

/**
 * Maps an OTLP/JSON ExportMetricsServiceRequest to our metric point rows.
 *
 * Gauges and sums are one row per point. Histograms, exponential histograms and
 * summaries keep their sum under the metric's own name (kind "histogram", as before)
 * and add `<name>.count`, plus `<name>.min` and `<name>.max` when the point has them;
 * a summary also adds one `<name>.quantile` row per quantile, with the quantile in the
 * row's attributes. Buckets are not stored.
 */
export function mapOtlpMetricsToRows(project: string, payload: OtlpMetricsPayload, receivedAt = new Date()): MetricPointRow[] {
  const rows: MetricPointRow[] = [];
  for (const rm of payload.resourceMetrics ?? []) {
    const resource = resourceToObject(rm.resource);
    for (const sm of rm.scopeMetrics ?? []) {
      for (const metric of sm.metrics ?? []) {
        const row = (
          metricName: string,
          kind: MetricPointRow["kind"],
          dp: { timeUnixNano?: string; startTimeUnixNano?: string; attributes?: OtlpKeyValue[] },
          value: number,
          extra: Record<string, unknown> = {},
        ): MetricPointRow => ({
          project,
          metricName,
          ts: firstTime([dp.timeUnixNano, dp.startTimeUnixNano], receivedAt),
          kind,
          value,
          attributes: { ...keyValuesToObject(dp.attributes), ...extra },
          resource,
        });
        for (const dp of metric.gauge?.dataPoints ?? []) rows.push(row(metric.name, "gauge", dp, numberValue(dp)));
        for (const dp of metric.sum?.dataPoints ?? []) rows.push(row(metric.name, "sum", dp, numberValue(dp)));
        const distributions = [...(metric.histogram?.dataPoints ?? []), ...(metric.exponentialHistogram?.dataPoints ?? [])];
        for (const dp of distributions) {
          rows.push(row(metric.name, "histogram", dp, dp.sum ?? 0));
          rows.push(row(`${metric.name}.count`, "sum", dp, toNumber(dp.count)));
          if (dp.min !== undefined) rows.push(row(`${metric.name}.min`, "gauge", dp, dp.min));
          if (dp.max !== undefined) rows.push(row(`${metric.name}.max`, "gauge", dp, dp.max));
        }
        for (const dp of metric.summary?.dataPoints ?? []) {
          rows.push(row(metric.name, "histogram", dp, dp.sum ?? 0));
          rows.push(row(`${metric.name}.count`, "sum", dp, toNumber(dp.count)));
          for (const q of dp.quantileValues ?? []) {
            rows.push(row(`${metric.name}.quantile`, "gauge", dp, q.value ?? 0, { quantile: q.quantile ?? 0 }));
          }
        }
      }
    }
  }
  return rows;
}

function toNumber(value: string | number | undefined): number {
  if (value === undefined) return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function numberValue(dp: OtlpNumberDataPoint): number {
  return dp.asDouble ?? (dp.asInt !== undefined ? toNumber(dp.asInt) : 0);
}
