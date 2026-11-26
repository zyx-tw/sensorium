/**
 * OTLP/HTTP responses (opentelemetry-proto docs/specification.md, "OTLP/HTTP Response").
 * A success is an empty Export*ServiceResponse; a failure carries a google.rpc.Status,
 * both encoded in the request's own format.
 */

export const JSON_CONTENT_TYPE = "application/json";
export const PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

/** google.rpc.Code for each HTTP status this receiver returns. */
const RPC_CODE: Record<number, number> = {
  400: 3, // INVALID_ARGUMENT
  401: 16, // UNAUTHENTICATED
  404: 5, // NOT_FOUND
  413: 3, // INVALID_ARGUMENT
  415: 3, // INVALID_ARGUMENT
  429: 8, // RESOURCE_EXHAUSTED
  503: 14, // UNAVAILABLE
};

function varint(value: number): number[] {
  const out: number[] = [];
  let v = value >>> 0;
  while (v > 0x7f) {
    out.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  out.push(v);
  return out;
}

/** Encodes google.rpc.Status { int32 code = 1; string message = 2; }. */
export function encodeRpcStatus(code: number, message: string): Uint8Array {
  const text = new TextEncoder().encode(message);
  return new Uint8Array([0x08, ...varint(code), 0x12, ...varint(text.length), ...text]);
}

export type Format = "json" | "protobuf";

/** Seconds a client should wait before retrying a 429 or 503. */
export const RETRY_AFTER_SECONDS = 5;

export function success(format: Format, extra: Record<string, unknown> = {}): Response {
  if (format === "protobuf") {
    return new Response(new Uint8Array(), { status: 200, headers: { "content-type": PROTOBUF_CONTENT_TYPE } });
  }
  return new Response(JSON.stringify(extra), { status: 200, headers: { "content-type": JSON_CONTENT_TYPE } });
}

export function failure(format: Format, status: number, message: string): Response {
  const headers: Record<string, string> = {};
  if (status === 429 || status === 503) headers["retry-after"] = String(RETRY_AFTER_SECONDS);
  if (format === "protobuf") {
    headers["content-type"] = PROTOBUF_CONTENT_TYPE;
    return new Response(encodeRpcStatus(RPC_CODE[status] ?? 2, message), { status, headers });
  }
  headers["content-type"] = JSON_CONTENT_TYPE;
  return new Response(JSON.stringify({ code: RPC_CODE[status] ?? 2, message, error: message }), { status, headers });
}
