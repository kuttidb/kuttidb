import { z } from "zod";

/** Stable Management API error envelope (openapi/management-v1.yaml#/schemas/Error). */
export const errorEnvelopeSchema = z.object({
  error: z.object({
    code: z.string(),
    message: z.string(),
    request_id: z.string().optional()
  })
});

export const collectionMetaSchema = z.object({
  count: z.number(),
  limit: z.number(),
  next_cursor: z.string().nullable().optional(),
  snapshot_revision: z.number().optional(),
  weakly_consistent: z.boolean().optional(),
  truncated: z.boolean().optional(),
  snapshot_generation: z.number().optional()
});

export const capabilitiesSchema = z.object({
  product: z.literal("KuttiDB"),
  server_version: z.string(),
  management_api_contract: z.string(),
  enabled_engines: z.array(z.string()).default([]),
  sse: z.object({ available: z.boolean() }).passthrough().optional(),
  audit: z.object({ required: z.boolean(), healthy: z.boolean() }).passthrough().optional(),
  operations: z.record(z.string(), z.array(z.string())).default({}),
  routing_modes: z.array(z.string()).default([]),
  limits: z.record(z.string(), z.number()).default({}),
  durable_consumer_deliveries: z.boolean().optional(),
  cursors: z.record(z.string(), z.object({ opaque: z.boolean(), ttl_seconds: z.number(), max_live: z.number() }).optional()).optional()
}).passthrough();

export type Capabilities = z.infer<typeof capabilitiesSchema>;
export type CollectionMeta = z.infer<typeof collectionMetaSchema>;

export class ApiError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly status: number,
    readonly requestId?: string
  ) {
    super(message);
    this.name = "ApiError";
  }

  /** Safe, user-actionable presentation per the console error map. */
  get hint(): string | null {
    switch (this.code) {
      case "precondition_failed": return "The resource changed elsewhere. Refresh it and confirm again.";
      case "precondition_required": return "A required revision header was missing. Refresh and retry.";
      case "persistence_unavailable": case "engine_unavailable": return "This engine is unhealthy; mutations are blocked but safe reads remain.";
      case "audit_unavailable": return "The audit trail is unavailable; all mutations are blocked until it recovers.";
      case "operation_in_doubt": return "The operation outcome is unknown. Reconcile the target before retrying — never repeat blindly.";
      case "delivery_expired": return "This delivery lease expired. Its receipt is now immutable.";
      case "rate_limited": return "The server rate-limited this action. Wait for the retry window.";
      case "resource_exhausted": return "A server limit from capabilities was reached.";
      case "idempotency_conflict": return "This idempotency key was already used for a different request. Retry with a fresh key.";
      case "unauthorized": return "This connection was locked. Enter its token to reconnect.";
      default: return null;
    }
  }
}

export type AdminRequest = {
  method?: string;
  body?: unknown;
  idempotencyKey?: string | undefined;
  ifMatch?: string | undefined;
  confirm?: string | undefined;
  accept?: string | undefined;
  signal?: AbortSignal | undefined;
};

export type AdminResponse<T> = { status: number; etag: string | null; json: T };

/**
 * Perform one Management API call through the console gateway. The gateway
 * injects the bearer token; this side never sees it. Identifiers passed in
 * `path` must already be validated opaque IDs (b64u:/d:/j-/gs:/kc: forms).
 */
export async function admin<T = unknown>(profileId: string, path: string, request: AdminRequest = {}): Promise<AdminResponse<T>> {
  const headers: Record<string, string> = { accept: "application/json" };
  if (request.body !== undefined) headers["content-type"] = "application/json";
  if (request.idempotencyKey) headers["idempotency-key"] = request.idempotencyKey;
  if (request.ifMatch) headers["if-match"] = request.ifMatch;
  if (request.confirm) headers["x-kuttidb-confirm"] = request.confirm;
  if (request.accept) headers["accept"] = request.accept;
  const response = await fetch(`/ui-api/connections/${encodeURIComponent(profileId)}/admin/${path}`, {
    method: request.method ?? "GET",
    headers,
    credentials: "same-origin",
    ...(request.body !== undefined ? { body: JSON.stringify(request.body) } : {}),
    ...(request.signal ? { signal: request.signal } : {})
  });
  const etag = response.headers.get("etag");
  const text = await response.text();
  let json: unknown = undefined;
  if (text.length > 0) {
    try { json = JSON.parse(text); } catch { json = undefined; }
  }
  if (!response.ok) {
    const parsed = errorEnvelopeSchema.safeParse(json);
    if (parsed.success) {
      throw new ApiError(parsed.data.error.code, parsed.data.error.message, response.status, parsed.data.error.request_id);
    }
    const gatewayCode = (json as { error?: { code?: string } } | undefined)?.error?.code ?? "upstream_unavailable";
    throw new ApiError(gatewayCode, "The console gateway could not complete this request.", response.status);
  }
  return { status: response.status, etag, json: json as T };
}

export function dataOf<T>(response: AdminResponse<{ data?: T }>): T {
  return response.json.data as T;
}

export function metaOf(response: AdminResponse<{ meta?: unknown }>): CollectionMeta | null {
  const parsed = collectionMetaSchema.safeParse(response.json.meta);
  return parsed.success ? parsed.data : null;
}

export async function list<T>(profileId: string, path: string, signal?: AbortSignal): Promise<{ data: T[]; meta: CollectionMeta | null; etag: string | null }> {
  const response = await admin<{ data?: T[]; meta?: unknown }>(profileId, path, { signal });
  return { data: response.json.data ?? [], meta: metaOf(response), etag: response.etag };
}

/** Fresh UUID per user intent; kept across retries of the same intent only. */
export function newIdempotencyKey(): string {
  return crypto.randomUUID();
}
