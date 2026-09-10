import { z } from "zod";

/**
 * Exact upstream method/path allowlist for the console gateway proxy, plus
 * response validation for the atomic job completion resources. Everything not
 * listed here is refused before any upstream byte is sent; this is the
 * same-origin boundary that keeps the browser token-confined to the gateway.
 *
 * Contract: openapi/management-v1.yaml (v1), including the atomic job
 * completion family (keyspaces/durable, job-completions, durable-operations,
 * completion-mode queue-consumer deliveries).
 */

/** Opaque Management API identifier segment forms. */
const B64U_ID = "b64u%3A[A-Za-z0-9_-]+"; // percent-encoded colon may arrive either way
const B64U_ID_RAW = "b64u:[A-Za-z0-9_-]+";
const ID = `(?:${B64U_ID}|${B64U_ID_RAW})`;
const UUID = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
const OPAQUE_ID = "[A-Za-z0-9_.:%-]{1,128}";
const NUMBER = "[0-9]+";

type Route = readonly [method: string, pattern: string];

/** Every upstream path the console may request, one row per (method, shape). */
export const UPSTREAM_ROUTES: readonly Route[] = [
  // Discovery and maintenance
  ["GET", "/capabilities"],
  ["GET", "/status"],
  ["GET", "/jobs"],
  ["GET", `/jobs/${OPAQUE_ID}`],
  ["DELETE", `/jobs/${OPAQUE_ID}`],
  ["GET", "/maintenance"],
  ["POST", "/maintenance/(?:keyspace|queue|stream)-checkpoint"],
  ["POST", "/maintenance/checkpoint-all"],

  // Keyspaces: default cache + single-flight claims
  ["GET", "/keyspaces"],
  ["GET", "/keyspaces/default"],
  ["GET", "/keyspaces/default/entries"],
  ["GET", `/keyspaces/default/entries/${ID}`],
  ["PUT", `/keyspaces/default/entries/${ID}`],
  ["DELETE", `/keyspaces/default/entries/${ID}`],
  ["POST", "/keyspaces/default/entries:batch-get"],
  ["POST", "/keyspaces/default/entries:batch-put"],
  ["POST", "/keyspaces/default/entries:batch-delete"],
  ["POST", "/keyspaces/default/claims"],
  ["GET", `/keyspaces/default/claims/${OPAQUE_ID}`],
  ["POST", `/keyspaces/default/claims/${OPAQUE_ID}:complete`],
  ["POST", `/keyspaces/default/claims/${OPAQUE_ID}:release`],
  ["POST", `/keyspaces/default/entries/${ID}:get-or-refresh`],

  // Atomic job completion: Durable state (never the default cache)
  ["GET", "/keyspaces/durable"],
  ["GET", "/keyspaces/durable/entries"],
  ["GET", `/keyspaces/durable/entries/${ID}`],
  ["PUT", `/keyspaces/durable/entries/${ID}`],
  ["DELETE", `/keyspaces/durable/entries/${ID}`],
  ["GET", "/job-completions"],
  ["POST", "/job-completions"],
  ["GET", `/job-completions/${UUID}`],
  ["GET", `/durable-operations/${UUID}`],

  // Queues and completion-capable consumer deliveries
  ["GET", "/queues"],
  ["POST", "/queues"],
  ["GET", `/queues/${ID}`],
  ["PATCH", `/queues/${ID}`],
  ["DELETE", `/queues/${ID}`],
  ["POST", `/queues/${ID}:purge`],
  ["GET", `/queues/${ID}/messages`],
  ["POST", `/queues/${ID}/messages`],
  ["POST", `/queues/${ID}/messages:batch`],
  ["GET", `/queues/${ID}/messages/${NUMBER}`],
  ["POST", `/queues/${ID}/deliveries`],
  ["POST", `/queues/${ID}/deliveries:batch`],
  ["GET", `/queues/${ID}/deliveries/${OPAQUE_ID}`],
  ["POST", `/queues/${ID}/deliveries/${OPAQUE_ID}:ack`],
  ["POST", `/queues/${ID}/deliveries/${OPAQUE_ID}:nack`],
  ["POST", `/queues/${ID}/deliveries:ack-batch`],
  ["POST", `/queues/${ID}/deliveries:nack-batch`],

  // Durable queue consumers (named consumer deliveries for completion)
  ["GET", "/queue-consumers"],
  ["POST", "/queue-consumers"],
  ["GET", `/queue-consumers/${ID}`],
  ["DELETE", `/queue-consumers/${ID}`],
  ["POST", `/queue-consumers/${ID}/deliveries`],

  // Legacy atomic cache+message operations
  ["POST", "/atomic-operations"],

  // Streams and consumer groups
  ["GET", "/streams"],
  ["POST", "/streams"],
  ["GET", `/streams/${ID}`],
  ["PATCH", `/streams/${ID}`],
  ["DELETE", `/streams/${ID}`],
  ["GET", `/streams/${ID}/partitions`],
  ["GET", `/streams/${ID}/partitions/${NUMBER}/records`],
  ["GET", `/streams/${ID}/partitions/${NUMBER}/records:tail`],
  ["GET", `/streams/${ID}/partitions/${NUMBER}/records/${NUMBER}`],
  ["POST", `/streams/${ID}/partitions/${NUMBER}:truncate`],
  ["POST", `/streams/${ID}/records`],
  ["POST", `/streams/${ID}/records:batch`],
  ["GET", "/consumer-groups"],
  ["GET", `/streams/${ID}/consumer-groups`],
  ["GET", `/streams/${ID}/consumer-groups/${ID}`],
  ["GET", `/streams/${ID}/consumer-groups/${ID}/members`],
  ["GET", `/streams/${ID}/consumer-groups/${ID}/offsets`],
  ["PUT", `/streams/${ID}/consumer-groups/${ID}/offsets/${NUMBER}`],
  ["POST", `/streams/${ID}/consumer-groups/${ID}/offsets:batch`],
  ["POST", `/streams/${ID}/consumer-groups/${ID}:reset-offsets`],
  ["POST", `/streams/${ID}/consumer-groups/${ID}/sessions`],
  ["GET", `/streams/${ID}/consumer-groups/${ID}/sessions/${OPAQUE_ID}`],
  ["POST", `/streams/${ID}/consumer-groups/${ID}/sessions/${OPAQUE_ID}:heartbeat`],
  ["POST", `/streams/${ID}/consumer-groups/${ID}/sessions/${OPAQUE_ID}:leave`],
  ["GET", `/streams/${ID}/consumer-groups/${ID}/sessions/${OPAQUE_ID}/records`],
  ["POST", `/streams/${ID}/consumer-groups/${ID}/sessions/${OPAQUE_ID}/offsets:commit`],

  // Routing
  ["GET", "/routing/routers"],
  ["POST", "/routing/routers"],
  ["GET", `/routing/routers/${ID}`],
  ["PATCH", `/routing/routers/${ID}`],
  ["DELETE", `/routing/routers/${ID}`],
  ["GET", `/routing/routers/${ID}/routes`],
  ["POST", `/routing/routers/${ID}/routes`],
  ["GET", `/routing/routers/${ID}/routes/${ID}`],
  ["DELETE", `/routing/routers/${ID}/routes/${ID}`],
  ["POST", `/routing/routers/${ID}/messages`],
  ["POST", "/routing/default/messages"]
];

/** True when the console gateway may forward this exact method+path upstream. */
export function isAllowedUpstreamRequest(method: string, relativePath: string): boolean {
  if (relativePath.length === 0 || relativePath.includes("//")) return false;
  const rawPath = relativePath.split("?")[0] ?? "";
  if (rawPath.startsWith("/") || rawPath.includes("../")) return false;
  const normalized = `/${rawPath}`;
  for (const [allowedMethod, pattern] of UPSTREAM_ROUTES) {
    if (allowedMethod === method && new RegExp(`^${pattern}$`).test(normalized)) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Response validation for the new (security-sensitive) resources. 64-bit
// identity/version/timestamp fields must arrive as decimal strings; binary
// envelopes as {"encoding":"base64","data":...}.
// ---------------------------------------------------------------------------

const decimalString = z.string().regex(/^[0-9]+$/);
const b64uId = z.string().regex(/^b64u:[A-Za-z0-9_-]+$/);
const uuid = z.string().uuid();
const binaryEnvelope = z.object({ encoding: z.literal("base64"), data: z.string() });

const durableKeyspaceResponse = z.object({
  data: z.object({
    name: z.literal("durable"),
    evictable: z.literal(false),
    entry_count: z.number(),
    live_bytes: z.number(),
    capacity_bytes: decimalString,
    persistence_healthy: z.boolean()
  }).passthrough()
}).passthrough();

const durableEntryMeta = z.object({
  entry_id: b64uId,
  key_bytes: z.number(),
  version: decimalString,
  last_commit_id: decimalString,
  value_size: z.number()
});

const durableEntriesResponse = z.object({ data: z.array(durableEntryMeta) }).passthrough();

const durableEntryResponse = z.object({
  entry_id: b64uId,
  key: b64uId,
  value: binaryEnvelope,
  version: decimalString,
  last_commit_id: decimalString
}).passthrough();

const durableMutationResponse = z.object({
  operation_id: uuid,
  commit_id: decimalString,
  state_version: decimalString,
  completed_at: decimalString,
  receipt_expires_at: decimalString,
  replayed: z.boolean()
}).passthrough();

const completionDeliveryResponse = z.object({
  delivery: z.object({
    store_id: b64uId,
    queue: z.string(),
    queue_incarnation: decimalString,
    message_id: decimalString,
    attempts: z.number(),
    redelivered: z.boolean(),
    lease_deadline_ms: decimalString,
    proof: z.string()
  }),
  input: z.object({
    queue_id: b64uId,
    queue_incarnation: decimalString,
    message_id: decimalString
  })
}).passthrough();

const jobCompletionResponse = z.object({
  status: z.literal("committed"),
  operation_id: uuid,
  storage_id: b64uId.nullable().optional(),
  commit_id: decimalString,
  input: z.object({ queue: z.string(), queue_incarnation: decimalString, message_id: decimalString, acknowledged: z.boolean() }).passthrough(),
  state: z.object({ key: b64uId, version: decimalString }).passthrough(),
  output: z.object({ queue: z.string(), queue_incarnation: decimalString, message_id: decimalString }).nullable(),
  completed_at: decimalString,
  receipt_expires_at: decimalString,
  replayed: z.boolean()
}).passthrough();

const completionReceipt = z.object({
  operation_id: uuid,
  commit_id: decimalString,
  state_version: decimalString,
  output_message_id: decimalString,
  completed_at: decimalString,
  receipt_expires_at: decimalString
}).passthrough();

const completionListResponse = z.object({ data: z.array(completionReceipt) }).passthrough();
const durableOperationResponse = z.object({
  operation_id: uuid,
  kind: z.enum(["state_put", "state_delete"]),
  commit_id: decimalString,
  state_version: decimalString,
  completed_at: decimalString,
  receipt_expires_at: decimalString
}).passthrough();

/**
 * Validate one new-resource response body. Returns null when the resource is
 * not covered by strict validation, "ok" when it validates, and a reason
 * string when the response violates the contract.
 */
export function validateJobCompletionResponse(method: string, relativePath: string, status: number, body: unknown): "ok" | null | string {
  if (status !== 200) return null;
  const rawPath = relativePath.split("?")[0] ?? "";
  const path = rawPath.startsWith("/") ? rawPath : `/${rawPath}`;
  const asJson = body as unknown;
  const data = (asJson as { data?: unknown } | null | undefined)?.data;
  if (method === "GET" && /^\/keyspaces\/durable$/.test(path)) {
    return durableKeyspaceResponse.safeParse(asJson).success ? "ok" : "keyspaces/durable shape";
  }
  if (method === "GET" && /^\/keyspaces\/durable\/entries$/.test(path)) {
    return durableEntriesResponse.safeParse(asJson).success ? "ok" : "durable entries shape";
  }
  if (method === "GET" && /^\/keyspaces\/durable\/entries\/[^/]+$/.test(path)) {
    // The entry detail body is top-level (no data envelope).
    return durableEntryResponse.safeParse(asJson).success ? "ok" : "durable entry shape";
  }
  if ((method === "PUT" || method === "DELETE") && /^\/keyspaces\/durable\/entries\/[^/]+$/.test(path)) {
    return durableMutationResponse.safeParse(asJson).success ? "ok" : "durable mutation receipt shape";
  }
  if (method === "POST" && path === "/job-completions") {
    return jobCompletionResponse.safeParse(asJson).success ? "ok" : "job completion receipt shape";
  }
  if (method === "GET" && path === "/job-completions") {
    return completionListResponse.safeParse(asJson).success ? "ok" : "completion list shape";
  }
  if (method === "GET" && /^\/job-completions\/[^/]+$/.test(path)) {
    return completionReceipt.safeParse(asJson).success ? "ok" : "completion receipt shape";
  }
  if (method === "GET" && /^\/durable-operations\/[^/]+$/.test(path)) {
    return durableOperationResponse.safeParse(asJson).success ? "ok" : "durable operation receipt shape";
  }
  if (method === "POST" && /^\/queue-consumers\/[^/]+\/deliveries$/.test(path)) {
    // Only completion-mode acquisitions carry the strict shape; the standard
    // mode answers 201 with its own {data:{delivery...}} envelope.
    return completionDeliveryResponse.safeParse(asJson).success ? "ok" : "completion delivery shape";
  }
  return null;
}
