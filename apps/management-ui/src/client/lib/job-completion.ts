import { z } from "zod";
import { ApiError } from "./api";
import type { Capabilities } from "./api";

/**
 * Atomic job completion (durable state + ACK + optional output publish).
 *
 * The server (openapi/management-v1.yaml, job-completion family) transports
 * 64-bit identity/version/timestamp fields as lossless DECIMAL STRINGS and
 * binary payloads as {"encoding":"base64","data":...}. Opaque binary ids are
 * b64u ("b64u:" + URL-safe Base64, unpadded). packages/admin-client generated
 * types do not cover these resources yet, so the shapes are declared here and
 * validated at runtime; do not hand-edit the generated file.
 */

/** Lossless decimal string: "17", never 17. Guards JS precision for u64. */
export const decimalStringSchema = z.string().regex(/^[0-9]+$/, "expected a decimal string");

export const binaryValueSchema = z.object({
  encoding: z.literal("base64"),
  data: z.string(),
  size: z.number().optional(),
  content_type: z.string().optional()
});
export type BinaryValueEnvelope = z.infer<typeof binaryValueSchema>;

/** job_completion block of GET /capabilities (absent on older servers). */
export const jobCompletionCapabilitySchema = z.object({
  available: z.boolean(),
  enabled: z.boolean(),
  limits: z.object({
    state_max_bytes: decimalStringSchema,
    receipts_max_bytes: decimalStringSchema,
    receipts_max_count: decimalStringSchema,
    receipt_retention_ms: decimalStringSchema,
    max_operation_bytes: decimalStringSchema
  })
}).passthrough();
export type JobCompletionCapability = z.infer<typeof jobCompletionCapabilitySchema>;

export const jobCompletionStatusSchema = z.object({
  enabled: z.boolean(),
  healthy: z.boolean(),
  state_entries: z.number(),
  state_bytes: z.number(),
  receipts: z.number(),
  receipt_bytes: z.number()
}).passthrough();
export type JobCompletionStatus = z.infer<typeof jobCompletionStatusSchema>;

/** GET /keyspaces/durable */
export const durableKeyspaceSchema = z.object({
  name: z.literal("durable"),
  evictable: z.boolean(),
  entry_count: z.number(),
  live_bytes: z.number(),
  capacity_bytes: decimalStringSchema,
  persistence_healthy: z.boolean(),
  storage_class: z.string()
}).passthrough();
export type DurableKeyspaceInfo = z.infer<typeof durableKeyspaceSchema>;

/** GET /keyspaces/durable/entries item */
export const durableEntryMetaSchema = z.object({
  entry_id: z.string().regex(/^b64u:[A-Za-z0-9_-]+$/),
  key_bytes: z.number(),
  version: decimalStringSchema,
  last_commit_id: decimalStringSchema,
  value_size: z.number()
});
export type DurableEntryMeta = z.infer<typeof durableEntryMetaSchema>;

/** GET /keyspaces/durable/entries/{entry_id} (top-level body + ETag "s-<version>"). */
export const durableEntrySchema = z.object({
  entry_id: z.string().regex(/^b64u:[A-Za-z0-9_-]+$/),
  key: z.string().regex(/^b64u:[A-Za-z0-9_-]+$/),
  value: binaryValueSchema,
  version: decimalStringSchema,
  last_commit_id: decimalStringSchema
}).passthrough();
export type DurableEntry = z.infer<typeof durableEntrySchema>;

/** PUT/DELETE /keyspaces/durable/entries/{entry_id} success body */
export const durableMutationReceiptSchema = z.object({
  operation_id: z.string().uuid(),
  commit_id: decimalStringSchema,
  state_version: decimalStringSchema,
  completed_at: decimalStringSchema,
  receipt_expires_at: decimalStringSchema,
  replayed: z.boolean()
});
export type DurableMutationReceipt = z.infer<typeof durableMutationReceiptSchema>;

/** POST /queue-consumers/{id}/deliveries with mode:"completion" (200, top level). */
export const jobDeliveryAcquisitionSchema = z.object({
  delivery: z.object({
    store_id: z.string().regex(/^b64u:[A-Za-z0-9_-]+$/),
    queue: z.string(),
    queue_incarnation: decimalStringSchema,
    message_id: decimalStringSchema,
    attempts: z.number(),
    redelivered: z.boolean(),
    lease_deadline_ms: decimalStringSchema,
    proof: z.string()
  }),
  input: z.object({
    queue_id: z.string().regex(/^b64u:[A-Za-z0-9_-]+$/),
    queue_incarnation: decimalStringSchema,
    message_id: decimalStringSchema
  })
});
export type JobDeliveryAcquisition = z.infer<typeof jobDeliveryAcquisitionSchema>;

/** POST /job-completions success body (first commit and matched replay). */
export const jobCompletionResultSchema = z.object({
  status: z.literal("committed"),
  operation_id: z.string().uuid(),
  storage_id: z.string().regex(/^b64u:[A-Za-z0-9_-]+$/).nullable().optional(),
  commit_id: decimalStringSchema,
  input: z.object({
    queue: z.string(),
    queue_incarnation: decimalStringSchema,
    message_id: decimalStringSchema,
    acknowledged: z.boolean()
  }),
  state: z.object({
    key: z.string().regex(/^b64u:[A-Za-z0-9_-]+$/),
    version: decimalStringSchema
  }),
  output: z.object({
    queue: z.string(),
    queue_incarnation: decimalStringSchema,
    message_id: decimalStringSchema
  }).nullable(),
  completed_at: decimalStringSchema,
  receipt_expires_at: decimalStringSchema,
  replayed: z.boolean()
});
export type JobCompletionResult = z.infer<typeof jobCompletionResultSchema>;

/** GET /job-completions and /job-completions/{operation_id} receipt item. */
export const completionReceiptSchema = z.object({
  operation_id: z.string().uuid(),
  commit_id: decimalStringSchema,
  state_version: decimalStringSchema,
  output_message_id: decimalStringSchema,
  completed_at: decimalStringSchema,
  receipt_expires_at: decimalStringSchema
});
export type CompletionReceipt = z.infer<typeof completionReceiptSchema>;

/** GET /durable-operations/{operation_id} */
export const durableOperationReceiptSchema = z.object({
  operation_id: z.string().uuid(),
  kind: z.enum(["state_put", "state_delete"]),
  commit_id: decimalStringSchema,
  state_version: decimalStringSchema,
  completed_at: decimalStringSchema,
  receipt_expires_at: decimalStringSchema
});
export type DurableOperationReceipt = z.infer<typeof durableOperationReceiptSchema>;

/**
 * Capability gate for every new action. `available` means the server binary
 * supports the feature; `enabled` means it is switched on for this instance.
 * When either is false the UI must explain and disable — never fall back to
 * separate put + publish + ACK calls.
 */
export function jobCompletionCapability(capabilities: CapabilitiesLike | undefined): JobCompletionCapability | null {
  const parsed = jobCompletionCapabilitySchema.safeParse(capabilities?.job_completion);
  return parsed.success ? parsed.data : null;
}

export type CapabilitiesLike = { job_completion?: unknown } & Record<string, unknown>;

/** The exact state ETag form (`"s-<version>"`), also the If-Match header value. */
export function stateEtag(version: string): string {
  return `"s-${version}"`;
}

export function versionFromEtag(etag: string | null): string | null {
  const match = /^"s-([0-9]+)"$/.exec(etag ?? "");
  return match ? (match[1] as string) : null;
}

/** Decimal-string → JS number, guarding precision for display math only. */
export function decimalToNumber(value: string): number {
  return Number(value);
}

/**
 * A frozen completion intent: exactly what will be submitted, including the
 * operation id generated once at composition. Retries must reuse this object
 * verbatim — field edits never leak into an unresolved retry.
 */
export type CompletionIntent = {
  operationId: string;
  input: { queue: string; queueIncarnation: string; messageId: string; deliveryProof: string };
  state: { key: string; expectedVersion: string; valueBase64: string };
  outgoing: { queue: string; queueIncarnation: string; valueBase64: string } | null;
};

/** Canonical POST /job-completions body for an intent. */
export function buildCompletionRequest(intent: CompletionIntent): Record<string, unknown> {
  return {
    operation_id: intent.operationId,
    input: {
      queue: intent.input.queue,
      queue_incarnation: intent.input.queueIncarnation,
      message_id: intent.input.messageId,
      delivery_proof: intent.input.deliveryProof
    },
    state: {
      key: intent.state.key,
      expected_version: intent.state.expectedVersion,
      value: { encoding: "base64", data: intent.state.valueBase64 }
    },
    outgoing: intent.outgoing
      ? {
          queue: intent.outgoing.queue,
          queue_incarnation: intent.outgoing.queueIncarnation,
          value: { encoding: "base64", data: intent.outgoing.valueBase64 }
        }
      : null
  };
}

/** The exact intent, re-serialized: retries reuse the identical payload. */
export function retrySameRequest(intent: CompletionIntent): { operationId: string; body: Record<string, unknown> } {
  return { operationId: intent.operationId, body: buildCompletionRequest(intent) };
}

export type SubmissionPhase =
  | { phase: "composing" }
  | { phase: "submitting"; intent: CompletionIntent }
  | { phase: "resolved"; intent: CompletionIntent; result: JobCompletionResult }
  | { phase: "unknown"; intent: CompletionIntent; error: unknown }
  | { phase: "failed"; intent: CompletionIntent; error: unknown };

/**
 * Classify a failed submission. "unknown" means the outcome cannot be proven:
 * timeout, lost connection, gateway reachability failure, or the server's
 * explicit operation_in_doubt envelope. Those show "Outcome unknown" and offer
 * receipt lookup plus an exact retry — never "Failed; try again".
 */
export function classifySubmissionFailure(error: unknown): "unknown" | "failed" {
  if (error instanceof ApiError) {
    if (error.code === "operation_in_doubt") return "unknown";
    // Gateway/unreachable transports leave the commit's fate unproven.
    if (error.code === "upstream_unavailable") return "unknown";
    return "failed";
  }
  if (error instanceof DOMException && (error.name === "AbortError" || error.name === "TimeoutError")) return "unknown";
  if (error instanceof Error) {
    if (error.name === "AbortError" || error.name === "TimeoutError" || error.name === "TypeError") return "unknown";
    return "failed";
  }
  return "failed";
}

export function advanceSubmission(state: SubmissionPhase, event: { kind: "submit"; intent: CompletionIntent } | { kind: "resolve"; result: JobCompletionResult } | { kind: "fail"; error: unknown }): SubmissionPhase {
  switch (event.kind) {
    case "submit":
      return { phase: "submitting", intent: event.intent };
    case "resolve": {
      if (state.phase !== "submitting") return state;
      return { phase: "resolved", intent: state.intent, result: event.result };
    }
    case "fail": {
      if (state.phase !== "submitting") return state;
      const outcome = classifySubmissionFailure(event.error);
      return outcome === "unknown"
        ? { phase: "unknown", intent: state.intent, error: event.error }
        : { phase: "failed", intent: state.intent, error: event.error };
    }
  }
}

/** True while an unresolved intent must keep its exact frozen payload. */
export function retryAllowed(state: SubmissionPhase): boolean {
  return state.phase === "unknown" || state.phase === "failed";
}

/**
 * The three intended effects, in plain language, shown before submit. The
 * wording is contractual: the input will be ACKed by the commit and no
 * separate ACK may follow a success.
 */
export function completionPreviewEffects(intent: CompletionIntent): string[] {
  const effects = [
    `ACK the input message ${intent.input.messageId} in Queue ${intent.input.queue} as part of the commit — do not send a separate ACK afterwards.`,
    `Write Durable state key ${intent.state.key} at expected version ${intent.state.expectedVersion} (${intent.state.valueBase64.length === 0 ? "empty value" : `${decodeBase64ByteLength(intent.state.valueBase64)} bytes`}).`
  ];
  if (intent.outgoing) {
    effects.push(`Publish one output message to Queue ${intent.outgoing.queue} (${intent.outgoing.valueBase64.length === 0 ? "empty body" : `${decodeBase64ByteLength(intent.outgoing.valueBase64)} bytes`}).`);
  } else {
    effects.push("No output message will be published.");
  }
  return effects;
}

function decodeBase64ByteLength(base64: string): number {
  if (base64.length === 0) return 0;
  const padding = base64.endsWith("==") ? 2 : base64.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((base64.length * 3) / 4) - padding);
}

/** Receipt wording: first completion vs Already completed (matched replay). */
export function receiptStatusWord(result: Pick<JobCompletionResult, "replayed">): string {
  return result.replayed ? "Already completed" : "First completion";
}
