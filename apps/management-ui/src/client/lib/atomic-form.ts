import { ApiError } from "./api";

/**
 * Pure helpers for the Atomic operations form: field-level validation and the
 * mutation-outcome classification that gates retries after an unknown result.
 * Kept framework-free so behavior tests can exercise them directly.
 */

export type AtomicFieldErrors = {
  key?: string;
  target?: string;
  routingKey?: string;
  value?: string;
  body?: string;
};

export type AtomicFormInput = {
  operation: string;
  keyText: string;
  targetName: string;
  routingKey: string;
  valueText: string;
  bodyText: string;
};

export type AtomicOpSpec = { op: string; needsRoutingKey: boolean; needsValue: boolean; needsBody: boolean };

/** Field-level validation for the structured (non-expert) atomic form. */
export function validateAtomicInput(
  input: AtomicFormInput,
  spec: AtomicOpSpec,
  toId: (text: string) => string,
  isValidId: (id: string) => boolean
): AtomicFieldErrors {
  const errors: AtomicFieldErrors = {};
  const keyId = input.keyText.trim().length > 0 ? toId(input.keyText.trim()) : "";
  const targetId = input.targetName.trim().length > 0 ? toId(input.targetName.trim()) : "";
  if (keyId.length === 0) errors.key = "A cache key is required.";
  else if (!isValidId(keyId)) errors.key = "Use plain text or a valid b64u identifier.";
  if (targetId.length === 0) errors.target = "A target queue is required.";
  else if (!isValidId(targetId)) errors.target = "Use plain text or a valid b64u identifier.";
  if (spec.needsRoutingKey && input.routingKey.trim().length === 0) errors.routingKey = "This operation requires a routing key.";
  if (spec.needsValue && input.valueText.length === 0) errors.value = "This operation requires a value.";
  if (spec.needsBody && input.bodyText.length === 0) errors.body = "This operation requires a message body.";
  return errors;
}

export type MutationOutcome = "clean" | "in_doubt" | "failed";

/**
 * Classify a mutation failure. `operation_in_doubt` means the outcome is
 * unknown: the caller must block resubmission until the operator reconciles
 * the target — never retry automatically.
 */
export function mutationOutcome(error: unknown): MutationOutcome {
  if (!error) return "clean";
  if (error instanceof ApiError && error.code === "operation_in_doubt") return "in_doubt";
  return "failed";
}
