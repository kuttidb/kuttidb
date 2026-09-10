import { describe, expect, it } from "vitest";
import { ApiError } from "./api";
import {
  advanceSubmission,
  buildCompletionRequest,
  classifySubmissionFailure,
  completionPreviewEffects,
  decimalToNumber,
  jobCompletionCapability,
  receiptStatusWord,
  retryAllowed,
  retrySameRequest,
  stateEtag,
  versionFromEtag,
  type CompletionIntent
} from "./job-completion";

const INTENT: CompletionIntent = {
  operationId: "f797bef0-8716-4e96-ade7-a0d75fca08c9",
  input: { queue: "jobs", queueIncarnation: "42", messageId: "17", deliveryProof: "cHJvb2Y=" },
  state: { key: "b64u:am9icw", expectedVersion: "3", valueBase64: "aGVsbG8=" },
  outgoing: null
};

describe("jobCompletionCapability", () => {
  it("parses the capability block with decimal-string limits", () => {
    const capability = jobCompletionCapability({
      job_completion: {
        available: true,
        enabled: true,
        limits: { state_max_bytes: "67108864", receipts_max_bytes: "134217728", receipts_max_count: "100000", receipt_retention_ms: "86400000", max_operation_bytes: "131072" }
      }
    });
    expect(capability).not.toBeNull();
    expect(capability?.limits.max_operation_bytes).toBe("131072");
  });

  it("returns null on older servers without the block (gate closed, no fallback)", () => {
    expect(jobCompletionCapability({})).toBeNull();
    expect(jobCompletionCapability(undefined)).toBeNull();
    // Malformed limits are rejected rather than half-trusted.
    expect(jobCompletionCapability({ job_completion: { available: true, enabled: true, limits: { state_max_bytes: 42 } } })).toBeNull();
  });
});

describe("buildCompletionRequest / retrySameRequest", () => {
  it("builds the exact POST /job-completions body", () => {
    const body = buildCompletionRequest(INTENT);
    expect(body).toEqual({
      operation_id: "f797bef0-8716-4e96-ade7-a0d75fca08c9",
      input: { queue: "jobs", queue_incarnation: "42", message_id: "17", delivery_proof: "cHJvb2Y=" },
      state: { key: "b64u:am9icw", expected_version: "3", value: { encoding: "base64", data: "aGVsbG8=" } },
      outgoing: null
    });
  });

  it("keeps the same operation id and payload on retry (no silent regeneration)", () => {
    const first = retrySameRequest(INTENT);
    const second = retrySameRequest(INTENT);
    expect(first.operationId).toBe(INTENT.operationId);
    expect(second).toEqual(first);
  });

  it("supports empty state values and output bodies as independent fields", () => {
    const body = buildCompletionRequest({
      ...INTENT,
      state: { key: "b64u:am9icw", expectedVersion: "0", valueBase64: "" },
      outgoing: { queue: "index", queueIncarnation: "9", valueBase64: "" }
    });
    expect((body.state as { value: { data: string } }).value.data).toBe("");
    expect((body.outgoing as { value: { data: string } }).value.data).toBe("");
  });
});

describe("classifySubmissionFailure", () => {
  it("classifies server operation_in_doubt as unknown", () => {
    expect(classifySubmissionFailure(new ApiError("operation_in_doubt", "unknown", 503))).toBe("unknown");
  });

  it("classifies lost transport (abort/timeout/network) as unknown", () => {
    const abort = new DOMException("The operation was aborted.", "AbortError");
    expect(classifySubmissionFailure(abort)).toBe("unknown");
    expect(classifySubmissionFailure(new TypeError("fetch failed"))).toBe("unknown");
    expect(classifySubmissionFailure(new ApiError("upstream_unavailable", "gateway unreachable", 502))).toBe("unknown");
  });

  it("classifies definitive refusals as failed, never unknown", () => {
    expect(classifySubmissionFailure(new ApiError("precondition_failed", "stale", 412))).toBe("failed");
    expect(classifySubmissionFailure(new ApiError("idempotency_conflict", "conflict", 409))).toBe("failed");
    expect(classifySubmissionFailure(new ApiError("delivery_expired", "expired", 409))).toBe("failed");
    expect(classifySubmissionFailure(new Error("boom"))).toBe("failed");
  });
});

describe("submission state machine", () => {
  it("freezes the intent on submit and resolves it", () => {
    const submitting = advanceSubmission({ phase: "composing" }, { kind: "submit", intent: INTENT });
    expect(submitting).toEqual({ phase: "submitting", intent: INTENT });
    const resolved = advanceSubmission(submitting, {
      kind: "resolve",
      result: {
        status: "committed",
        operation_id: INTENT.operationId,
        commit_id: "7",
        input: { queue: "jobs", queue_incarnation: "42", message_id: "17", acknowledged: true },
        state: { key: "b64u:am9icw", version: "4" },
        output: null,
        completed_at: "1000",
        receipt_expires_at: "2000",
        replayed: false
      }
    });
    expect(resolved.phase).toBe("resolved");
  });

  it("routes a timeout failure to the unknown phase with the frozen intent", () => {
    const submitting = advanceSubmission({ phase: "composing" }, { kind: "submit", intent: INTENT });
    const aborted = advanceSubmission(submitting, { kind: "fail", error: new DOMException("aborted", "AbortError") });
    expect(aborted.phase).toBe("unknown");
    expect(aborted).toMatchObject({ intent: INTENT });
    expect(retryAllowed(aborted)).toBe(true);
  });

  it("routes a definitive failure to the failed phase (retry still exact)", () => {
    const submitting = advanceSubmission({ phase: "composing" }, { kind: "submit", intent: INTENT });
    const failedState = advanceSubmission(submitting, { kind: "fail", error: new ApiError("precondition_failed", "stale", 412) });
    expect(failedState.phase).toBe("failed");
    expect(retryAllowed(failedState)).toBe(true);
    expect(retrySameRequest((failedState as { intent: CompletionIntent }).intent).operationId).toBe(INTENT.operationId);
  });

  it("ignores resolve/fail events outside the submitting phase", () => {
    const submitting = advanceSubmission({ phase: "composing" }, { kind: "submit", intent: INTENT });
    const failedState = advanceSubmission(submitting, { kind: "fail", error: new ApiError("precondition_failed", "stale", 412) });
    const settled = advanceSubmission(failedState, { kind: "resolve", result: {} as never });
    expect(settled).toBe(failedState);
  });
});

describe("completion preview effects", () => {
  it("lists the three effects in plain language with ACK wording", () => {
    const effects = completionPreviewEffects({
      ...INTENT,
      outgoing: { queue: "index", queueIncarnation: "9", valueBase64: "aW5kZXg=" }
    });
    expect(effects).toHaveLength(3);
    expect(effects[0]).toContain("ACK");
    expect(effects[0]).toContain("jobs");
    expect(effects[1]).toContain("Durable state");
    expect(effects[1]).toContain("3");
    expect(effects[2]).toContain("index");
  });

  it("states when no output message will be published", () => {
    const effects = completionPreviewEffects(INTENT);
    expect(effects[2]).toContain("No output message");
  });

  it("shows byte lengths and supports empty values", () => {
    const effects = completionPreviewEffects({
      ...INTENT,
      state: { key: "b64u:am9icw", expectedVersion: "0", valueBase64: "" },
      outgoing: { queue: "index", queueIncarnation: "9", valueBase64: "" }
    });
    expect(effects[1]).toContain("empty value");
    expect(effects[2]).toContain("empty body");
  });
});

describe("receipt wording", () => {
  it("distinguishes first completion from Already completed", () => {
    expect(receiptStatusWord({ replayed: false })).toBe("First completion");
    expect(receiptStatusWord({ replayed: true })).toBe("Already completed");
  });
});

describe("state ETag helpers", () => {
  it("round-trips the s-<version> envelope", () => {
    expect(stateEtag("17")).toBe('"s-17"');
    expect(versionFromEtag('"s-17"')).toBe("17");
    expect(versionFromEtag('"q-17"')).toBeNull();
    expect(versionFromEtag(null)).toBeNull();
  });

  it("converts decimal strings to numbers for display math only", () => {
    expect(decimalToNumber("9007199254740993")).toBeGreaterThan(Number.MAX_SAFE_INTEGER);
  });
});
