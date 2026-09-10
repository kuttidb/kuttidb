import { describe, expect, it } from "vitest";
import { isAllowedUpstreamRequest, validateJobCompletionResponse } from "./upstream-policy.js";

const B64U = "b64u:am9icw";
const UUID = "f797bef0-8716-4e96-ade7-a0d75fca08c9";

describe("upstream allowlist", () => {
  it("allows the established Management API resources", () => {
    expect(isAllowedUpstreamRequest("GET", "capabilities")).toBe(true);
    expect(isAllowedUpstreamRequest("GET", "status")).toBe(true);
    expect(isAllowedUpstreamRequest("GET", `keyspaces/default/entries/${B64U}`)).toBe(true);
    expect(isAllowedUpstreamRequest("PUT", `keyspaces/default/entries/${B64U}`)).toBe(true);
    expect(isAllowedUpstreamRequest("DELETE", `queues/${B64U}`)).toBe(true);
    expect(isAllowedUpstreamRequest("POST", `queues/${B64U}/deliveries/${"d:0123456789abcdef0123456789abcdef"}:ack`)).toBe(true);
    expect(isAllowedUpstreamRequest("POST", "maintenance/checkpoint-all")).toBe(true);
  });

  it("allows every new atomic job completion resource", () => {
    expect(isAllowedUpstreamRequest("GET", "keyspaces/durable")).toBe(true);
    expect(isAllowedUpstreamRequest("GET", "keyspaces/durable/entries")).toBe(true);
    expect(isAllowedUpstreamRequest("GET", `keyspaces/durable/entries/${B64U}`)).toBe(true);
    expect(isAllowedUpstreamRequest("PUT", `keyspaces/durable/entries/${B64U}`)).toBe(true);
    expect(isAllowedUpstreamRequest("DELETE", `keyspaces/durable/entries/${B64U}`)).toBe(true);
    expect(isAllowedUpstreamRequest("POST", "job-completions")).toBe(true);
    expect(isAllowedUpstreamRequest("GET", `job-completions/${UUID}`)).toBe(true);
    expect(isAllowedUpstreamRequest("GET", `durable-operations/${UUID}`)).toBe(true);
    expect(isAllowedUpstreamRequest("POST", `queue-consumers/${B64U}/deliveries`)).toBe(true);
  });

  it("rejects methods and paths outside the contract", () => {
    expect(isAllowedUpstreamRequest("DELETE", "capabilities")).toBe(false);
    expect(isAllowedUpstreamRequest("GET", "keyspaces/durable/entries/..%2f..%2f")).toBe(false);
    expect(isAllowedUpstreamRequest("GET", "keyspaces//durable")).toBe(false);
    expect(isAllowedUpstreamRequest("POST", "keyspaces/durable")).toBe(false);
    expect(isAllowedUpstreamRequest("DELETE", "job-completions")).toBe(false);
    expect(isAllowedUpstreamRequest("GET", "unknown-resource")).toBe(false);
    expect(isAllowedUpstreamRequest("GET", "")).toBe(false);
    expect(isAllowedUpstreamRequest("GET", "queues/x/deliveries/y:ack")).toBe(false);
  });
});

describe("job completion response validation", () => {
  const entry = { entry_id: B64U, key: B64U, value: { encoding: "base64", data: "aGVsbG8=" }, version: "12", last_commit_id: "7" };

  it("accepts contract-shaped responses", () => {
    expect(validateJobCompletionResponse("GET", "keyspaces/durable", 200, {
      data: { name: "durable", evictable: false, entry_count: 1, live_bytes: 10, capacity_bytes: "1000", persistence_healthy: true, storage_class: "queue_wal" }
    })).toBe("ok");
    expect(validateJobCompletionResponse("GET", `keyspaces/durable/entries/${B64U}`, 200, entry)).toBe("ok");
    expect(validateJobCompletionResponse("GET", `job-completions/${UUID}`, 200, {
      operation_id: UUID, commit_id: "7", state_version: "12", output_message_id: "3", completed_at: "100", receipt_expires_at: "200"
    })).toBe("ok");
    expect(validateJobCompletionResponse("POST", `queue-consumers/${B64U}/deliveries`, 200, {
      delivery: { store_id: "b64u:c3RvcmU", queue: "jobs", queue_incarnation: "42", message_id: "17", attempts: 1, redelivered: false, lease_deadline_ms: "999", proof: "cHJvb2Y=" },
      input: { queue_id: B64U, queue_incarnation: "42", message_id: "17" }
    })).toBe("ok");
  });

  it("rejects rounded JSON numbers where lossless decimal strings are required", () => {
    expect(validateJobCompletionResponse("GET", `keyspaces/durable/entries/${B64U}`, 200, { ...entry, version: 12 })).not.toBe("ok");
    expect(validateJobCompletionResponse("GET", "keyspaces/durable", 200, {
      data: { name: "durable", evictable: false, entry_count: 1, live_bytes: 1, capacity_bytes: 1000, persistence_healthy: true }
    })).not.toBe("ok");
  });

  it("rejects wrong kinds on the durable-operations lookup", () => {
    expect(validateJobCompletionResponse("GET", `durable-operations/${UUID}`, 200, {
      operation_id: UUID, kind: "completion", commit_id: "7", state_version: "12", completed_at: "100", receipt_expires_at: "200"
    })).not.toBe("ok");
  });

  it("ignores resources outside the strict family and non-200 statuses", () => {
    expect(validateJobCompletionResponse("GET", "queues", 200, { data: [] })).toBeNull();
    expect(validateJobCompletionResponse("GET", `job-completions/${UUID}`, 404, { error: {} })).toBeNull();
  });
});
