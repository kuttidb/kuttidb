// @vitest-environment happy-dom
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { ConnectionContext, type ConnectionState } from "@/state/connections";
import { clearAllPending, getPendingDelivery, setPendingDelivery } from "@/lib/job-intent-store";
import { JobCompletionView } from "./job-completion";
import type { Capabilities } from "@/lib/api";

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

type FetchCall = { url: string; init: RequestInit | undefined };

/** Hand-rolled Response stand-in for the `admin()` fetch transport. */
function jsonResponse(status: number, body: unknown, etag: string | null = null) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name: string) => (name.toLowerCase() === "etag" ? etag : null) },
    text: async () => JSON.stringify(body)
  };
}

function abortError(): Error {
  const error = new Error("The operation was aborted.");
  error.name = "AbortError";
  return error;
}

const JOB_COMPLETION_CAPS = {
  available: true,
  enabled: true,
  limits: { state_max_bytes: "1", receipts_max_bytes: "1", receipts_max_count: "1", receipt_retention_ms: "1", max_operation_bytes: "131072" }
};

const ENABLED_CAPS = {
  product: "KuttiDB",
  server_version: "0.2.0",
  management_api_contract: "1.0",
  enabled_engines: ["queues", "keyspaces"],
  operations: {},
  limits: {},
  routing_modes: [],
  job_completion: JOB_COMPLETION_CAPS
} as unknown as Capabilities;

const DISABLED_CAPS = {
  product: "KuttiDB",
  server_version: "0.2.0",
  management_api_contract: "1.0",
  enabled_engines: ["queues", "keyspaces"],
  operations: {},
  limits: {},
  routing_modes: [],
  job_completion: { ...JOB_COMPLETION_CAPS, enabled: false }
} as unknown as Capabilities;

function mockState(capabilities: Capabilities | undefined, live: boolean): ConnectionState {
  const liveConnection = { profileId: "p1", endpoint: "http://127.0.0.1:7380", capabilities: capabilities ?? {}, connectedAt: "", lastUsedAt: "" };
  return {
    profiles: [{ id: "p1", label: "Test", endpoint: "http://127.0.0.1:7380", rememberProfile: true }],
    live: live ? new Map([["p1", liveConnection]]) : new Map(),
    capabilities: new Map(capabilities ? [["p1", capabilities]] : []),
    connect: async () => undefined,
    reconnect: async () => undefined,
    disconnect: async () => undefined,
    removeProfile: async () => undefined,
    lockAll: async () => undefined,
    refreshCapabilities: async () => undefined,
    mutationsBlocked: () => false
  } as unknown as ConnectionState;
}

function renderJobCompletionView(caps: Capabilities | undefined, responder: (url: string, init: RequestInit | undefined) => Promise<unknown>, live = true) {
  const calls: FetchCall[] = [];
  let respond = responder;
  vi.stubGlobal("fetch", (input: string | URL | Request, init?: RequestInit) => {
    const url = String(input);
    calls.push({ url, init });
    return respond(url, init);
  });
  const container = document.createElement("div");
  document.body.append(container);
  const root: Root = createRoot(container);
  act(() => {
    root.render(
      <ConnectionContext.Provider value={mockState(caps, live)}>
        <JobCompletionView profileId="p1" />
      </ConnectionContext.Provider>
    );
  });
  return {
    calls,
    respond: (next: typeof responder) => { respond = next; },
    unmount: () => act(() => root.unmount()),
    click: (text: string) => {
      const button = Array.from(container.querySelectorAll("button")).find((candidate) => candidate.textContent?.includes(text)) as HTMLButtonElement | undefined;
      if (!button) return false;
      act(() => { button.click(); });
      return true;
    },
    setText: (id: string, value: string) => {
      const input = container.querySelector(`#${id}`) as HTMLInputElement | HTMLTextAreaElement | null;
      if (!input) return;
      act(() => {
        const setter = input instanceof HTMLTextAreaElement
          ? Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")?.set
          : Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
        setter?.call(input, value);
        input.dispatchEvent(new Event("input", { bubbles: true }));
      });
    },
    text: () => container.textContent ?? ""
  };
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function fillValidForm(view: ReturnType<typeof renderJobCompletionView>, stateValue = "done"): void {
  view.setText("job-input-queue", "jobs");
  view.setText("job-input-message", "17");
  view.setText("job-input-incarnation", "42");
  view.setText("job-input-proof", "cHJvb2Y=");
  view.setText("job-state-key", "order:42");
  view.setText("job-state-version", "0");
  view.setText("job-state-value", stateValue);
}

function committedResult(operationId: string, replayed: boolean) {
  return jsonResponse(200, {
    status: "committed",
    operation_id: operationId,
    commit_id: "7",
    input: { queue: "jobs", queue_incarnation: "42", message_id: "17", acknowledged: true },
    state: { key: "b64u:b3JkZXI6NDI", version: "4" },
    output: null,
    completed_at: "1000",
    receipt_expires_at: "2000",
    replayed
  });
}

describe("JobCompletionView", () => {
  beforeEach(() => {
    clearAllPending();
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("explains and disables everything on servers without the capability (never a fallback)", () => {
    const view = renderJobCompletionView(undefined, async () => jsonResponse(200, {}));
    expect(view.text()).toContain("Atomic job completion is unavailable on this server");
    expect(view.text()).toContain("no fallback");
    expect(view.click("Complete job")).toBe(false);
    view.unmount();
  });

  it("explains the disabled state read-only on servers started without the feature", () => {
    const view = renderJobCompletionView(DISABLED_CAPS, async () => jsonResponse(200, {}));
    expect(view.text()).toContain("started without it");
    view.unmount();
  });

  it("previews the three effects with ACK wording before submit", () => {
    const view = renderJobCompletionView(ENABLED_CAPS, async () => jsonResponse(200, {}));
    fillValidForm(view);
    const preview = view.text();
    expect(preview).toContain("ACK the input message 17 in Queue jobs");
    expect(preview).toContain("no separate ACK");
    expect(preview).toContain("Durable state key b64u:");
    expect(preview).toContain("No output message will be published");
    view.unmount();
  });

  it("submits with the Completion ID as the Idempotency-Key and shows First completion", async () => {
    let operationId = "";
    const view = renderJobCompletionView(ENABLED_CAPS, async (_url, init) => {
      if (init?.method === "POST") {
        operationId = (JSON.parse(String(init.body)) as { operation_id: string }).operation_id;
        return committedResult(operationId, false);
      }
      return jsonResponse(404, { error: { code: "not_found", message: "x" } });
    });
    fillValidForm(view);
    expect(view.click("Complete job")).toBe(true);
    await flush();
    const post = view.calls.find((call) => call.init?.method === "POST");
    expect(post?.url).toContain("/admin/job-completions");
    const headers = (post?.init?.headers ?? {}) as Record<string, string>;
    const body = JSON.parse(String(post?.init?.body)) as { operation_id: string; outgoing: unknown; input: { delivery_proof: string } };
    expect(body.operation_id).toMatch(UUID_PATTERN);
    expect(headers["idempotency-key"]).toBe(body.operation_id);
    expect(body.input.delivery_proof).toBe("cHJvb2Y=");
    expect(body.outgoing).toBeNull();
    expect(view.text()).toContain("First completion");
    expect(view.text()).toContain("no separate ACK");
    view.unmount();
  });

  it("shows Outcome unknown after a lost response and retries with the exact same ID and payload", async () => {
    let attempts = 0;
    let firstBody = "";
    const view = renderJobCompletionView(ENABLED_CAPS, async (_url, init) => {
      if (init?.method === "POST") {
        attempts += 1;
        if (attempts === 1) {
          firstBody = String(init.body);
          const abort = new Error("aborted");
          abort.name = "AbortError";
          throw abort; // lost response → outcome unknown
        }
        return committedResult((JSON.parse(String(init.body)) as { operation_id: string }).operation_id, true);
      }
      return jsonResponse(404, { error: { code: "not_found", message: "x" } });
    });
    fillValidForm(view);
    view.click("Complete job");
    await flush();
    expect(attempts).toBe(1);
    expect(view.text()).toContain("Outcome unknown");
    expect(view.text()).not.toContain("Failed; try again");

    // Edits made while unresolved must NOT leak into a retry.
    view.setText("job-state-value", "EDITED-WHILE-UNRESOLVED");

    expect(view.click("Retry same completion")).toBe(true);
    await flush();
    expect(attempts).toBe(2);
    const posts = view.calls.filter((call) => call.init?.method === "POST");
    expect(String(posts[1]?.init?.body)).toBe(firstBody);
    const firstHeaders = (posts[0]?.init?.headers ?? {}) as Record<string, string>;
    const secondHeaders = (posts[1]?.init?.headers ?? {}) as Record<string, string>;
    const operationId = (JSON.parse(firstBody) as { operation_id: string }).operation_id;
    expect(firstHeaders["idempotency-key"]).toBe(operationId);
    expect(secondHeaders["idempotency-key"]).toBe(operationId);
    view.unmount();
  });

  it("offers Check completion that looks the receipt up without the proof", async () => {
    const view = renderJobCompletionView(ENABLED_CAPS, async (url, init) => {
      if (init?.method === "POST") throw abortErrorNamed();
      const operationId = url.match(/\/admin\/job-completions\/([0-9a-f-]{36})$/)?.[1];
      if (operationId) {
        return jsonResponse(200, {
          operation_id: operationId, commit_id: "7", state_version: "4",
          output_message_id: "0", completed_at: "1000", receipt_expires_at: "2000"
        });
      }
      return jsonResponse(404, { error: { code: "not_found", message: "x" } });
    });
    fillValidForm(view);
    view.click("Complete job");
    await flush();
    expect(view.click("Check completion")).toBe(true);
    await flush();
    const lookup = view.calls.find((call) => /\/admin\/job-completions\/[0-9a-f-]{36}$/.test(call.url));
    expect(lookup).toBeDefined();
    expect(view.text()).toContain("Already completed");
    view.unmount();
  });

  it("picks up a held delivery and wipes pending state when the connection is gone", () => {
    setPendingDelivery("p1", {
      queue: "jobs", queueId: "b64u:am9icw", storeId: "b64u:c3RvcmU", queueIncarnation: "42",
      messageId: "17", attempts: 1, redelivered: false, leaseDeadlineMs: "9999999999999",
      deliveryProof: "cHJvb2Y=", acquiredAt: Date.now()
    });
    const view = renderJobCompletionView(ENABLED_CAPS, async () => jsonResponse(200, {}), false);
    expect(view.text()).toContain("Delivery held from Queue jobs");
    expect(getPendingDelivery("p1")).toBeNull();
    view.unmount();
  });

  it("shows the visible copyable Completion ID control", () => {
    const view = renderJobCompletionView(ENABLED_CAPS, async () => jsonResponse(200, {}));
    expect(view.text()).toContain("Completion ID (operation id)");
    expect(view.text()).toContain("reused on every retry of the same completion");
    view.unmount();
  });
});

function abortErrorNamed(): Error {
  const error = new Error("aborted");
  error.name = "AbortError";
  return error;
}

async function flush(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
    await new Promise((resolve) => setTimeout(resolve, 0));
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}
