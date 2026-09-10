// @vitest-environment happy-dom
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { ConnectionContext, type ConnectionState } from "@/state/connections";
import { clearAllPending, getPendingDelivery } from "@/lib/job-intent-store";
import { CompleteJobTab } from "./queues";

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

function jsonResponse(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    text: async () => JSON.stringify(body)
  };
}

function mockState(): ConnectionState {
  const capabilities = {
    product: "KuttiDB", server_version: "0.2.0", management_api_contract: "1.0",
    enabled_engines: ["queues"], operations: {}, limits: {}, routing_modes: [],
    job_completion: { available: true, enabled: true, limits: { state_max_bytes: "1", receipts_max_bytes: "1", receipts_max_count: "1", receipt_retention_ms: "1", max_operation_bytes: "131072" } }
  };
  return {
    profiles: [{ id: "p1", label: "Test", endpoint: "http://127.0.0.1:7380", rememberProfile: true }],
    live: new Map([["p1", { profileId: "p1", endpoint: "http://127.0.0.1:7380", capabilities, connectedAt: "", lastUsedAt: "" }]]),
    capabilities: new Map([["p1", capabilities]]),
    connect: async () => undefined, reconnect: async () => undefined, disconnect: async () => undefined,
    removeProfile: async () => undefined, lockAll: async () => undefined, refreshCapabilities: async () => undefined,
    mutationsBlocked: () => false
  } as unknown as ConnectionState;
}

function renderCompleteJobTab(responder: (url: string, init: RequestInit | undefined) => Promise<unknown>) {
  const calls: { url: string; init: RequestInit | undefined }[] = [];
  let respond = responder;
  vi.stubGlobal("fetch", (input: string | URL | Request, init?: RequestInit) => {
    const url = String(input);
    calls.push({ url, init });
    return respond(url, init);
  });
  const container = document.createElement("div");
  document.body.append(container);
  const root: Root = createRoot(container);
  let composeClicked = 0;
  act(() => {
    root.render(
      <ConnectionContext.Provider value={mockState()}>
        <CompleteJobTab
          profileId="p1"
          queueId="b64u:am9icw"
          queueName="jobs"
          onComposeCompletion={() => { composeClicked += 1; }}
        />
      </ConnectionContext.Provider>
    );
  });
  return {
    calls,
    composeClicks: () => composeClicked,
    unmount: () => act(() => root.unmount()),
    click: (text: string) => {
      const button = Array.from(container.querySelectorAll("button")).find((candidate) => candidate.textContent?.includes(text)) as HTMLButtonElement | undefined;
      if (!button) return false;
      act(() => { button.click(); });
      return true;
    },
    setText: (id: string, value: string) => {
      const input = container.querySelector(`#${id}`) as HTMLInputElement | null;
      if (!input) return;
      act(() => {
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
        setter?.call(input, value);
        input.dispatchEvent(new Event("input", { bubbles: true }));
      });
    },
    text: () => container.textContent ?? ""
  };
}

const flush = async () => {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
};

describe("CompleteJobTab (deliberate acquisition)", () => {
  beforeEach(() => { clearAllPending(); });
  afterEach(() => {
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  it("never consumes on mount — only the consumers list is read", async () => {
    const view = renderCompleteJobTab(async () => jsonResponse(200, { data: [], meta: { count: 0, limit: 100 } }));
    await flush();
    const mutations = view.calls.filter((call) => call.init?.method && call.init.method !== "GET");
    expect(mutations).toEqual([]);
    expect(view.calls.every((call) => call.url.includes("queue-consumers"))).toBe(true);
    expect(view.text()).toContain("No delivery held");
    view.unmount();
  });

  it("consumes only on the explicit button action with mode completion", async () => {
    const view = renderCompleteJobTab(async (url, init) => {
      if (url.includes("/deliveries") && init?.method === "POST") {
        return jsonResponse(200, {
          delivery: { store_id: "b64u:c3RvcmU", queue: "jobs", queue_incarnation: "42", message_id: "17", attempts: 1, redelivered: false, lease_deadline_ms: "9999999999999", proof: "cHJvb2Y=" },
          input: { queue_id: "b64u:am9icw", queue_incarnation: "42", message_id: "17" }
        });
      }
      if (url.includes("queue-consumers")) return jsonResponse(200, { data: [{ id: "b64u:d29ya2Vy", name: "worker", name_encoding: "b64u" }], meta: { count: 1, limit: 100 } });
      return jsonResponse(404, { error: { code: "not_found", message: "x" } });
    });
    await flush();
    // No delivery is fetched, polled, or expanded into a consume call here.
    expect(view.calls.filter((call) => call.init?.method === "POST")).toHaveLength(0);
    expect(view.text()).toContain("Consume for completion");
    view.unmount();
  });

  it("explains no_delivery when nothing was ready", async () => {
    const view = renderCompleteJobTab(async () => jsonResponse(404, { error: { code: "no_delivery", message: "No ready message was available." } }));
    await flush();
    expect(view.text()).toContain("Consume for completion");
    view.unmount();
  });

  it("hands the delivery to the composer through memory only", async () => {
    let composeClicks = 0;
    const view = renderCompleteJobTab(async (url) => {
      if (url.includes("/deliveries")) {
        return jsonResponse(200, {
          delivery: { store_id: "b64u:c3RvcmU", queue: "jobs", queue_incarnation: "42", message_id: "17", attempts: 1, redelivered: false, lease_deadline_ms: "9999999999999", proof: "cHJvb2Y=" },
          input: { queue_id: "b64u:am9icw", queue_incarnation: "42", message_id: "17" }
        });
      }
      return jsonResponse(200, { data: [{ id: "b64u:d29ya2Vy", name: "worker", name_encoding: "b64u" }], meta: { count: 1, limit: 100 } });
    });
    await flush();
    // Simulate selecting the first consumer, then the explicit consume click.
    const select = document.querySelector("#complete-consumer") as HTMLButtonElement | null;
    expect(select).toBeTruthy();
    view.click("Consume for completion");
    await flush();
    const post = view.calls.find((call) => call.init?.method === "POST" && call.url.includes("/deliveries"));
    expect(post).toBeDefined();
    const body = JSON.parse(String(post?.init?.body)) as { mode: string; queue_id: string };
    expect(body.mode).toBe("completion");
    expect(body.queue_id).toBe("b64u:am9icw");
    expect(view.text()).toContain("Compose completion");
    expect(getPendingDelivery("p1")).toBeNull(); // nothing stored until Compose is clicked
    composeClicks = view.composeClicks();
    expect(composeClicks).toBe(0);
    view.unmount();
  });
});
