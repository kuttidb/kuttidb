// @vitest-environment happy-dom
import { describe, expect, it, beforeEach, vi } from "vitest";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { usePolling } from "./use-polling";

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

function renderHookToDom(useHookImplementation: () => ReturnType<typeof usePolling>): {
  result: { current: ReturnType<typeof usePolling> | null };
  unmount: () => void;
} {
  const container = document.createElement("div");
  document.body.append(container);
  const result: { current: ReturnType<typeof usePolling> | null } = { current: null };
  function Probe() {
    result.current = useHookImplementation();
    return null;
  }
  const root = createRoot(container);
  act(() => { root.render(<Probe />); });
  return {
    result,
    unmount: () => act(() => root.unmount())
  };
}

describe("usePolling", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it("marks a background failure stale only after a previous success", async () => {
    let shouldFail = false;
    let calls = 0;
    const hook = renderHookToDom(() => usePolling(async () => {
      calls += 1;
      if (shouldFail) throw new Error("read failed");
    }, 10));

    // First load succeeds.
    await act(async () => { await vi.runOnlyPendingTimersAsync(); });
    expect(hook.result.current?.lastUpdated).not.toBeNull();
    expect(hook.result.current?.error).toBeNull();
    expect(hook.result.current?.stale).toBe(false);

    // Background failure after success: keep data, mark it stale.
    shouldFail = true;
    await act(async () => { await vi.runOnlyPendingTimersAsync(); });
    expect(hook.result.current?.error).not.toBeNull();
    expect(hook.result.current?.stale).toBe(true);
    expect(hook.result.current?.lastUpdated).not.toBeNull();

    // Recovery clears the stale marker.
    shouldFail = false;
    await act(async () => { await vi.runOnlyPendingTimersAsync(); });
    expect(hook.result.current?.error).toBeNull();
    expect(hook.result.current?.stale).toBe(false);
    expect(calls).toBeGreaterThanOrEqual(3);
    hook.unmount();
  });

  it("does not mark the first failed load stale — there is no old data", async () => {
    const hook = renderHookToDom(() => usePolling(async () => {
      throw new Error("initial failure");
    }, 10));
    await act(async () => { await vi.runOnlyPendingTimersAsync(); });
    expect(hook.result.current?.error).not.toBeNull();
    expect(hook.result.current?.lastUpdated).toBeNull();
    expect(hook.result.current?.stale).toBe(false);
    hook.unmount();
  });
});
