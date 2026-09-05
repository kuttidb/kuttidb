// @vitest-environment happy-dom
import { describe, expect, it } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { ConfirmDestructive } from "./confirm";

(globalThis as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;

function renderConfirm(props: Omit<Parameters<typeof ConfirmDestructive>[0], never>): {
  rerender: (props: Partial<Parameters<typeof ConfirmDestructive>[0]>) => void;
  unmount: () => void;
} {
  const container = document.createElement("div");
  document.body.append(container);
  const root: Root = createRoot(container);
  let current = { ...props };
  const draw = () => { act(() => { root.render(<ConfirmDestructive {...current} />); }); };
  draw();
  return {
    rerender: (next) => { current = { ...current, ...next }; draw(); },
    unmount: () => act(() => root.unmount())
  };
}

describe("ConfirmDestructive", () => {
  it("renders a failure inside the dialog so the explanation stays visible", () => {
    const view = renderConfirm({
      open: true,
      onOpenChange: () => {},
      title: "Delete queue",
      description: "Durably removes the queue.",
      confirmId: "b64u:abc",
      confirmLabel: "Delete queue",
      error: new Error("The server refused the request."),
      onConfirm: () => {}
    });
    const alert = document.querySelector("[role='alert']");
    expect(alert).not.toBeNull();
    expect(alert?.textContent).toContain("The server refused the request.");
    // The typed-ID instruction and consequence stay in the same modal.
    expect(document.body.textContent).toContain("Type");
    expect(document.body.textContent).toContain("Durably removes the queue.");
    view.unmount();
  });

  it("keeps the final action disabled until the exact ID is typed", () => {
    const view = renderConfirm({
      open: true,
      onOpenChange: () => {},
      title: "Delete queue",
      description: "Durably removes the queue.",
      confirmId: "b64u:abc",
      confirmLabel: "Delete queue",
      onConfirm: () => {}
    });
    const action = () => Array.from(document.querySelectorAll("button")).find((button) => button.textContent === "Delete queue") as HTMLButtonElement;
    expect(action()).toBeDefined();
    expect(action().disabled).toBe(true);
    const input = document.querySelector("input") as HTMLInputElement;

    const type = (value: string) => {
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
      setter?.call(input, value);
      input.dispatchEvent(new Event("input", { bubbles: true }));
      view.rerender({});
    };
    type("b64u:wrong");
    expect(action().disabled).toBe(true);
    type("b64u:abc");
    expect(action().disabled).toBe(false);
    view.unmount();
  });
});
