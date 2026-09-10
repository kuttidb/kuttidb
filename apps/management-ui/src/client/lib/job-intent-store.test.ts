import { describe, expect, it } from "vitest";
import { clearAllPending, clearPendingDelivery, getPendingDelivery, setPendingDelivery } from "./job-intent-store";

const DELIVERY = {
  queue: "jobs",
  queueId: "b64u:am9icw",
  storeId: "b64u:c3RvcmU",
  queueIncarnation: "42",
  messageId: "17",
  attempts: 1,
  redelivered: false,
  leaseDeadlineMs: "99999999999999",
  deliveryProof: "cHJvb2Y=",
  acquiredAt: 1_000
};

describe("pending delivery store (browser memory only)", () => {
  it("holds a delivery per profile and never shares it across profiles", () => {
    setPendingDelivery("profile-a", DELIVERY);
    expect(getPendingDelivery("profile-a")).toEqual(DELIVERY);
    expect(getPendingDelivery("profile-b")).toBeNull();
    clearAllPending();
  });

  it("clears one profile without touching another", () => {
    setPendingDelivery("profile-a", DELIVERY);
    setPendingDelivery("profile-b", { ...DELIVERY, messageId: "18" });
    clearPendingDelivery("profile-a");
    expect(getPendingDelivery("profile-a")).toBeNull();
    expect(getPendingDelivery("profile-b")?.messageId).toBe("18");
    clearAllPending();
  });

  it("keeps nothing after clearAllPending (lock/disconnect)", () => {
    setPendingDelivery("profile-a", DELIVERY);
    clearAllPending();
    expect(getPendingDelivery("profile-a")).toBeNull();
    // And nothing was persisted: the store is a module map, not storage.
    expect(typeof localStorage).toBe("undefined");
  });
});
