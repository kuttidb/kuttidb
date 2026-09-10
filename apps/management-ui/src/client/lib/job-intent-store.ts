import { useEffect } from "react";

/**
 * Browser-memory holding area for an acquired completion delivery and a
 * pending completion intent, scoped per connection profile.
 *
 * Hard rules (docs/plans/ATOMIC_JOB_COMPLETION_IMPLEMENTATION_INSTRUCTION.md
 * §11): tokens, delivery proofs, and job payloads never enter
 * localStorage/sessionStorage/URLs/logs — this module holds them in memory
 * only, keys every entry by connection profile so a saved intent can never be
 * submitted through a different profile, and is wiped on lock/disconnect.
 */

export type PendingDelivery = {
  /** Queue identity the delivery came from (raw name and b64u id). */
  queue: string;
  queueId: string;
  storeId: string;
  queueIncarnation: string;
  messageId: string;
  attempts: number;
  redelivered: boolean;
  /** Epoch-ms lease deadline; expired deliveries cannot be committed. */
  leaseDeadlineMs: string;
  /** Opaque one-use proof; memory-only, never persisted or logged. */
  deliveryProof: string;
  acquiredAt: number;
};

const pendingByProfile = new Map<string, PendingDelivery>();

export function setPendingDelivery(profileId: string, delivery: PendingDelivery): void {
  pendingByProfile.set(profileId, delivery);
}

export function getPendingDelivery(profileId: string): PendingDelivery | null {
  return pendingByProfile.get(profileId) ?? null;
}

export function clearPendingDelivery(profileId: string): void {
  pendingByProfile.delete(profileId);
}

/** Drop held private state when a connection is locked or disconnected. */
export function clearAllPending(): void {
  pendingByProfile.clear();
}

/**
 * Effect that clears this profile's pending delivery whenever the live
 * connection disappears (lock, disconnect, session expiry).
 */
export function useClearPendingOnDisconnect(profileId: string, isLive: boolean): void {
  useEffect(() => {
    if (!isLive) clearPendingDelivery(profileId);
  }, [profileId, isLive]);
}
