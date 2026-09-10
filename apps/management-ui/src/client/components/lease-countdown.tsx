import { useEffect, useState } from "react";
import { formatDuration } from "@/lib/format";

/**
 * Live lease countdown for an acquired delivery. Renders text only (no live
 * region churn every second for screen readers), and reports expiry so the
 * caller can disable the commit action.
 */
export function useLeaseCountdown(leaseDeadlineMs: string | number | null): { remainingMs: number | null; expired: boolean } {
  const parsed = typeof leaseDeadlineMs === "string" ? Number(leaseDeadlineMs) : leaseDeadlineMs;
  const deadline = parsed !== null && Number.isFinite(parsed) ? parsed : null;
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (deadline === null) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [deadline]);
  if (deadline === null || deadline <= 0) return { remainingMs: null, expired: false };
  const remainingMs = deadline - now;
  return { remainingMs, expired: remainingMs <= 0 };
}

export function LeaseCountdown({ leaseDeadlineMs, className }: { leaseDeadlineMs: string | number | null; className?: string }) {
  const { remainingMs, expired } = useLeaseCountdown(leaseDeadlineMs);
  if (remainingMs === null) return <span className={className}>Lease deadline unknown</span>;
  if (expired) return <span className={`${className ?? ""} text-destructive`}>Lease expired — this delivery can no longer be committed</span>;
  return (
    <span className={className}>
      Lease expires in <span className="font-mono tabular-nums">{formatDuration(remainingMs)}</span>
    </span>
  );
}
