import { useEffect, useRef, useState } from "react";

/**
 * Poll a loader with jitter, pause while the tab is hidden, and back off on
 * failure. The loader identity may change every render; only the latest runs.
 */
export function usePolling(loader: () => Promise<void>, intervalMs: number, enabled = true): { lastUpdated: number | null; error: Error | null; refresh: () => void } {
  const loaderRef = useRef(loader);
  loaderRef.current = loader;
  const [tick, setTick] = useState(0);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const failuresRef = useRef(0);

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;
    let timer: number | undefined;
    const run = async () => {
      if (document.hidden) { schedule(2000); return; }
      try {
        await loaderRef.current();
        if (cancelled) return;
        failuresRef.current = 0;
        setError(null);
        setLastUpdated(Date.now());
        schedule(intervalMs * (0.85 + Math.random() * 0.3));
      } catch (reason) {
        if (cancelled) return;
        failuresRef.current += 1;
        setError(reason instanceof Error ? reason : new Error(String(reason)));
        schedule(Math.min(intervalMs * 2 ** failuresRef.current, 30_000));
      }
    };
    const schedule = (delay: number) => {
      timer = window.setTimeout(() => { if (!cancelled) void run(); }, delay);
    };
    void run();
    return () => { cancelled = true; if (timer !== undefined) window.clearTimeout(timer); };
  }, [intervalMs, enabled, tick]);

  return { lastUpdated, error, refresh: () => setTick((value) => value + 1) };
}
