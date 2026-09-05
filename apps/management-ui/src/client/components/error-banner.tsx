import { useState } from "react";
import { AlertTriangle, Copy, Check, RefreshCw } from "lucide-react";
import { ApiError } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

/**
 * One consistent error surface: stable code, actionable hint, and a copyable
 * safe request ID. Never shows tokens or raw network stack traces.
 */
export function ErrorBanner({ error, onRetry, className }: { error: unknown; onRetry?: () => void; className?: string }) {
  const [copied, setCopied] = useState(false);
  if (!error) return null;
  const api = error instanceof ApiError ? error : null;
  const title = api ? api.message : error instanceof Error ? error.message : "Something went wrong.";
  const hint = api?.hint ?? null;
  const copyRequestId = async () => {
    if (!api?.requestId) return;
    try {
      await navigator.clipboard.writeText(api.requestId);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch { /* clipboard unavailable; the ID stays visible */ }
  };
  return (
    <div role="alert" className={cn("rounded-none border border-destructive/40 bg-danger-surface px-3 py-2.5 text-sm", className)}>
      <div className="flex items-start gap-2">
        <AlertTriangle className="mt-0.5 size-4 shrink-0 text-destructive" aria-hidden />
        <div className="min-w-0 flex-1">
          <p className="font-medium text-destructive">{title}</p>
          {hint && <p className="mt-0.5 text-muted-foreground">{hint}</p>}
          <div className="mt-1.5 flex flex-wrap items-center gap-2">
            <span className="font-mono text-xs text-muted-foreground">{api ? `code: ${api.code}` : "error"}</span>
            {api?.requestId && (
              <Button variant="ghost" size="xs" onClick={() => void copyRequestId()}>
                {copied ? <Check className="mr-1 size-3 text-success" /> : <Copy className="mr-1 size-3" />} request ID
              </Button>
            )}
            {onRetry && (
              <Button variant="outline" size="xs" onClick={onRetry}>
                <RefreshCw className="mr-1 size-3" /> Retry
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
