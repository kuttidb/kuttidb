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
    await navigator.clipboard.writeText(api.requestId);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  };
  return (
    <div role="alert" className={cn("rounded-lg border border-destructive/40 bg-destructive/5 px-3 py-2.5 text-sm", className)}>
      <div className="flex items-start gap-2">
        <AlertTriangle className="size-4 mt-0.5 shrink-0 text-destructive" />
        <div className="min-w-0 flex-1">
          <p className="font-medium text-destructive">{title}</p>
          {hint && <p className="text-muted-foreground mt-0.5">{hint}</p>}
          <div className="flex items-center gap-2 mt-1.5">
            <span className="font-mono text-[11px] text-muted-foreground">{api ? `code: ${api.code}` : "error"}</span>
            {api?.requestId && (
              <Button variant="ghost" size="sm" className="h-6 px-2 text-[11px]" onClick={() => void copyRequestId()}>
                {copied ? <Check className="size-3 mr-1" /> : <Copy className="size-3 mr-1" />} request ID
              </Button>
            )}
            {onRetry && (
              <Button variant="outline" size="sm" className="h-6 px-2 text-[11px]" onClick={onRetry}>
                <RefreshCw className="size-3 mr-1" /> Retry
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
