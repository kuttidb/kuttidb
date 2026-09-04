import type { ReactNode } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Copy, Check, RefreshCw, ChevronLeft } from "lucide-react";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { usePolling } from "@/hooks/use-polling";

export function PageHeader({ title, description, actions, children }: { title: string; description?: ReactNode; actions?: ReactNode; children?: ReactNode }) {
  return (
    <header className="mb-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
          {description && <p className="text-sm text-muted-foreground mt-1">{description}</p>}
        </div>
        {actions && <div className="flex items-center gap-2">{actions}</div>}
      </div>
      {children}
    </header>
  );
}

export function CopyId({ id, className }: { id: string; className?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      title={`Copy ${id}`}
      className={cn("inline-flex items-center gap-1 font-mono text-xs text-muted-foreground hover:text-foreground transition-colors max-w-full", className)}
      onClick={(event) => {
        event.stopPropagation();
        void navigator.clipboard.writeText(id);
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1200);
      }}
    >
      <span className="truncate">{id}</span>
      {copied ? <Check className="size-3 shrink-0 text-chart-3" /> : <Copy className="size-3 shrink-0 opacity-60" />}
    </button>
  );
}

const stateStyles: Record<string, string> = {
  succeeded: "bg-chart-3/15 text-chart-3 border-transparent",
  healthy: "bg-chart-3/15 text-chart-3 border-transparent",
  ready: "bg-chart-3/15 text-chart-3 border-transparent",
  active: "bg-primary/10 text-primary border-transparent",
  running: "bg-primary/10 text-primary border-transparent",
  queued: "bg-secondary text-secondary-foreground border-transparent",
  delayed: "bg-secondary text-secondary-foreground border-transparent",
  failed: "bg-destructive/10 text-destructive border-transparent",
  cancelled: "bg-muted text-muted-foreground border-transparent",
  in_flight: "bg-chart-2/15 text-chart-2 border-transparent",
  "in-flight": "bg-chart-2/15 text-chart-2 border-transparent"
};

export function StateBadge({ state, className }: { state: string; className?: string }) {
  return <Badge variant="outline" className={cn("font-mono text-[11px]", stateStyles[state] ?? "bg-secondary", className)}>{state}</Badge>;
}

export function DetailGrid({ rows }: { rows: { label: string; value: ReactNode; mono?: boolean }[] }) {
  return (
    <dl className="grid grid-cols-[minmax(120px,auto)_1fr] gap-x-4 gap-y-2 text-sm">
      {rows.map((row) => (
        <div key={row.label} className="contents">
          <dt className="text-muted-foreground">{row.label}</dt>
          <dd className={cn("min-w-0 break-words", row.mono && "font-mono text-xs")}>{row.value}</dd>
        </div>
      ))}
    </dl>
  );
}

export function LastRefreshed({ lastUpdated, onRefresh, polling }: { lastUpdated: number | null; onRefresh: () => void; polling?: boolean }) {
  return (
    <div className="flex items-center gap-2 text-xs text-muted-foreground">
      {polling && <span className="relative flex size-2"><span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-chart-3 opacity-60" /><span className="relative inline-flex size-2 rounded-full bg-chart-3" /></span>}
      <span>{lastUpdated ? `Updated ${new Date(lastUpdated).toLocaleTimeString()}` : "Not loaded yet"}</span>
      <Button variant="ghost" size="icon" className="size-6" onClick={onRefresh} title="Refresh now"><RefreshCw className="size-3" /></Button>
    </div>
  );
}

/** Cursor pagination with a local back stack; cursors stay opaque. */
export function CursorPager({ nextCursor, backStack, onBack, onNext, weaklyConsistent }: {
  nextCursor: string | null | undefined;
  backStack: string[];
  onBack: () => void;
  onNext: () => void;
  weaklyConsistent?: boolean | undefined;
}) {
  return (
    <div className="flex items-center gap-2">
      <Button variant="outline" size="sm" disabled={backStack.length === 0} onClick={onBack}>
        <ChevronLeft className="size-4" /> Back
      </Button>
      <Button variant="outline" size="sm" disabled={!nextCursor} onClick={onNext}>Next page</Button>
      {weaklyConsistent && <span className="text-xs text-muted-foreground">May change while you browse — weakly consistent.</span>}
    </div>
  );
}

export { usePolling };
