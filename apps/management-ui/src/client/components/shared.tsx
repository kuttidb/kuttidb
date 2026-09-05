import type { ReactNode } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Copy, Check, RefreshCw, ChevronLeft, Monitor, Moon, Sun } from "lucide-react";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { usePolling } from "@/hooks/use-polling";
import { useConnections } from "@/state/connections";
import { useTheme, type ThemePreference } from "@/lib/theme";

/*
 * Shared console patterns (docs/design/MANAGEMENT_UI_DESIGN_SYSTEM.md §6):
 * BrandLockup, PageHeader, Section, SummaryStrip, StatusDot, StateBadge,
 * DetailGrid, CopyId, LastRefreshed, CursorPager, EmptyState.
 */

/** Brand mark: original mascot artwork, upright, plus the wordmark. */
export function BrandLockup({ markSize = 28, className, subtitle = "Console" }: {
  markSize?: number;
  className?: string;
  subtitle?: string | null;
}) {
  return (
    <span className={cn("inline-flex items-center gap-2.5", className)}>
      <img
        src="/kuttidb-mark.png"
        alt="KuttiDB"
        width={markSize}
        height={markSize}
        style={{ height: markSize, width: "auto" }}
        className="object-contain"
        draggable={false}
      />
      <span className="min-w-0 leading-none">
        <span className="block text-[22px] leading-none font-bold tracking-[-0.055em]">
          KuttiDB<span className="text-brand-orange">.</span>
        </span>
        {subtitle && <span className="mt-1 block text-xs leading-none text-muted-foreground">{subtitle}</span>}
      </span>
    </span>
  );
}

/** PageHeader: one h1 (26 px), optional one-sentence description, actions. */
export function PageHeader({ title, description, actions, breadcrumb, children }: {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  breadcrumb?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <header className="mb-6">
      {breadcrumb && <div className="mb-2">{breadcrumb}</div>}
      <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
        <div className="min-w-0">
          <h1 className="text-[26px] leading-8 font-semibold tracking-[-0.035em] break-words">{title}</h1>
          {description && <p className="mt-1 max-w-3xl text-sm text-muted-foreground">{description}</p>}
        </div>
        {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
      </div>
      {children}
    </header>
  );
}

/** Section: heading over a top rule, content below; never a nested card. */
export function Section({ title, actions, children, className }: {
  title: ReactNode;
  actions?: ReactNode;
  children?: ReactNode;
  className?: string;
}) {
  return (
    <section className={cn("border-t border-rule-strong pt-3", className)}>
      <div className="mb-3 flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
        <h2 className="text-base leading-6 font-semibold tracking-[-0.015em]">{title}</h2>
        {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
      </div>
      {children}
    </section>
  );
}

/** SummaryStrip: shared top/bottom rules, equal columns, internal dividers. */
export function SummaryStrip({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={cn(
        "grid grid-cols-1 border-y border-rule-strong [&>*+*]:border-border [&>*+*]:border-t md:grid-flow-col md:auto-cols-fr md:[&>*+*]:border-t-0 md:[&>*+*]:border-l",
        className
      )}
      data-slot="summary-strip"
    >
      {children}
    </div>
  );
}

/** One summary cell: label, main quantity, up to three secondary facts. */
export function SummaryCell({ label, href, quantity, facts, className }: {
  label: ReactNode;
  href?: string;
  quantity?: ReactNode;
  facts?: ReactNode;
  className?: string;
}) {
  const heading = href
    ? <a href={href} className="text-sm font-medium underline decoration-border underline-offset-4 hover:decoration-foreground">{label}</a>
    : <span className="text-sm font-medium">{label}</span>;
  return (
    <div className={cn("min-w-0 px-5 py-4 max-md:px-0", className)}>
      <div className="flex items-baseline justify-between gap-2">{heading}</div>
      {quantity !== undefined && <p className="mt-1 text-[28px] leading-8 font-medium tabular-nums">{quantity}</p>}
      {facts && <div className="mt-1.5 text-xs leading-[18px] text-muted-foreground">{facts}</div>}
    </div>
  );
}

const stateVariants: Record<string, "success" | "info" | "neutral" | "warning" | "destructive"> = {
  succeeded: "success",
  healthy: "success",
  ready: "success",
  available: "success",
  running: "info",
  active: "info",
  in_flight: "info",
  "in-flight": "info",
  queued: "neutral",
  delayed: "neutral",
  cancelled: "neutral",
  pending: "neutral",
  stale: "warning",
  degraded: "warning",
  unknown: "warning",
  expired: "warning",
  failed: "destructive",
  unhealthy: "destructive"
};

const stateLabels: Record<string, string> = {
  in_flight: "in flight",
  "in-flight": "in flight"
};

/** Status tag for tables; pair the variant color with its word. */
export function StateBadge({ state, className }: { state: string; className?: string }) {
  const variant = stateVariants[state] ?? "neutral";
  return (
    <Badge variant={variant} className={cn("font-mono text-xs", className)}>
      {stateLabels[state] ?? state}
    </Badge>
  );
}

/** Small status dot; circles are reserved for dots and switch knobs. */
export function StatusDot({ tone, className }: { tone: "success" | "info" | "warning" | "destructive" | "neutral"; className?: string }) {
  const colors: Record<typeof tone, string> = {
    success: "bg-success",
    info: "bg-info",
    warning: "bg-warning",
    destructive: "bg-destructive",
    neutral: "bg-muted-foreground"
  };
  return <span aria-hidden className={cn("inline-block size-2 shrink-0 rounded-full", colors[tone], className)} />;
}

export function DetailGrid({ rows, className }: { rows: { label: string; value: ReactNode; mono?: boolean }[]; className?: string }) {
  return (
    <dl className={cn("grid grid-cols-[minmax(120px,auto)_1fr] gap-x-4 gap-y-2 text-sm", className)}>
      {rows.map((row) => (
        <div key={row.label} className="contents">
          <dt className="text-muted-foreground">{row.label}</dt>
          <dd className={cn("min-w-0 break-words", row.mono && "font-mono text-xs")}>{row.value}</dd>
        </div>
      ))}
    </dl>
  );
}

/** Copy control: success shows only after the clipboard write resolves. */
export function CopyId({ id, className, label }: { id: string; className?: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(id);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch {
      setCopied(false);
    }
  };
  return (
    <button
      type="button"
      title={label ?? `Copy ${id}`}
      aria-label={label ?? `Copy ${id}`}
      className={cn("inline-flex max-w-full items-center gap-1 font-mono text-xs text-muted-foreground transition-colors hover:text-foreground", className)}
      onClick={(event) => {
        event.stopPropagation();
        void copy();
      }}
    >
      <span className="truncate">{id}</span>
      {copied ? <Check className="size-3 shrink-0 text-success" aria-label="Copied" /> : <Copy className="size-3 shrink-0 opacity-60" />}
    </button>
  );
}

/**
 * Refresh affordance: shows the last successful read time and marks stale
 * data explicitly. A stale state keeps the old data but never looks current.
 */
export function LastRefreshed({ lastUpdated, onRefresh, stale = false, className }: {
  lastUpdated: number | null;
  onRefresh: () => void;
  stale?: boolean;
  className?: string;
}) {
  const stamp = lastUpdated ? new Date(lastUpdated).toLocaleTimeString() : null;
  return (
    <span className={cn("inline-flex items-center gap-2 text-xs text-muted-foreground", className)}>
      {stale
        ? <span className="inline-flex items-center gap-1.5 text-warning"><StatusDot tone="warning" />Stale — showing {stamp ?? "last"} read</span>
        : <span>{lastUpdated ? `Updated ${stamp}` : "Not loaded yet"}</span>}
      <Button variant="ghost" size="icon" className="size-6" onClick={onRefresh} aria-label="Refresh now" title="Refresh now">
        <RefreshCw className="size-3" />
      </Button>
    </span>
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
    <div className="flex flex-wrap items-center gap-2">
      <Button variant="outline" size="sm" disabled={backStack.length === 0} onClick={onBack}>
        <ChevronLeft className="size-4" /> Back
      </Button>
      <Button variant="outline" size="sm" disabled={!nextCursor} onClick={onNext}>Next page</Button>
      {weaklyConsistent && <span className="text-xs text-muted-foreground">May change while you browse — weakly consistent.</span>}
    </div>
  );
}

/** Empty collection: a plain sentence plus the permitted action. */
export function EmptyState({ title, hint, action, className }: {
  title: string;
  hint?: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("grid justify-items-start gap-1.5 px-4 py-10", className)}>
      <p className="text-sm font-medium">{title}</p>
      {hint && <p className="text-sm text-muted-foreground">{hint}</p>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

/** Theme control: resolves an explicit stored preference before OS preference. */
export function ThemeMenu({ className }: { className?: string }) {
  const { preference, setPreference, resolved } = useTheme();
  const options: { value: ThemePreference; label: string; icon: ReactNode }[] = [
    { value: "light", label: "Light", icon: <Sun className="size-4" /> },
    { value: "dark", label: "Dark", icon: <Moon className="size-4" /> },
    { value: "system", label: "System", icon: <Monitor className="size-4" /> }
  ];
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="icon" className={className} aria-label={`Theme: ${preference}`} title={`Theme: ${preference}`}>
          {resolved === "dark" ? <Moon className="size-4" /> : <Sun className="size-4" />}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        {options.map((option) => (
          <DropdownMenuItem key={option.value} onClick={() => setPreference(option.value)}>
            {option.icon}
            {option.label}
            {preference === option.value && <Check className="ml-auto size-3.5" />}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/**
 * Standalone branded frame for pages outside the connected shell: Connections,
 * locked sessions, unknown profiles. One top row (brand + theme), bounded
 * content width, consistent 32 px desktop padding.
 */
export function StandaloneFrame({ children, wide = false }: { children: ReactNode; wide?: boolean }) {
  return (
    <div className="min-h-dvh bg-background">
      <header className="border-b border-rule-strong">
        <div className="mx-auto flex w-full max-w-[1120px] items-center justify-between gap-4 px-4 py-4 md:px-8">
          <BrandLockup markSize={32} />
          <ThemeMenu />
        </div>
      </header>
      <div className={cn("mx-auto w-full px-4 py-8 md:px-8", wide ? "max-w-[1120px]" : "max-w-[720px]")}>
        {children}
      </div>
    </div>
  );
}

/**
 * Connection identity for destructive-dialog context: the human label plus the
 * endpoint, so the target server is named inside the modal, not only in the
 * page header behind it.
 */
export function ConnectionContextLine({ profileId, className }: { profileId: string; className?: string }) {
  const { profiles, live } = useConnections();
  const label = profiles.find((profile) => profile.id === profileId)?.label;
  const endpoint = live.get(profileId)?.endpoint;
  return (
    <span className={cn("inline-flex flex-wrap items-baseline gap-x-1.5", className)}>
      <span className="font-medium text-foreground">Connection:</span>
      <span>{label ?? "unknown profile"}</span>
      {endpoint && <span className="font-mono text-xs">{endpoint}</span>}
    </span>
  );
}

export { usePolling };
export { Skeleton } from "@/components/ui/skeleton";
