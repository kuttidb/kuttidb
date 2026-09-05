import { useCallback, useState } from "react";
import { ArrowRight } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ErrorBanner } from "@/components/error-banner";
import { DetailGrid, EmptyState, LastRefreshed, PageHeader, Section, StateBadge, StatusDot, SummaryCell, SummaryStrip } from "@/components/shared";
import { usePolling } from "@/hooks/use-polling";
import { useConnections } from "@/state/connections";
import { admin, list, ApiError } from "@/lib/api";
import { formatBytes, formatDuration } from "@/lib/format";
import type { StatusShape, MaintenanceEntry, JobEntry } from "@/lib/types";

export function OverviewView({ profileId, onNavigate }: { profileId: string; onNavigate: (segments: string[]) => void }) {
  const { capabilities, mutationsBlocked } = useConnections();
  const [status, setStatus] = useState<StatusShape | null>(null);
  const [maintenance, setMaintenance] = useState<MaintenanceEntry[]>([]);
  const [jobs, setJobs] = useState<JobEntry[]>([]);
  const [loadError, setLoadError] = useState<Error | null>(null);

  const loader = useCallback(async () => {
    const [statusResponse, maintenanceResponse, jobsResponse] = await Promise.all([
      admin<StatusShape>(profileId, "status"),
      list<MaintenanceEntry>(profileId, "maintenance"),
      list<JobEntry>(profileId, "jobs?limit=6")
    ]);
    setStatus(statusResponse.json);
    setMaintenance(maintenanceResponse.data);
    setJobs(jobsResponse.data);
  }, [profileId]);

  const { lastUpdated, error, stale, refresh } = usePolling(loader, 15_000);
  const caps = capabilities.get(profileId);
  // Health fields are unknown until the first status read; never default to healthy.
  const auditHealthy: boolean | null = status ? status.audit.healthy : null;
  const persistenceHealthy: boolean | null = status ? status.persistence_healthy : null;
  const ready = status?.ready ?? false;
  /**
   * Real hash links with exactly one connection prefix (matching the shell's
   * `#/c/{profileId}/...` routes). The router picks these up directly.
   */
  const hrefFor = useCallback((path: string[]) =>
    `#/c/${encodeURIComponent(profileId)}/${path.map(encodeURIComponent).join("/")}`, [profileId]);

  return (
    <div>
      <PageHeader
        title="Overview"
        description="Bounded process and engine health. Dashboard state is weakly consistent — not a transactional snapshot."
        actions={
          <span className="flex items-center gap-3">
            <span className="text-xs text-muted-foreground">Weakly consistent</span>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
          </span>
        }
      />
      {mutationsBlocked(profileId) && (
        <ErrorBanner error={new ApiError("audit_unavailable", "The audit trail is unhealthy. All mutations are blocked for this connection; reads remain available.", 503)} className="mb-4" />
      )}
      {error && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}

      {!status ? (
        <>
          <div className="mb-6 flex flex-wrap items-center gap-x-5 gap-y-2 text-sm">
            <span className="flex items-center gap-2 font-medium text-muted-foreground">
              <StatusDot tone="neutral" />
              {error ? "Unknown" : "Checking…"}
            </span>
            <span className="text-xs text-muted-foreground">Readiness, durability, uptime, and audit state stay unknown until the first status read completes.</span>
          </div>
          {!error && (
            <div className="grid gap-6">
              <div className="grid grid-cols-1 sm:grid-cols-3">
                {Array.from({ length: 3 }, (_, index) => <Skeleton key={index} className="h-24" />)}
              </div>
              <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
                <Skeleton className="h-48 lg:col-span-2" />
                <Skeleton className="h-48" />
              </div>
            </div>
          )}
        </>
      ) : (
        <>
          <div className="mb-6 flex flex-wrap items-center gap-x-5 gap-y-2 text-sm">
            <span className="flex items-center gap-2 font-medium">
              <StatusDot tone={ready && persistenceHealthy ? "success" : "destructive"} />
              {ready ? (persistenceHealthy ? "Ready" : "Degraded") : "Not ready"}
            </span>
            <span className="text-muted-foreground">v{status.server_version}</span>
            <span className="text-muted-foreground">uptime {formatDuration(status.uptime_seconds * 1000)}</span>
            <Badge variant="outline" className="font-mono text-xs">{status.durability}</Badge>
            <Badge variant={auditHealthy ? "success" : "destructive"} className="font-mono text-xs">
              audit {auditHealthy ? "healthy" : "unhealthy"}
            </Badge>
            <span className="text-muted-foreground text-xs ml-auto">{status.event_loops} event loops · {status.event_backend}</span>
          </div>

          <SummaryStrip className="mb-6">
            <SummaryCell
              label={<SummaryLink href={hrefFor(["keyspace"])}>Keyspace</SummaryLink>}
              quantity={status.keyspace.entry_count}
              facts={`Live bytes ${formatBytes(status.keyspace.live_bytes)} · Expired ${status.keyspace.expired_count} · Evicted ${status.keyspace.evicted_count}`}
            />
            <SummaryCell
              label={<SummaryLink href={hrefFor(["queues"])}>Queues</SummaryLink>}
              quantity={status.queues.count}
              facts={`Ready ${status.queues.ready_depth} · In flight ${status.queues.in_flight} · Dead letter ${status.queues.dead_letter_count}`}
            />
            <SummaryCell
              label={<SummaryLink href={hrefFor(["streams"])}>Streams</SummaryLink>}
              quantity={status.streams.count}
              facts={`Partitions ${status.streams.partition_count} · Retained ${formatBytes(status.streams.retained_bytes)} · Groups ${status.streams.group_count}`}
            />
          </SummaryStrip>

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
            <Section title="Recent jobs" className="lg:col-span-2">
              {jobs.length === 0
                ? <EmptyState title="No maintenance jobs yet." hint="Maintenance jobs appear here after they run." className="px-0" />
                : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Job</TableHead>
                        <TableHead>Kind</TableHead>
                        <TableHead>State</TableHead>
                        <TableHead className="text-right"><span className="sr-only">Actions</span></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {jobs.map((job) => (
                        <TableRow key={job.job_id}>
                          <TableCell className="font-mono text-xs">{job.job_id}</TableCell>
                          <TableCell className="text-muted-foreground">{job.kind}</TableCell>
                          <TableCell><StateBadge state={job.state} /></TableCell>
                          <TableCell className="text-right">
                            <a className="inline-flex h-6 items-center gap-1 px-2 text-xs text-muted-foreground underline decoration-border underline-offset-4 hover:text-foreground hover:decoration-foreground" href={hrefFor(["operations", "maintenance"])}>
                              view <ArrowRight className="size-3" />
                            </a>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
            </Section>
            <Section title="Persistence & engines">
              <div className="grid gap-3">
                <DetailGrid
                  rows={[
                    ...maintenance.map((engine) => ({
                      label: engine.engine,
                      value: (
                        <span className="inline-flex items-center gap-1.5">
                          <StatusDot tone={engine.checkpoint_available ? "success" : "neutral"} />
                          checkpoint {engine.checkpoint_available ? "available" : "unavailable"}
                        </span>
                      )
                    })),
                    { label: "Enabled engines", value: (caps?.enabled_engines ?? []).join(", ") || "unknown" },
                    { label: "API contract", value: caps?.management_api_contract ?? "?", mono: true },
                    { label: "Queue persistence", value: status.queues.persistence_healthy ? "healthy" : "unhealthy" },
                    { label: "Stream persistence", value: status.streams.persistence_healthy ? "healthy" : "unhealthy" }
                  ]}
                />
                {loadError && <ErrorBanner error={loadError} />}
              </div>
            </Section>
          </div>

          <Section title="Management pressure" className="mt-6">
            <div className="flex flex-wrap gap-x-10 gap-y-3 text-sm">
              {([
                ["Active tails", status.management.active_tails],
                ["Active deliveries", status.management.active_deliveries],
                ["Active claims", status.management.active_claims],
                ["Queued jobs", status.management.queued_jobs],
                ["Running jobs", status.management.running_jobs]
              ] as [string, number][]).map(([label, value]) => (
                <div key={label}>
                  <p className="text-xs text-muted-foreground">{label}</p>
                  <p className="font-medium tabular-nums">{value}</p>
                </div>
              ))}
            </div>
          </Section>
        </>
      )}
    </div>
  );
}

/**
 * Summary headings and job links are real hash links with a single connection
 * prefix, so they are announced as links and survive middle-click/new-tab.
 */
function SummaryLink({ children, href }: { children: React.ReactNode; href: string }) {
  return (
    <a
      href={href}
      className="text-sm font-medium underline decoration-border underline-offset-4 transition-colors hover:decoration-foreground"
    >
      {children}
    </a>
  );
}
