import { useCallback, useState } from "react";
import { Database, ListOrdered, Workflow, Gauge, ScrollText, ArrowRight } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorBanner } from "@/components/error-banner";
import { LastRefreshed, PageHeader, StateBadge } from "@/components/shared";
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

  const { lastUpdated, error, refresh } = usePolling(loader, 15_000);
  const caps = capabilities.get(profileId);
  const auditHealthy = status?.audit?.healthy ?? true;
  const persistenceHealthy = status?.persistence_healthy ?? true;
  const ready = status?.ready ?? false;

  return (
    <div>
      <PageHeader
        title="Overview"
        description="Bounded process and engine health. Dashboard state is weakly consistent — not a transactional snapshot."
        actions={<LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} polling />}
      />
      {error && !status && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {!status && !error && (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {Array.from({ length: 4 }, (_, index) => <Skeleton key={index} className="h-28 rounded-xl" />)}
        </div>
      )}
      {status && (
        <>
          {mutationsBlocked(profileId) && (
            <ErrorBanner error={new ApiError("audit_unavailable", "The audit trail is unhealthy. All mutations are blocked for this connection; reads remain available.", 503)} className="mb-4" />
          )}
          <div className="rounded-xl border bg-card p-4 mb-4">
            <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
              <span className="flex items-center gap-2 font-medium">
                <span className={`size-2 rounded-full ${ready && persistenceHealthy ? "bg-chart-3" : "bg-destructive"}`} />
                {ready ? (persistenceHealthy ? "Ready" : "Degraded") : "Not ready"}
              </span>
              <span className="text-muted-foreground">v{status.server_version}</span>
              <span className="text-muted-foreground">uptime {formatDuration(status.uptime_seconds * 1000)}</span>
              <Badge variant="outline" className="font-mono text-[11px]">{status.durability}</Badge>
              <Badge variant="outline" className={`font-mono text-[11px] ${auditHealthy ? "bg-chart-3/15 text-chart-3 border-transparent" : "bg-destructive/10 text-destructive border-transparent"}`}>
                audit {auditHealthy ? "healthy" : "unhealthy"}
              </Badge>
              <span className="text-muted-foreground text-xs ml-auto">{status.event_loops} event loops · {status.event_backend}</span>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4 mb-4">
            <EngineCard
              icon={<Database className="size-4" />} title="Keyspace" onOpen={() => onNavigate(["c", profileId, "keyspace"])}
              rows={[["entries", status.keyspace.entry_count], ["live bytes", formatBytes(status.keyspace.live_bytes)], ["expired", status.keyspace.expired_count], ["evicted", status.keyspace.evicted_count]]}
            />
            <EngineCard
              icon={<ListOrdered className="size-4" />} title="Queues" onOpen={() => onNavigate(["c", profileId, "queues"])}
              rows={[["queues", status.queues.count], ["ready", status.queues.ready_depth], ["in-flight", status.queues.in_flight], ["dead-letter", status.queues.dead_letter_count]]}
            />
            <EngineCard
              icon={<Workflow className="size-4" />} title="Streams" onOpen={() => onNavigate(["c", profileId, "streams"])}
              rows={[["streams", status.streams.count], ["partitions", status.streams.partition_count], ["retained", formatBytes(status.streams.retained_bytes)], ["groups", status.streams.group_count]]}
            />
            <EngineCard
              icon={<Gauge className="size-4" />} title="Management pressure"
              rows={[["tails", status.management.active_tails], ["deliveries", status.management.active_deliveries], ["claims", status.management.active_claims], ["jobs", `${status.management.queued_jobs}q / ${status.management.running_jobs}r`]]}
            />
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center gap-2"><ScrollText className="size-4 text-muted-foreground" />Recent jobs</CardTitle>
              </CardHeader>
              <CardContent>
                {jobs.length === 0
                  ? <p className="text-sm text-muted-foreground">No maintenance jobs yet.</p>
                  : (
                    <ul className="space-y-1.5">
                      {jobs.map((job) => (
                        <li key={job.job_id} className="flex items-center gap-2 text-sm">
                          <StateBadge state={job.state} />
                          <span className="font-mono text-xs">{job.job_id}</span>
                          <span className="text-muted-foreground truncate">{job.kind}</span>
                          <Button variant="ghost" size="sm" className="ml-auto h-6 px-2 text-xs" onClick={() => onNavigate(["c", profileId, "operations", "maintenance"])}>
                            view <ArrowRight className="size-3" />
                          </Button>
                        </li>
                      ))}
                    </ul>
                  )}
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium">Persistence & engines</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex flex-wrap gap-2">
                  {maintenance.map((engine) => (
                    <Badge key={engine.engine} variant="outline" className={`font-mono text-[11px] ${engine.checkpoint_available ? "bg-chart-3/15 text-chart-3 border-transparent" : "bg-muted text-muted-foreground border-transparent"}`}>
                      {engine.engine}: checkpoint {engine.checkpoint_available ? "available" : "unavailable"}
                    </Badge>
                  ))}
                </div>
                <p className="text-xs text-muted-foreground">
                  Enabled engines: {(caps?.enabled_engines ?? []).join(", ") || "unknown"}. Contract {caps?.management_api_contract ?? "?"}.
                  Queue persistence {status.queues.persistence_healthy ? "healthy" : "unhealthy"}, stream persistence {status.streams.persistence_healthy ? "healthy" : "unhealthy"}.
                </p>
                {loadError && <ErrorBanner error={loadError} />}
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </div>
  );
}

function EngineCard({ icon, title, rows, onOpen }: { icon: React.ReactNode; title: string; rows: [string, React.ReactNode][]; onOpen?: () => void }) {
  return (
    <Card className={onOpen ? "cursor-pointer hover:border-primary/40 transition-colors" : undefined} onClick={onOpen}>
      <CardHeader className="pb-1">
        <CardTitle className="text-sm font-medium flex items-center gap-2 text-muted-foreground">{icon}{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <dl className="grid grid-cols-2 gap-x-4 gap-y-1.5 text-sm">
          {rows.map(([label, value]) => (
            <div key={label}>
              <dt className="text-xs text-muted-foreground">{label}</dt>
              <dd className="font-medium tabular-nums">{value}</dd>
            </div>
          ))}
        </dl>
      </CardContent>
    </Card>
  );
}
