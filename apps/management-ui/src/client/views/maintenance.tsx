import { useCallback, useState } from "react";
import { HardDriveDownload, Ban } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ConfirmDestructive } from "@/components/confirm";
import { ErrorBanner } from "@/components/error-banner";
import { ConnectionContextLine, CopyId, DetailGrid, EmptyState, LastRefreshed, PageHeader, Section, Skeleton, StateBadge, StatusDot } from "@/components/shared";
import { admin, list, newIdempotencyKey } from "@/lib/api";
import { formatTimestamp } from "@/lib/format";
import type { JobEntry, MaintenanceEntry } from "@/lib/types";

export function MaintenanceView({ profileId }: { profileId: string }) {
  const [engines, setEngines] = useState<MaintenanceEntry[]>([]);
  const [jobs, setJobs] = useState<JobEntry[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [cancelTarget, setCancelTarget] = useState<JobEntry | null>(null);
  const [detailJob, setDetailJob] = useState<JobEntry | null>(null);

  const loader = useCallback(async () => {
    const [maintenanceResponse, jobsResponse] = await Promise.all([
      list<MaintenanceEntry>(profileId, "maintenance"),
      list<JobEntry>(profileId, "jobs")
    ]);
    setEngines(maintenanceResponse.data);
    setJobs(jobsResponse.data);
  }, [profileId]);
  const { lastUpdated, error: pollError, stale, refresh } = usePolling(loader, 5_000);

  const checkpoint = async (kind: "keyspace-checkpoint" | "queue-checkpoint" | "stream-checkpoint" | "checkpoint-all") => {
    setBusy(true); setError(null);
    try {
      const response = await admin<{ data: { job_id: string } }>(profileId, `maintenance/${kind}`, { method: "POST", idempotencyKey: newIdempotencyKey(), body: {} });
      toast.success(`Job ${response.json.data.job_id} queued`);
      refresh();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const cancel = async (job: JobEntry) => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `jobs/${job.job_id}`, { method: "DELETE", idempotencyKey: newIdempotencyKey(), body: {} });
      toast.success(`Job ${job.job_id} cancellation requested`);
      setCancelTarget(null);
      refresh();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const viewError = error ?? pollError;

  return (
    <div>
      <PageHeader
        title="Maintenance"
        description="Checkpoints run as bounded asynchronous jobs. Only queued jobs are cancellable; an accepted job is queued, not completed."
        actions={<LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />}
      />
      {viewError && <ErrorBanner error={viewError} onRetry={refresh} className="mb-4" />}
      <Section title="Checkpoints">
        <div className="grid gap-3">
          {lastUpdated === null && !viewError ? (
            <div className="grid gap-2" aria-busy="true">
              {Array.from({ length: 2 }, (_, index) => <Skeleton key={index} className="h-9 w-64" />)}
            </div>
          ) : (
            <ul className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
              {engines.map((engine) => (
                <li key={engine.engine} className="flex items-center justify-between gap-2 border px-3 py-2.5">
                  <span className="inline-flex items-center gap-2 text-sm">
                    <StatusDot tone={engine.checkpoint_available ? "success" : "neutral"} />
                    {engine.engine}
                  </span>
                  <Button size="xs" variant="outline" disabled={busy || !engine.checkpoint_available}
                    onClick={() => void checkpoint(`${engine.engine === "keyspace" ? "keyspace" : engine.engine === "queue" ? "queue" : "stream"}-checkpoint` as "keyspace-checkpoint")}>
                    {engine.checkpoint_available ? "Checkpoint" : "Unavailable"}
                  </Button>
                </li>
              ))}
            </ul>
          )}
          <div>
            <Button size="sm" disabled={busy} onClick={() => void checkpoint("checkpoint-all")}><HardDriveDownload className="size-4" />Checkpoint all engines</Button>
          </div>
        </div>
      </Section>
      <div className="mt-6 border-t border-rule-strong pt-3">
        <h2 className="mb-3 text-base font-semibold tracking-[-0.015em]">Jobs</h2>
        {jobs.length === 0
          ? <EmptyState title="No jobs yet." hint="Checkpoint jobs you enqueue appear here with their live state." />
          : (
            <Table>
              <TableHeader>
                <TableRow><TableHead>Job</TableHead><TableHead>Kind</TableHead><TableHead>State</TableHead><TableHead>Created</TableHead><TableHead className="text-right">Actions</TableHead></TableRow>
              </TableHeader>
              <TableBody>
                {jobs.map((job) => (
                  <TableRow key={job.job_id}>
                    <TableCell><CopyId id={job.job_id} /></TableCell>
                    <TableCell className="font-mono text-xs">{job.kind}</TableCell>
                    <TableCell><StateBadge state={job.state} /></TableCell>
                    <TableCell className="text-xs text-muted-foreground">{formatTimestamp(job.created_at)}</TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-1">
                        <Button size="sm" variant="ghost" onClick={() => setDetailJob(job)}>Inspect</Button>
                        {job.state === "queued" && (
                          <Button size="sm" variant="ghost" className="text-destructive" onClick={() => setCancelTarget(job)}><Ban className="size-3.5" />Cancel</Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
      </div>

      <Dialog open={detailJob !== null} onOpenChange={(open) => { if (!open) setDetailJob(null); }}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader><DialogTitle className="break-all">Job {detailJob?.job_id}</DialogTitle></DialogHeader>
          {detailJob && (
            <div className="grid gap-3 py-2">
              <DetailGrid rows={[
                { label: "Kind", value: detailJob.kind, mono: true },
                { label: "State", value: <StateBadge state={detailJob.state} /> },
                { label: "Created", value: formatTimestamp(detailJob.created_at) },
                { label: "Completed", value: detailJob.completed_at ? formatTimestamp(detailJob.completed_at) : "—" }
              ]} />
              {detailJob.error && <ErrorBanner error={new Error(detailJob.error.message ?? "Job failed")} />}
              {detailJob.result && <pre className="max-h-48 overflow-auto bg-muted p-2 font-mono text-xs">{JSON.stringify(detailJob.result, null, 2)}</pre>}
            </div>
          )}
          <DialogFooter><Button variant="outline" onClick={() => setDetailJob(null)}>Close</Button></DialogFooter>
        </DialogContent>
      </Dialog>
      {cancelTarget && (
        <ConfirmDestructive
          open onOpenChange={() => { setCancelTarget(null); setError(null); }} confirmId={cancelTarget.job_id} inFlight={busy}
          title="Cancel queued job" description={`Cancels ${cancelTarget.kind} while it is still queued.`}
          context={<ConnectionContextLine profileId={profileId} />}
          error={error}
          confirmLabel="Cancel job"
          onConfirm={() => void cancel(cancelTarget)}
        />
      )}
    </div>
  );
}
