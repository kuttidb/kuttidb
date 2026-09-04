import { useCallback, useState } from "react";
import { HardDriveDownload, Ban } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ConfirmDestructive } from "@/components/confirm";
import { ErrorBanner } from "@/components/error-banner";
import { CopyId, DetailGrid, LastRefreshed, PageHeader, StateBadge } from "@/components/shared";
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
  const { lastUpdated, refresh } = usePolling(loader, 5_000);

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

  return (
    <div>
      <PageHeader
        title="Maintenance & jobs"
        description="Checkpoints run as bounded asynchronous jobs. Only queued jobs are cancellable."
        actions={<LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} polling />}
      />
      {error && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      <Card className="mb-4">
        <CardHeader className="pb-2"><CardTitle className="text-sm flex items-center gap-2"><HardDriveDownload className="size-4 text-muted-foreground" />Checkpoints</CardTitle></CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {engines.map((engine) => (
              <Button key={engine.engine} size="sm" variant="outline" disabled={busy || !engine.checkpoint_available}
                onClick={() => void checkpoint(`${engine.engine === "keyspace" ? "keyspace" : engine.engine === "queue" ? "queue" : "stream"}-checkpoint` as "keyspace-checkpoint")}>
                {engine.engine} {engine.checkpoint_available ? "" : "(unavailable)"}
              </Button>
            ))}
            <Button size="sm" disabled={busy} onClick={() => void checkpoint("checkpoint-all")}>All engines</Button>
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-sm">Jobs</CardTitle></CardHeader>
        <CardContent className="p-0">
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
                  <TableCell className="text-muted-foreground text-xs">{formatTimestamp(job.created_at)}</TableCell>
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
              {jobs.length === 0 && <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground py-8">No jobs yet.</TableCell></TableRow>}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Dialog open={detailJob !== null} onOpenChange={(open) => { if (!open) setDetailJob(null); }}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader><DialogTitle>Job {detailJob?.job_id}</DialogTitle></DialogHeader>
          {detailJob && (
            <div className="grid gap-3 py-2">
              <DetailGrid rows={[
                { label: "Kind", value: detailJob.kind, mono: true },
                { label: "State", value: <StateBadge state={detailJob.state} /> },
                { label: "Created", value: formatTimestamp(detailJob.created_at) },
                { label: "Completed", value: formatTimestamp(detailJob.completed_at ?? null) }
              ]} />
              {detailJob.error && <ErrorBanner error={new Error(detailJob.error.message ?? "Job failed")} />}
              {detailJob.result && <pre className="font-mono text-xs bg-muted rounded-md p-2 max-h-48 overflow-auto">{JSON.stringify(detailJob.result, null, 2)}</pre>}
            </div>
          )}
          <DialogFooter><Button variant="outline" onClick={() => setDetailJob(null)}>Close</Button></DialogFooter>
        </DialogContent>
      </Dialog>
      {cancelTarget && (
        <ConfirmDestructive
          open onOpenChange={() => setCancelTarget(null)} confirmId={cancelTarget.job_id} inFlight={busy}
          title="Cancel queued job" description={`Cancels ${cancelTarget.kind} while it is still queued.`}
          onConfirm={() => void cancel(cancelTarget)}
        />
      )}
    </div>
  );
}
