import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { Plus, Trash2, Send, Scissors, Activity, Pencil } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { BinaryValue, encodeDraft, type BinaryField } from "@/components/binary-value";
import { ConfirmDestructive } from "@/components/confirm";
import { ErrorBanner } from "@/components/error-banner";
import { CopyId, ConnectionContextLine, EmptyState, LastRefreshed, PageHeader, Section, Skeleton, StatusDot } from "@/components/shared";
import { admin, list, newIdempotencyKey } from "@/lib/api";
import { nameFromId } from "@/lib/codec";
import { formatBytes, formatDuration } from "@/lib/format";
import type { StreamDetail, StreamPartition, StreamRecord, StreamSummary, JobEntry } from "@/lib/types";

export function StreamsView({ profileId, onOpenStream }: { profileId: string; onOpenStream: (streamId: string) => void }) {
  const [streams, setStreams] = useState<StreamSummary[]>([]);
  const [declareOpen, setDeclareOpen] = useState(false);
  const { lastUpdated, error, stale, refresh } = usePolling(
    useCallback(async () => setStreams((await list<StreamSummary>(profileId, "streams")).data), [profileId]),
    15_000
  );
  const loadedOnce = lastUpdated !== null || error !== null;
  return (
    <div>
      <PageHeader
        title="Streams"
        description="Durable partitioned append logs with replay and retention."
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <Button size="sm" onClick={() => setDeclareOpen(true)}><Plus className="size-4" />Declare stream</Button>
          </>
        }
      />
      {error && streams.length === 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {error && stale && streams.length > 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {!loadedOnce ? (
        <div className="grid gap-2" aria-busy="true">
          {Array.from({ length: 4 }, (_, index) => <Skeleton key={index} className="h-11 w-full" />)}
          <p className="text-sm text-muted-foreground">Checking streams…</p>
        </div>
      ) : streams.length === 0 ? (
        <div className="border-t border-rule-strong pt-2">
          <EmptyState
            title="No streams declared."
            hint="Declare a durable stream to append partitioned records."
            action={<Button size="sm" onClick={() => setDeclareOpen(true)}><Plus className="size-4" />Declare stream</Button>}
          />
        </div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead><TableHead>Stream ID</TableHead>
              <TableHead className="text-right">Partitions</TableHead><TableHead className="text-right">Records</TableHead><TableHead className="text-right">Retained</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {streams.map((stream) => (
              <TableRow key={stream.id} className="cursor-pointer" onClick={() => onOpenStream(stream.id)}>
                <TableCell>
                  <a
                    href={`#/c/${encodeURIComponent(profileId)}/streams/${encodeURIComponent(stream.id)}`}
                    className="text-left font-medium hover:underline"
                    onClick={(event) => event.stopPropagation()}
                  >
                    {stream.name}
                  </a>
                </TableCell>
                <TableCell><CopyId id={stream.id} /></TableCell>
                <TableCell className="text-right tabular-nums">{stream.partition_count}</TableCell>
                <TableCell className="text-right tabular-nums">{stream.retained_record_count}</TableCell>
                <TableCell className="text-right tabular-nums">{formatBytes(stream.retained_bytes)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
      <DeclareStreamDialog open={declareOpen} onOpenChange={setDeclareOpen} profileId={profileId} onDone={refresh} />
    </div>
  );
}

function DeclareStreamDialog({ open, onOpenChange, profileId, onDone }: { open: boolean; onOpenChange: (open: boolean) => void; profileId: string; onDone: () => void }) {
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState("1");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const submit = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, "streams", { method: "POST", idempotencyKey: newIdempotencyKey(), body: { name: name.trim(), partitions: Math.max(1, Number(partitions) || 1) } });
      toast.success(`Stream ${name.trim()} declared`);
      onOpenChange(false); setName(""); setPartitions("1");
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader><DialogTitle>Declare stream</DialogTitle><DialogDescription>Ordering is guaranteed within a partition, not across partitions.</DialogDescription></DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="grid gap-1.5"><Label htmlFor="stream-name">Name</Label><Input id="stream-name" value={name} onChange={(event) => setName(event.target.value)} spellCheck={false} /></div>
          <div className="grid gap-1.5"><Label htmlFor="stream-partitions">Partitions</Label><Input id="stream-partitions" inputMode="numeric" value={partitions} onChange={(event) => setPartitions(event.target.value)} /></div>
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={() => void submit()} disabled={busy || name.trim().length === 0}>{busy ? "Declaring…" : "Declare"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export function StreamDetailView({ profileId, streamId, onBack }: { profileId: string; streamId: string; onBack: () => void }) {
  const [detail, setDetail] = useState<StreamDetail | null>(null);
  const [partitions, setPartitions] = useState<StreamPartition[]>([]);
  const [etag, setEtag] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [retentionOpen, setRetentionOpen] = useState(false);
  const [truncatePartition, setTruncatePartition] = useState<number | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);
  /** Revision captured when the confirmation opens, not the polling snapshot. */
  const [confirmEtag, setConfirmEtag] = useState<string | null>(null);
  const [dialogError, setDialogError] = useState<Error | null>(null);
  const [busy, setBusy] = useState(false);

  const loader = useCallback(async () => {
    const [detailResponse, partitionsResponse] = await Promise.all([
      admin<{ data: StreamDetail }>(profileId, `streams/${streamId}`),
      list<StreamPartition>(profileId, `streams/${streamId}/partitions`)
    ]);
    setDetail(detailResponse.json.data);
    setEtag(detailResponse.etag);
    setPartitions(partitionsResponse.data);
  }, [profileId, streamId]);
  const { lastUpdated, error: pollError, stale, refresh } = usePolling(loader, 15_000);

  // Confirmation-time revision for the destructive dialog (delete and
  // truncate): fetch the current ETag when the dialog opens.
  const destructiveTarget = deleteOpen ? "delete" : truncatePartition !== null ? "truncate" : null;
  useEffect(() => {
    if (!destructiveTarget) return;
    let cancelled = false;
    setConfirmEtag(null);
    admin<{ data: StreamDetail }>(profileId, `streams/${streamId}`)
      .then((response) => { if (!cancelled) setConfirmEtag(response.etag); })
      .catch(() => { /* the mutation will surface the real precondition error */ });
    return () => { cancelled = true; };
  }, [destructiveTarget, profileId, streamId]);

  const decodedName = detail ? nameFromId(detail.id) : null;

  const deleteStream = async () => {
    setBusy(true); setDialogError(null);
    try {
      await admin<JobEntry>(profileId, `streams/${streamId}`, { method: "DELETE", idempotencyKey: newIdempotencyKey(), ifMatch: confirmEtag ?? etag ?? undefined, confirm: streamId, body: {} });
      toast.success("Deletion job queued — track it under Maintenance");
      setDeleteOpen(false);
      onBack();
    } catch (reason) { setDialogError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const truncate = async (partition: number, baseOffset: number) => {
    setBusy(true); setDialogError(null);
    try {
      await admin(profileId, `streams/${streamId}/partitions/${partition}:truncate`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), ifMatch: confirmEtag ?? etag ?? undefined, confirm: streamId,
        body: { base_offset: baseOffset }
      });
      toast.success("Truncation job queued — track it under Maintenance");
      setTruncatePartition(null);
      refresh();
    } catch (reason) { setDialogError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const viewError = error ?? pollError;
  const connectionContext = (
    <>
      <ConnectionContextLine profileId={profileId} />
      {decodedName && <span>Stream: <span className="font-medium text-foreground">{decodedName}</span></span>}
    </>
  );

  return (
    <div>
      <PageHeader
        title={decodedName ?? streamId}
        breadcrumb={
          <button type="button" className="text-sm text-muted-foreground hover:text-foreground hover:underline" onClick={onBack}>
            Streams
          </button>
        }
        description={<CopyId id={streamId} label={`Copy stream ID ${streamId}`} />}
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <Button variant="outline" size="sm" onClick={() => setRetentionOpen(true)}><Pencil className="size-4" />Retention</Button>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" aria-label="Stream actions" aria-haspopup="menu"><Activity className="size-4" />Actions</Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                {partitions.map((partition) => (
                  <DropdownMenuItem key={partition.partition} onClick={() => setTruncatePartition(partition.partition)}>
                    <Scissors className="size-4" />Truncate partition {partition.partition}…
                  </DropdownMenuItem>
                ))}
                <DropdownMenuSeparator />
                <DropdownMenuItem variant="destructive" onClick={() => setDeleteOpen(true)}><Trash2 className="size-4" />Delete stream…</DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </>
        }
      />
      {(viewError || error) && <ErrorBanner error={viewError ?? error} onRetry={refresh} className="mb-4" />}
      {detail && (
        <>
          <div className="mb-4 border-y border-rule-strong">
            <dl className="flex flex-wrap gap-x-8 gap-y-1 px-0 py-3 text-sm">
              <div className="flex gap-2"><dt className="text-muted-foreground">Partitions</dt><dd className="font-medium tabular-nums">{partitions.length}</dd></div>
              <div className="flex gap-2"><dt className="text-muted-foreground">Retention</dt><dd className="font-medium">{formatBytes(detail.max_retained_bytes)} / {formatDuration(detail.max_retained_age_ms)} max</dd></div>
              <div className="flex gap-2"><dt className="text-muted-foreground">Revision</dt><dd className="font-mono text-xs">{etag ?? `s-${detail.revision}`}</dd></div>
            </dl>
          </div>
          <Tabs defaultValue="records">
            <TabsList className="mb-4">
              <TabsTrigger value="records">Records</TabsTrigger>
              <TabsTrigger value="append">Append</TabsTrigger>
              <TabsTrigger value="tail">Tail</TabsTrigger>
              <TabsTrigger value="partitions">Partitions</TabsTrigger>
            </TabsList>
            <TabsContent value="records"><RecordsTab profileId={profileId} streamId={streamId} partitions={partitions} /></TabsContent>
            <TabsContent value="append"><AppendTab profileId={profileId} streamId={streamId} partitions={partitions} onAppended={refresh} /></TabsContent>
            <TabsContent value="tail"><TailTab profileId={profileId} streamId={streamId} partitions={partitions} /></TabsContent>
            <TabsContent value="partitions">
              <div className="border-t border-rule-strong pt-3">
                <Table>
                  <TableHeader><TableRow><TableHead>Partition</TableHead><TableHead className="text-right">Base offset</TableHead><TableHead className="text-right">Next offset</TableHead><TableHead className="text-right">Retained</TableHead></TableRow></TableHeader>
                  <TableBody>
                    {partitions.map((partition) => (
                      <TableRow key={partition.partition}>
                        <TableCell className="font-mono">{partition.partition}</TableCell>
                        <TableCell className="text-right tabular-nums">{partition.base_offset}</TableCell>
                        <TableCell className="text-right tabular-nums">{partition.next_offset}</TableCell>
                        <TableCell className="text-right tabular-nums">{formatBytes(partition.retained_bytes)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </TabsContent>
          </Tabs>
        </>
      )}
      {detail && (
        <RetentionDialog
          open={retentionOpen} onOpenChange={setRetentionOpen} profileId={profileId} streamId={streamId}
          detail={detail} etag={etag} onDone={refresh}
        />
      )}
      <ConfirmDestructive
        open={deleteOpen} onOpenChange={setDeleteOpen} confirmId={streamId} inFlight={busy}
        title="Delete stream"
        confirmLabel="Delete stream"
        description="Queues a conditional durable deletion job that removes the stream, all retained records, and its consumer-group offsets."
        context={connectionContext}
        error={dialogError}
        onConfirm={() => void deleteStream()}
      />
      {truncatePartition !== null && detail && (
        <TruncateDialog
          open onOpenChange={() => { setTruncatePartition(null); setDialogError(null); }} partition={truncatePartition}
          baseOffset={partitions.find((entry) => entry.partition === truncatePartition)?.base_offset ?? 0}
          nextOffset={partitions.find((entry) => entry.partition === truncatePartition)?.next_offset ?? 0}
          inFlight={busy}
          context={connectionContext}
          error={dialogError}
          onConfirm={(baseOffset) => void truncate(truncatePartition, baseOffset)}
        />
      )}
    </div>
  );
}

function TruncateDialog({ open, onOpenChange, partition, baseOffset, nextOffset, inFlight, context, error, onConfirm }: {
  open: boolean; onOpenChange: (open: boolean) => void; partition: number; baseOffset: number; nextOffset: number; inFlight: boolean;
  context?: ReactNode; error?: unknown; onConfirm: (baseOffset: number) => void;
}) {
  const [value, setValue] = useState(String(nextOffset));
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader><DialogTitle>Truncate partition {partition}</DialogTitle>
          <DialogDescription>Advances the retained base offset. Retained range: {baseOffset} → {nextOffset}. Runs as a bounded asynchronous job.</DialogDescription></DialogHeader>
        <div className="grid gap-1.5 py-2">
          <Label htmlFor="truncate-offset">New base offset</Label>
          <Input id="truncate-offset" inputMode="numeric" value={value} onChange={(event) => setValue(event.target.value)} className="font-mono" />
          {context && <p className="text-xs text-muted-foreground">{context}</p>}
          {error ? <ErrorBanner error={error} /> : null}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button variant="destructive" disabled={inFlight} onClick={() => onConfirm(Math.max(0, Number(value) || 0))}>{inFlight ? "Queueing…" : "Queue truncation"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function RetentionDialog({ open, onOpenChange, profileId, streamId, detail, etag, onDone }: {
  open: boolean; onOpenChange: (open: boolean) => void; profileId: string; streamId: string; detail: StreamDetail; etag: string | null; onDone: () => void;
}) {
  const [bytes, setBytes] = useState(String(detail.max_retained_bytes));
  const [ageMs, setAgeMs] = useState(String(detail.max_retained_age_ms));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const submit = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `streams/${streamId}`, {
        method: "PATCH", idempotencyKey: newIdempotencyKey(), ifMatch: etag ?? undefined,
        body: { max_retained_bytes: Math.max(0, Number(bytes) || 0), max_retained_age_ms: Math.max(0, Number(ageMs) || 0) }
      });
      toast.success("Retention ceilings updated; out-of-policy records trimmed");
      onOpenChange(false);
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader><DialogTitle>Retention ceilings</DialogTitle>
          <DialogDescription>Both values are required and replace the current ceilings atomically (ETag {etag ?? "unknown"}).</DialogDescription></DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="grid gap-1.5"><Label htmlFor="retention-bytes">Max retained bytes (0 = unlimited)</Label>
            <Input id="retention-bytes" inputMode="numeric" value={bytes} onChange={(event) => setBytes(event.target.value)} className="font-mono" /></div>
          <div className="grid gap-1.5"><Label htmlFor="retention-age">Max retained age ms (0 = unlimited)</Label>
            <Input id="retention-age" inputMode="numeric" value={ageMs} onChange={(event) => setAgeMs(event.target.value)} className="font-mono" /></div>
          <p className="text-xs text-muted-foreground">Before: {formatBytes(detail.max_retained_bytes)} / {formatDuration(detail.max_retained_age_ms)}</p>
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={() => void submit()} disabled={busy}>{busy ? "Updating…" : "Update durably"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function RecordsTab({ profileId, streamId, partitions }: { profileId: string; streamId: string; partitions: StreamPartition[] }) {
  const [partition, setPartition] = useState("0");
  const [offset, setOffset] = useState("0");
  const [records, setRecords] = useState<StreamRecord[]>([]);
  const [error, setError] = useState<Error | null>(null);
  const [busy, setBusy] = useState(false);
  const nextOffsetOf = partitions.find((entry) => entry.partition === Number(partition))?.next_offset ?? 0;

  const load = async (from: number) => {
    setBusy(true); setError(null);
    try {
      const response = await list<StreamRecord>(profileId, `streams/${streamId}/partitions/${partition}/records?offset=${from}&max_records=50&max_bytes=131072`);
      setRecords(response.data);
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  useEffect(() => { void load(Number(offset) || 0); /* eslint-disable-line react-hooks/exhaustive-deps */ }, [profileId, streamId, partition]);

  return (
    <div className="grid gap-3">
      <div className="flex flex-wrap items-end gap-2">
        <div className="grid gap-1.5">
          <Label htmlFor="records-partition">Partition</Label>
          <Select value={partition} onValueChange={(value) => { setPartition(value); setOffset("0"); }}>
            <SelectTrigger id="records-partition" className="w-40"><SelectValue /></SelectTrigger>
            <SelectContent>
              {partitions.map((entry) => <SelectItem key={entry.partition} value={String(entry.partition)}>Partition {entry.partition}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="records-offset">Offset</Label>
          <Input id="records-offset" value={offset} onChange={(event) => setOffset(event.target.value)} inputMode="numeric" className="w-32 font-mono" />
        </div>
        <Button size="sm" variant="outline" onClick={() => void load(Number(offset) || 0)} disabled={busy}>Fetch from offset</Button>
        <Button size="sm" variant="ghost" onClick={() => { setOffset(String(nextOffsetOf)); void load(nextOffsetOf); }} disabled={busy || nextOffsetOf === 0}>Latest</Button>
      </div>
      {error && <ErrorBanner error={error} onRetry={() => void load(Number(offset) || 0)} />}
      <div className="grid gap-3">
        {busy && records.length === 0 && <div aria-busy="true" className="grid gap-2">{Array.from({ length: 3 }, (_, index) => <Skeleton key={index} className="h-20 w-full" />)}</div>}
        {records.map((record) => (
          <div key={`${record.partition}:${record.offset}`} className="border bg-card p-3">
            <p className="mb-2 flex items-center gap-2 font-mono text-xs text-muted-foreground">
              partition {record.partition} · offset {record.offset}
              {record.key && <Badge variant="neutral" className="text-xs">keyed</Badge>}
            </p>
            {record.key && (
              <div className="mb-2">
                <p className="mb-1 text-xs text-muted-foreground">Key</p>
                <BinaryValue value={record.key as BinaryField} compact />
              </div>
            )}
            <div>
              <p className="mb-1 text-xs text-muted-foreground">Body</p>
              <BinaryValue value={record.body as BinaryField} />
            </div>
          </div>
        ))}
        {records.length === 0 && !busy && <p className="text-sm text-muted-foreground">No records in this range. Partition and offset are independent — an offset belongs to one partition.</p>}
      </div>
    </div>
  );
}

function AppendTab({ profileId, streamId, partitions, onAppended }: { profileId: string; streamId: string; partitions: StreamPartition[]; onAppended: () => void }) {
  const [mode, setMode] = useState<"text" | "json" | "base64">("text");
  const [raw, setRaw] = useState("");
  const [keyText, setKeyText] = useState("");
  const [partition, setPartition] = useState("auto");
  const [batchLines, setBatchLines] = useState("");
  const [batch, setBatch] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const draft = encodeDraft(mode, raw);

  const appendOne = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `streams/${streamId}/records`, {
        method: "POST", idempotencyKey: newIdempotencyKey(),
        body: {
          body: draft.base64,
          ...(keyText.trim() ? { key: encodeDraft("text", keyText.trim()).base64 } : {}),
          ...(partition !== "auto" ? { partition: Number(partition) } : {})
        }
      });
      toast.success("Record appended");
      setRaw("");
      onAppended();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const appendBatch = async () => {
    const lines = batchLines.split("\n").map((line) => line.trim()).filter(Boolean);
    if (lines.length === 0) return;
    setBusy(true); setError(null);
    try {
      await admin(profileId, `streams/${streamId}/records:batch`, {
        method: "POST", idempotencyKey: newIdempotencyKey(),
        body: {
          records: lines.map((line) => ({ body: encodeDraft(mode, line).base64 })),
          ...(partition !== "auto" ? { partition: Number(partition) } : {})
        }
      });
      toast.success(`${lines.length} records appended as one durable batch`);
      setBatchLines("");
      onAppended();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  return (
    <div className="border-t border-rule-strong pt-3">
      <Section
        title="Append records"
        actions={
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            <Switch checked={batch} onCheckedChange={setBatch} aria-label="Batch mode" /> batch mode
          </label>
        }
      >
        <div className="grid gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <Tabs value={mode} onValueChange={(value) => setMode(value as typeof mode)}>
              <TabsList aria-label="Encoding"><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger><TabsTrigger value="base64">Base64</TabsTrigger></TabsList>
            </Tabs>
            <div className="grid gap-1.5">
              <Label htmlFor="append-partition">Partition</Label>
              <Select value={partition} onValueChange={setPartition}>
                <SelectTrigger id="append-partition" className="w-44"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="auto">auto (keyed)</SelectItem>
                  {partitions.map((entry) => <SelectItem key={entry.partition} value={String(entry.partition)}>Partition {entry.partition}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
            {!batch && (
              <div className="grid flex-1 gap-1.5">
                <Label htmlFor="append-key">Partition key (optional)</Label>
                <Input id="append-key" value={keyText} onChange={(event) => setKeyText(event.target.value)} spellCheck={false} className="max-w-xs" />
              </div>
            )}
          </div>
          {!batch ? (
            <>
              <div className="grid gap-1.5">
                <Label htmlFor="append-body">Record body</Label>
                <Textarea id="append-body" value={raw} onChange={(event) => setRaw(event.target.value)} rows={4} spellCheck={false} className="max-w-[640px] font-mono text-xs" />
              </div>
              <div className="flex flex-wrap items-center gap-3">
                <span className="text-xs text-muted-foreground">{draft.bytes} bytes</span>
                {draft.error && <span className="text-xs text-destructive">{draft.error}</span>}
                <Button className="ml-auto" onClick={() => void appendOne()} disabled={busy || !draft.base64 || Boolean(draft.error)}><Send className="size-4" />{busy ? "Appending…" : "Append"}</Button>
              </div>
            </>
          ) : (
            <>
              <p className="text-xs text-muted-foreground">One record body per line (up to 100), written as one durable WAL batch.</p>
              <div className="grid gap-1.5">
                <Label htmlFor="append-batch">Record bodies, one per line</Label>
                <Textarea id="append-batch" value={batchLines} onChange={(event) => setBatchLines(event.target.value)} rows={6} spellCheck={false} className="max-w-[640px] font-mono text-xs" />
              </div>
              <Button className="justify-self-end" onClick={() => void appendBatch()} disabled={busy || batchLines.trim().length === 0}><Send className="size-4" />{busy ? "Appending…" : "Append batch"}</Button>
            </>
          )}
          {error && <ErrorBanner error={error} />}
        </div>
      </Section>
    </div>
  );
}

type TailEvent = { partition: number; offset: number; body?: BinaryField; key?: BinaryField; kind: "record" | "offset_out_of_range" };

/** Bounded tail snapshot: the gateway buffers SSE for safety, so results arrive as one bounded page. */
function TailTab({ profileId, streamId, partitions }: { profileId: string; streamId: string; partitions: StreamPartition[] }) {
  const [partition, setPartition] = useState("0");
  const [offset, setOffset] = useState(String(partitions[0]?.next_offset ?? 0));
  const [events, setEvents] = useState<TailEvent[]>([]);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => () => abortRef.current?.abort(), []);

  const stop = () => { abortRef.current?.abort(); abortRef.current = null; setRunning(false); };

  const start = async () => {
    setError(null); setEvents([]); setRunning(true);
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const response = await admin<string>(profileId, `streams/${streamId}/partitions/${partition}/records:tail?offset=${Number(offset) || 0}`, {
        accept: "text/event-stream", signal: controller.signal
      });
      const text = typeof response.json === "string" ? response.json : JSON.stringify(response.json);
      const parsed: TailEvent[] = [];
      for (const block of text.split("\n\n")) {
        const dataLine = block.split("\n").find((line) => line.startsWith("data:"));
        if (!dataLine) continue;
        try {
          const payload = JSON.parse(dataLine.slice(5).trim()) as Record<string, unknown>;
          if (payload.event === "offset_out_of_range" || payload.reason === "offset_out_of_range") {
            parsed.push({ partition: Number(partition), offset: Number(offset) || 0, kind: "offset_out_of_range" });
            continue;
          }
          const body = payload.body as BinaryField | undefined;
          const key = payload.key as BinaryField | undefined;
          if (body) parsed.push({ partition: payload.partition as number, offset: payload.offset as number, body, key, kind: "record" });
        } catch { /* ignore malformed keepalive blocks */ }
      }
      setEvents(parsed);
      toast.info(`${parsed.filter((event) => event.kind === "record").length} tail events captured`);
    } catch (reason) {
      if (!controller.signal.aborted) setError(reason instanceof Error ? reason : new Error(String(reason)));
    } finally {
      setRunning(false);
      abortRef.current = null;
    }
  };

  return (
    <div className="border-t border-rule-strong pt-3">
      <Section title="Live tail">
        <div className="grid gap-3">
          <p className="max-w-3xl text-xs text-muted-foreground">
            Bounded SSE worker through the console gateway. The server ends each worker after a short lifetime and throttles to the advertised event rate; the gateway buffers for safety, so events arrive as one bounded page. Tailing stops on navigation or disconnect.
          </p>
          <div className="flex flex-wrap items-end gap-2">
            <div className="grid gap-1.5"><Label htmlFor="tail-partition">Partition</Label>
              <Select value={partition} onValueChange={setPartition}>
                <SelectTrigger id="tail-partition" className="w-36"><SelectValue /></SelectTrigger>
                <SelectContent>{partitions.map((entry) => <SelectItem key={entry.partition} value={String(entry.partition)}>Partition {entry.partition}</SelectItem>)}</SelectContent>
              </Select>
            </div>
            <div className="grid gap-1.5"><Label htmlFor="tail-offset">Offset</Label>
              <Input id="tail-offset" value={offset} onChange={(event) => setOffset(event.target.value)} inputMode="numeric" className="w-28 font-mono" /></div>
            {running
              ? <Button variant="outline" onClick={stop} aria-live="polite">Stop tail</Button>
              : <Button onClick={() => void start()} aria-live="polite"><Activity className="size-4" />Start tail</Button>}
            {running && <span className="inline-flex items-center gap-1.5 text-xs text-info"><StatusDot tone="info" />Tail running</span>}
          </div>
          {error && <ErrorBanner error={error} />}
          <div className="grid gap-2">
            {events.map((event) => event.kind === "offset_out_of_range"
              ? <div key={`${event.partition}:${event.offset}`} className="border border-warning/40 bg-warning-surface px-3 py-2 text-sm text-warning">offset_out_of_range — retention advanced past offset {event.offset}.</div>
              : (
                <div key={`${event.partition}:${event.offset}`} className="border p-2">
                  <p className="mb-1 font-mono text-xs text-muted-foreground">partition {event.partition} · offset {event.offset}</p>
                  <BinaryValue value={event.body} compact />
                </div>
              ))}
            {events.length === 0 && running && <p className="text-sm text-muted-foreground">Waiting for tail events…</p>}
          </div>
        </div>
      </Section>
    </div>
  );
}

export function useStreamsList(profileId: string) {
  const [streams, setStreams] = useState<StreamSummary[]>([]);
  const load = useCallback(async () => setStreams((await list<StreamSummary>(profileId, "streams")).data), [profileId]);
  useEffect(() => { void load(); }, [load]);
  return streams;
}
