import { useCallback, useEffect, useRef, useState } from "react";
import { Plus, Trash2, Send, Scissors, Activity, Pencil } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
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
import { CopyId, CursorPager, DetailGrid, LastRefreshed, PageHeader, StateBadge } from "@/components/shared";
import { admin, ApiError, list, newIdempotencyKey } from "@/lib/api";
import { idFromName, nameFromId } from "@/lib/codec";
import { formatBytes, formatDuration } from "@/lib/format";
import type { StreamDetail, StreamPartition, StreamRecord, StreamSummary, JobEntry } from "@/lib/types";

export function StreamsView({ profileId, onOpenStream }: { profileId: string; onOpenStream: (streamId: string) => void }) {
  const [streams, setStreams] = useState<StreamSummary[]>([]);
  const [declareOpen, setDeclareOpen] = useState(false);
  const { lastUpdated, error, refresh } = usePolling(
    useCallback(async () => setStreams((await list<StreamSummary>(profileId, "streams")).data), [profileId]),
    15_000
  );
  return (
    <div>
      <PageHeader
        title="Streams"
        description="Durable partitioned append logs with replay and retention."
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} />
            <Button size="sm" onClick={() => setDeclareOpen(true)}><Plus className="size-4 mr-1" />Declare stream</Button>
          </>
        }
      />
      {error && streams.length === 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      <Card>
        <CardContent className="p-0">
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
                  <TableCell className="font-medium">{stream.name}</TableCell>
                  <TableCell><CopyId id={stream.id} /></TableCell>
                  <TableCell className="text-right tabular-nums">{stream.partition_count}</TableCell>
                  <TableCell className="text-right tabular-nums">{stream.retained_record_count}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatBytes(stream.retained_bytes)}</TableCell>
                </TableRow>
              ))}
              {streams.length === 0 && <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground py-8">No streams declared.</TableCell></TableRow>}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
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
          <div className="grid gap-2"><Label htmlFor="stream-name">Name</Label><Input id="stream-name" value={name} onChange={(event) => setName(event.target.value)} spellCheck={false} /></div>
          <div className="grid gap-2"><Label htmlFor="stream-partitions">Partitions</Label><Input id="stream-partitions" inputMode="numeric" value={partitions} onChange={(event) => setPartitions(event.target.value)} /></div>
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
  const { lastUpdated, refresh } = usePolling(loader, 15_000);

  const decodedName = detail ? nameFromId(detail.id) : null;

  const deleteStream = async () => {
    setBusy(true); setError(null);
    try {
      await admin<JobEntry>(profileId, `streams/${streamId}`, { method: "DELETE", idempotencyKey: newIdempotencyKey(), ifMatch: etag ?? undefined, confirm: streamId, body: {} });
      toast.success("Deletion job queued — track it under Maintenance");
      setDeleteOpen(false);
      onBack();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const truncate = async (partition: number, baseOffset: number) => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `streams/${streamId}/partitions/${partition}:truncate`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), ifMatch: etag ?? undefined, confirm: streamId,
        body: { base_offset: baseOffset }
      });
      toast.success("Truncation job queued — track it under Maintenance");
      setTruncatePartition(null);
      refresh();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  return (
    <div>
      <PageHeader
        title={decodedName ?? streamId}
        description={<span className="font-mono text-xs">{streamId}</span>}
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} />
            <Button variant="outline" size="sm" onClick={() => setRetentionOpen(true)}><Pencil className="size-4 mr-1" />Retention</Button>
            <DropdownMenu>
              <DropdownMenuTrigger asChild><Button variant="outline" size="icon"><Activity className="size-4" /></Button></DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                {partitions.map((partition) => (
                  <DropdownMenuItem key={partition.partition} onClick={() => setTruncatePartition(partition.partition)}>
                    <Scissors className="size-4 mr-2" />Truncate partition {partition.partition}…
                  </DropdownMenuItem>
                ))}
                <DropdownMenuItem className="text-destructive" onClick={() => setDeleteOpen(true)}><Trash2 className="size-4 mr-2" />Delete stream…</DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </>
        }
      />
      {error && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {detail && (
        <Tabs defaultValue="records">
          <TabsList className="mb-4">
            <TabsTrigger value="records">Records</TabsTrigger>
            <TabsTrigger value="append"><Send className="size-3.5 mr-1" />Append</TabsTrigger>
            <TabsTrigger value="tail"><Activity className="size-3.5 mr-1" />Tail</TabsTrigger>
            <TabsTrigger value="partitions">Partitions</TabsTrigger>
          </TabsList>
          <TabsContent value="records"><RecordsTab profileId={profileId} streamId={streamId} partitions={partitions} /></TabsContent>
          <TabsContent value="append"><AppendTab profileId={profileId} streamId={streamId} partitions={partitions} onAppended={refresh} /></TabsContent>
          <TabsContent value="tail"><TailTab profileId={profileId} streamId={streamId} partitions={partitions} /></TabsContent>
          <TabsContent value="partitions">
            <Card>
              <CardContent className="p-0">
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
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
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
        description="Queues a conditional durable deletion job that removes the stream, all retained records, and its consumer-group offsets."
        onConfirm={() => void deleteStream()}
      />
      {truncatePartition !== null && detail && (
        <TruncateDialog
          open onOpenChange={() => setTruncatePartition(null)} partition={truncatePartition}
          baseOffset={partitions.find((entry) => entry.partition === truncatePartition)?.base_offset ?? 0}
          nextOffset={partitions.find((entry) => entry.partition === truncatePartition)?.next_offset ?? 0}
          inFlight={busy}
          onConfirm={(baseOffset) => void truncate(truncatePartition, baseOffset)}
        />
      )}
    </div>
  );
}

function TruncateDialog({ open, onOpenChange, partition, baseOffset, nextOffset, inFlight, onConfirm }: {
  open: boolean; onOpenChange: (open: boolean) => void; partition: number; baseOffset: number; nextOffset: number; inFlight: boolean; onConfirm: (baseOffset: number) => void;
}) {
  const [value, setValue] = useState(String(nextOffset));
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader><DialogTitle>Truncate partition {partition}</DialogTitle>
          <DialogDescription>Advances the retained base offset. Retained range: {baseOffset} → {nextOffset}. Runs as a bounded asynchronous job.</DialogDescription></DialogHeader>
        <div className="grid gap-2 py-2">
          <Label htmlFor="truncate-offset">New base offset</Label>
          <Input id="truncate-offset" inputMode="numeric" value={value} onChange={(event) => setValue(event.target.value)} className="font-mono" />
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
          <div className="grid gap-2"><Label htmlFor="retention-bytes">Max retained bytes (0 = unlimited)</Label>
            <Input id="retention-bytes" inputMode="numeric" value={bytes} onChange={(event) => setBytes(event.target.value)} className="font-mono" /></div>
          <div className="grid gap-2"><Label htmlFor="retention-age">Max retained age ms (0 = unlimited)</Label>
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
      <div className="flex flex-wrap items-center gap-2">
        <Select value={partition} onValueChange={(value) => { setPartition(value); setOffset("0"); }}>
          <SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
          <SelectContent>
            {partitions.map((entry) => <SelectItem key={entry.partition} value={String(entry.partition)}>Partition {entry.partition}</SelectItem>)}
          </SelectContent>
        </Select>
        <Input value={offset} onChange={(event) => setOffset(event.target.value)} inputMode="numeric" className="w-32 font-mono" placeholder="offset" />
        <Button size="sm" variant="outline" onClick={() => void load(Number(offset) || 0)} disabled={busy}>Fetch from offset</Button>
        <Button size="sm" variant="ghost" onClick={() => { setOffset(String(nextOffsetOf)); void load(nextOffsetOf); }} disabled={busy || nextOffsetOf === 0}>Latest</Button>
      </div>
      {error && <ErrorBanner error={error} onRetry={() => void load(Number(offset) || 0)} />}
      <div className="grid gap-3">
        {records.map((record) => (
          <Card key={`${record.partition}:${record.offset}`}>
            <CardHeader className="pb-1">
              <CardTitle className="text-xs font-mono font-normal text-muted-foreground flex items-center gap-2">
                partition {record.partition} · offset {record.offset}
                {record.key && <Badge variant="outline" className="text-[10px] font-normal">keyed</Badge>}
              </CardTitle>
            </CardHeader>
            <CardContent className="grid gap-2">
              {record.key && (
                <div>
                  <p className="text-xs text-muted-foreground mb-1">Key</p>
                  <BinaryValue value={record.key as BinaryField} compact />
                </div>
              )}
              <div>
                <p className="text-xs text-muted-foreground mb-1">Body</p>
                <BinaryValue value={record.body as BinaryField} />
              </div>
            </CardContent>
          </Card>
        ))}
        {records.length === 0 && !busy && <p className="text-sm text-muted-foreground">No records in range.</p>}
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
    <Card>
      <CardHeader className="pb-2"><CardTitle className="text-sm flex items-center justify-between">Append records
        <label className="flex items-center gap-2 text-xs font-normal text-muted-foreground"><Switch checked={batch} onCheckedChange={setBatch} className="scale-90" /> batch mode</label>
      </CardTitle></CardHeader>
      <CardContent className="grid gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <Tabs value={mode} onValueChange={(value) => setMode(value as typeof mode)}>
            <TabsList><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger><TabsTrigger value="base64">Base64</TabsTrigger></TabsList>
          </Tabs>
          <Select value={partition} onValueChange={setPartition}>
            <SelectTrigger className="w-44"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="auto">Partition: auto (keyed)</SelectItem>
              {partitions.map((entry) => <SelectItem key={entry.partition} value={String(entry.partition)}>Partition {entry.partition}</SelectItem>)}
            </SelectContent>
          </Select>
          {!batch && <Input value={keyText} onChange={(event) => setKeyText(event.target.value)} placeholder="Optional partition key" spellCheck={false} className="max-w-xs" />}
        </div>
        {!batch ? (
          <>
            <Textarea value={raw} onChange={(event) => setRaw(event.target.value)} rows={4} spellCheck={false} className="font-mono text-xs" />
            <div className="flex items-center gap-3">
              <span className="text-xs text-muted-foreground">{draft.bytes} bytes</span>
              {draft.error && <span className="text-xs text-destructive">{draft.error}</span>}
              <Button className="ml-auto" onClick={() => void appendOne()} disabled={busy || !draft.base64 || Boolean(draft.error)}><Send className="size-4 mr-1" />Append</Button>
            </div>
          </>
        ) : (
          <>
            <p className="text-xs text-muted-foreground">One record body per line (up to 100), written as one durable WAL batch.</p>
            <Textarea value={batchLines} onChange={(event) => setBatchLines(event.target.value)} rows={6} spellCheck={false} className="font-mono text-xs" />
            <Button className="ml-auto justify-self-end" onClick={() => void appendBatch()} disabled={busy || batchLines.trim().length === 0}><Send className="size-4 mr-1" />Append batch</Button>
          </>
        )}
        {error && <ErrorBanner error={error} />}
      </CardContent>
    </Card>
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
    <Card>
      <CardHeader className="pb-2"><CardTitle className="text-sm">Live tail</CardTitle></CardHeader>
      <CardContent className="grid gap-3">
        <p className="text-xs text-muted-foreground">
          Bounded SSE worker through the console gateway. The server ends each worker after a short lifetime and throttles to the advertised event rate; the gateway buffers for safety, so events arrive as one bounded page. Tailing stops on navigation or disconnect.
        </p>
        <div className="flex items-end gap-2">
          <div className="grid gap-1"><Label>Partition</Label>
            <Select value={partition} onValueChange={setPartition}>
              <SelectTrigger className="w-36"><SelectValue /></SelectTrigger>
              <SelectContent>{partitions.map((entry) => <SelectItem key={entry.partition} value={String(entry.partition)}>{entry.partition}</SelectItem>)}</SelectContent>
            </Select>
          </div>
          <div className="grid gap-1"><Label htmlFor="tail-offset">Offset</Label>
            <Input id="tail-offset" value={offset} onChange={(event) => setOffset(event.target.value)} inputMode="numeric" className="w-28 font-mono" /></div>
          {running
            ? <Button variant="outline" onClick={stop}>Stop</Button>
            : <Button onClick={() => void start()}><Activity className="size-4 mr-1" />Start tail</Button>}
          {running && <StateBadge state="running" />}
        </div>
        {error && <ErrorBanner error={error} />}
        <div className="grid gap-2">
          {events.map((event) => event.kind === "offset_out_of_range"
            ? <div key={`${event.partition}:${event.offset}`} className="rounded-lg border border-warning/40 bg-warning/10 px-3 py-2 text-sm">offset_out_of_range — retention advanced past offset {event.offset}.</div>
            : (
              <div key={`${event.partition}:${event.offset}`} className="rounded-lg border p-2">
                <p className="text-xs font-mono text-muted-foreground mb-1">partition {event.partition} · offset {event.offset}</p>
                <BinaryValue value={event.body} compact />
              </div>
            ))}
        </div>
      </CardContent>
    </Card>
  );
}

export function useStreamsList(profileId: string) {
  const [streams, setStreams] = useState<StreamSummary[]>([]);
  const load = useCallback(async () => setStreams((await list<StreamSummary>(profileId, "streams")).data), [profileId]);
  useEffect(() => { void load(); }, [load]);
  return streams;
}
