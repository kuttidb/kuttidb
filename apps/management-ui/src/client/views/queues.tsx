import { useCallback, useEffect, useState } from "react";
import { Plus, Trash2, Send, Inbox, Users, Lock, Eraser } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { BinaryValue, decodedPreview, encodeDraft, type BinaryField } from "@/components/binary-value";
import { ConfirmDestructive } from "@/components/confirm";
import { ErrorBanner } from "@/components/error-banner";
import { CopyId, CursorPager, DetailGrid, EmptyState, LastRefreshed, PageHeader, Section, StateBadge, ConnectionContextLine } from "@/components/shared";
import { admin, ApiError, list, newIdempotencyKey } from "@/lib/api";
import { nameFromId } from "@/lib/codec";
import { formatBytes } from "@/lib/format";
import { useConnections } from "@/state/connections";
import type { DeliveryDetail, DeliveryReceipt, QueueConsumer, QueueDetail, QueueMessage, QueueSummary } from "@/lib/types";

type MessageStateFilter = "all" | "ready" | "delayed" | "in-flight";

export function QueuesView({ profileId, onOpenQueue }: { profileId: string; onOpenQueue: (queueId: string) => void }) {
  const [queues, setQueues] = useState<QueueSummary[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [declareOpen, setDeclareOpen] = useState(false);
  const { lastUpdated, error, stale, refresh } = usePolling(
    useCallback(async () => {
      setQueues((await list<QueueSummary>(profileId, "queues")).data);
      setLoaded(true);
    }, [profileId]),
    15_000
  );

  return (
    <div>
      <PageHeader
        title="Queues"
        description="Durable queues with at-least-once delivery. Listing never includes message bodies."
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <Button size="sm" onClick={() => setDeclareOpen(true)}><Plus className="size-4 mr-1" />Declare queue</Button>
          </>
        }
      />
      {!loaded && !error && (
        <div className="grid gap-2" aria-busy="true">
          <span className="sr-only">Loading queues…</span>
          {Array.from({ length: 5 }, (_, index) => <Skeleton key={index} className="h-11" />)}
        </div>
      )}
      {error && !loaded && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {loaded && (
        <>
          {error && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
          {queues.length === 0 ? (
            <EmptyState
              title="No queues yet."
              hint="Declare a durable queue to publish and consume messages."
              action={<Button size="sm" onClick={() => setDeclareOpen(true)}><Plus className="size-4 mr-1" />Declare queue</Button>}
            />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Queue ID</TableHead>
                  <TableHead className="text-right">Ready</TableHead>
                  <TableHead className="text-right">In-flight</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {queues.map((queue) => (
                  <TableRow key={queue.id} className="cursor-pointer" onClick={() => onOpenQueue(queue.id)}>
                    <TableCell>
                      {/* Real hash link: supports open-in-new-tab and copy-link. */}
                      <a
                        href={`#/c/${encodeURIComponent(profileId)}/queues/${encodeURIComponent(queue.id)}`}
                        className="text-left font-medium hover:underline"
                        onClick={(event) => event.stopPropagation()}
                      >
                        {queue.name}
                      </a>
                    </TableCell>
                    <TableCell><CopyId id={queue.id} /></TableCell>
                    <TableCell className="text-right tabular-nums">{queue.ready_depth}</TableCell>
                    <TableCell className="text-right tabular-nums">{queue.in_flight}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </>
      )}
      <DeclareQueueDialog open={declareOpen} onOpenChange={setDeclareOpen} profileId={profileId} onDone={refresh} />
    </div>
  );
}

function DeclareQueueDialog({ open, onOpenChange, profileId, onDone }: { open: boolean; onOpenChange: (open: boolean) => void; profileId: string; onDone: () => void }) {
  const [name, setName] = useState("");
  const [durable, setDurable] = useState(true);
  const [maxDepth, setMaxDepth] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const submit = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, "queues", {
        method: "POST", idempotencyKey: newIdempotencyKey(),
        body: { name: name.trim(), durable, ...(maxDepth.trim().length > 0 ? { max_depth: Number(maxDepth) } : {}) }
      });
      toast.success(`Queue ${name.trim()} declared`);
      onOpenChange(false); setName(""); setMaxDepth("");
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[480px]">
        <DialogHeader><DialogTitle>Declare queue</DialogTitle><DialogDescription>Declarations are durable; re-declaring with different options is rejected.</DialogDescription></DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="grid gap-2"><Label htmlFor="queue-name">Name</Label><Input id="queue-name" value={name} onChange={(event) => setName(event.target.value)} spellCheck={false} /></div>
          <div className="flex items-center justify-between">
            <Label htmlFor="queue-durable">Durable</Label>
            <Switch id="queue-durable" checked={durable} onCheckedChange={setDurable} />
          </div>
          <div className="grid gap-2"><Label htmlFor="queue-depth">Max depth (optional)</Label><Input id="queue-depth" inputMode="numeric" value={maxDepth} onChange={(event) => setMaxDepth(event.target.value)} placeholder="unbounded" /></div>
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

export function QueueDetailView({ profileId, queueId, onBack }: { profileId: string; queueId: string; onBack: () => void }) {
  const [detail, setDetail] = useState<QueueDetail | null>(null);
  const [etag, setEtag] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [dialogError, setDialogError] = useState<Error | null>(null);
  const [purgeOpen, setPurgeOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  /** Revision captured when the confirmation opens, not the polling snapshot. */
  const [confirmEtag, setConfirmEtag] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const loader = useCallback(async () => {
    const response = await admin<{ data: QueueDetail }>(profileId, `queues/${queueId}`);
    setDetail(response.json.data);
    setEtag(response.etag);
  }, [profileId, queueId]);
  const { lastUpdated, error: pollError, stale, refresh } = usePolling(loader, 15_000);

  // Confirmation-time revision: fetch the current ETag when a destructive
  // dialog opens, so a concurrent change during polling cannot be confirmed
  // against a stale revision.
  useEffect(() => {
    if (!purgeOpen && !deleteOpen) return;
    let cancelled = false;
    setConfirmEtag(null);
    admin<{ data: QueueDetail }>(profileId, `queues/${queueId}`)
      .then((response) => { if (!cancelled) setConfirmEtag(response.etag); })
      .catch(() => { /* the mutation will surface the real precondition error */ });
    return () => { cancelled = true; };
  }, [purgeOpen, deleteOpen, profileId, queueId]);

  const destructive = async (kind: "purge" | "delete") => {
    setBusy(true); setDialogError(null);
    try {
      await admin(profileId, kind === "purge" ? `queues/${queueId}:purge` : `queues/${queueId}`, {
        method: kind === "purge" ? "POST" : "DELETE",
        idempotencyKey: newIdempotencyKey(),
        ifMatch: confirmEtag ?? etag ?? undefined,
        confirm: queueId,
        body: {}
      });
      toast.success(kind === "purge" ? "Queue purged" : "Queue deleted");
      setPurgeOpen(false); setDeleteOpen(false);
      if (kind === "delete") { onBack(); return; }
      refresh();
    } catch (reason) {
      // Keep the explanation inside the open dialog until it is resolved.
      setDialogError(reason instanceof Error ? reason : new Error(String(reason)));
    }
    finally { setBusy(false); }
  };

  const decodedName = detail ? nameFromId(detail.id) : null;
  const connectionContext = <ConnectionContextLine profileId={profileId} />;

  const openDestructiveDialog = (kind: "purge" | "delete") => {
    setDialogError(null);
    if (kind === "purge") setPurgeOpen(true); else setDeleteOpen(true);
  };

  return (
    <div>
      <PageHeader
        breadcrumb={<button type="button" className="text-sm text-link hover:underline" onClick={onBack}>Queues</button>}
        title={decodedName ?? queueId}
        description={<CopyId id={queueId} />}
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" aria-label="Queue actions">Actions</Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={() => openDestructiveDialog("purge")}><Eraser className="size-4 mr-2" />Purge messages…</DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem className="text-destructive" onClick={() => openDestructiveDialog("delete")}><Trash2 className="size-4 mr-2" />Delete queue…</DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </>
        }
      />
      {pollError && <ErrorBanner error={pollError} onRetry={refresh} className="mb-4" />}
      {error && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {!detail && !pollError && (
        <div className="grid gap-2" aria-busy="true">
          <span className="sr-only">Loading queue…</span>
          {Array.from({ length: 5 }, (_, index) => <Skeleton key={index} className="h-11" />)}
        </div>
      )}
      {detail && (
        <Tabs defaultValue="overview">
          <TabsList className="mb-4">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="messages"><Inbox className="size-3.5 mr-1" />Messages</TabsTrigger>
            <TabsTrigger value="publish"><Send className="size-3.5 mr-1" />Publish</TabsTrigger>
            <TabsTrigger value="deliveries"><Lock className="size-3.5 mr-1" />Deliveries</TabsTrigger>
            <TabsTrigger value="consumers"><Users className="size-3.5 mr-1" />Consumers</TabsTrigger>
          </TabsList>
          <TabsContent value="overview">
            <Section title="Queue facts">
              <DetailGrid rows={[
                { label: "Name", value: detail.name },
                { label: "Queue ID", value: <CopyId id={detail.id} />, mono: true },
                { label: "Durable", value: String(detail.durable) },
                { label: "Max depth", value: detail.max_depth === 0 ? "unbounded" : detail.max_depth },
                { label: "Max deliveries", value: detail.max_deliveries === 0 ? "unbounded" : detail.max_deliveries },
                { label: "Dead-letter queue", value: detail.dead_letter_queue ? <CopyId id={detail.dead_letter_queue} /> : "—" },
                { label: "Ready depth", value: detail.ready_depth },
                { label: "In-flight", value: detail.in_flight },
                { label: "Revision (ETag)", value: etag ?? `q-${detail.revision}`, mono: true }
              ]} />
            </Section>
          </TabsContent>
          <TabsContent value="messages"><MessagesTab profileId={profileId} queueId={queueId} /></TabsContent>
          <TabsContent value="publish"><PublishTab profileId={profileId} queueId={queueId} onPublished={refresh} /></TabsContent>
          <TabsContent value="deliveries"><DeliveriesTab profileId={profileId} queueId={queueId} onChanged={refresh} /></TabsContent>
          <TabsContent value="consumers"><ConsumersTab profileId={profileId} queueId={queueId} /></TabsContent>
        </Tabs>
      )}
      <ConfirmDestructive
        open={purgeOpen} onOpenChange={setPurgeOpen} confirmId={queueId} inFlight={busy}
        title="Purge all retained messages"
        description="Discards ready, delayed, and in-flight messages. This cannot be undone."
        affected={detail ? <>Queue <span className="font-mono text-xs">{decodedName ?? detail.id}</span> currently holds {detail.ready_depth} ready and {detail.in_flight} in-flight messages.</> : undefined}
        context={connectionContext}
        error={dialogError}
        confirmLabel="Purge messages"
        onConfirm={() => void destructive("purge")}
      />
      <ConfirmDestructive
        open={deleteOpen} onOpenChange={setDeleteOpen} confirmId={queueId} inFlight={busy}
        title="Delete queue"
        description="Durably removes the queue and every retained delivery. Deletion is refused while a durable route still targets it."
        context={connectionContext}
        error={dialogError}
        confirmLabel="Delete queue"
        onConfirm={() => void destructive("delete")}
      />
    </div>
  );
}

function MessagesTab({ profileId, queueId }: { profileId: string; queueId: string }) {
  const [messages, setMessages] = useState<QueueMessage[]>([]);
  const [stateFilter, setStateFilter] = useState<MessageStateFilter>("all");
  const [includeBody, setIncludeBody] = useState(true);
  const [cursor, setCursor] = useState<string | null>(null);
  const [backStack, setBackStack] = useState<string[]>([]);
  const [meta, setMeta] = useState<{ nextCursor: string | null; weak: boolean } | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loaded, setLoaded] = useState(false);

  const load = useCallback(async (cursorValue: string | null) => {
    setError(null);
    try {
      const stateQuery = stateFilter === "all" ? "" : `&state=${stateFilter}`;
      const bodyQuery = includeBody ? "&include=body" : "";
      const response = await list<QueueMessage>(profileId, `queues/${queueId}/messages?limit=50${stateQuery}${bodyQuery}${cursorValue ? `&cursor=${encodeURIComponent(cursorValue)}` : ""}`);
      setMessages(response.data);
      setMeta({ nextCursor: response.meta?.next_cursor ?? null, weak: response.meta?.weakly_consistent ?? false });
      setLoaded(true);
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
  }, [profileId, queueId, stateFilter, includeBody]);

  useEffect(() => { setBackStack([]); setCursor(null); void load(null); }, [load]);

  return (
    <div className="grid gap-3">
      <div className="flex flex-wrap items-end gap-4">
        <div className="grid gap-1.5">
          <Label htmlFor="message-state">State</Label>
          <Select value={stateFilter} onValueChange={(value) => { setStateFilter(value as MessageStateFilter); setBackStack([]); setCursor(null); void load(null); }}>
            <SelectTrigger id="message-state" className="w-40"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All states</SelectItem>
              <SelectItem value="ready">ready</SelectItem>
              <SelectItem value="delayed">delayed</SelectItem>
              <SelectItem value="in-flight">in-flight</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <label className="flex items-center gap-2 pb-2.5 text-sm">
          <Switch checked={includeBody} onCheckedChange={(checked) => { setIncludeBody(checked); setBackStack([]); setCursor(null); void load(null); }} /> Include bodies
        </label>
        <span className="ml-auto self-end pb-2.5 text-xs text-muted-foreground">Browsing never consumes, requeues, or reorders messages.</span>
      </div>
      {error && <ErrorBanner error={error} onRetry={() => void load(cursor)} />}
      {!loaded && !error && (
        <div className="grid gap-2" aria-busy="true">
          <span className="sr-only">Loading messages…</span>
          {Array.from({ length: 4 }, (_, index) => <Skeleton key={index} className="h-11" />)}
        </div>
      )}
      {loaded && messages.length === 0 && (
        <EmptyState title="No retained messages in this state." />
      )}
      {loaded && messages.length > 0 && (
        <Table>
          <TableHeader>
            <TableRow><TableHead>ID</TableHead><TableHead>State</TableHead><TableHead className="text-right">Size</TableHead><TableHead className="text-right">Deliveries</TableHead><TableHead>Body</TableHead></TableRow>
          </TableHeader>
          <TableBody>
            {messages.map((message) => (
              <TableRow key={message.message_id}>
                <TableCell className="font-mono text-xs">{message.message_id}</TableCell>
                <TableCell><StateBadge state={message.state} /></TableCell>
                <TableCell className="text-right tabular-nums">{formatBytes(message.size)}</TableCell>
                <TableCell className="text-right tabular-nums">{message.delivery_count}{message.redelivered ? <Badge variant="outline" className="ml-2 text-xs">redelivered</Badge> : null}</TableCell>
                <TableCell className="max-w-sm">
                  {message.body
                    ? <BinaryValue value={message.body as BinaryField} compact />
                    : <span className="text-xs text-muted-foreground">{decodedPreview(null)}</span>}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
      <CursorPager
        nextCursor={meta?.nextCursor} backStack={backStack} weaklyConsistent={meta?.weak}
        onBack={() => { const previous = backStack[backStack.length - 1]; if (previous === undefined) return; setBackStack((stack) => stack.slice(0, -1)); setCursor(previous); void load(previous); }}
        onNext={() => { if (!meta?.nextCursor) return; setBackStack((stack) => [...stack, cursor ?? ""]); setCursor(meta.nextCursor); void load(meta.nextCursor); }}
      />
    </div>
  );
}

function PublishTab({ profileId, queueId, onPublished }: { profileId: string; queueId: string; onPublished: () => void }) {
  const [mode, setMode] = useState<"text" | "json" | "base64">("text");
  const [raw, setRaw] = useState("");
  const [batch, setBatch] = useState(false);
  const [batchLines, setBatchLines] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const draft = encodeDraft(mode, raw);

  const publishOne = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `queues/${queueId}/messages`, { method: "POST", idempotencyKey: newIdempotencyKey(), body: { body: draft.base64 } });
      toast.success("Message published");
      setRaw("");
      onPublished();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const publishBatch = async () => {
    const bodies = batchLines.split("\n").map((line) => line.trim()).filter(Boolean).map((line) => encodeDraft(mode, line).base64);
    if (bodies.some((body) => body.length === 0)) { setError(new ApiError("validation_failed", "One batch line could not be encoded.", 400)); return; }
    setBusy(true); setError(null);
    try {
      await admin(profileId, `queues/${queueId}/messages:batch`, { method: "POST", idempotencyKey: newIdempotencyKey(), body: { messages: bodies.map((body) => ({ body })) } });
      toast.success(`${bodies.length} messages published`);
      setBatchLines("");
      onPublished();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  return (
    <Section
      title="Publish message"
      actions={
        <label className="flex items-center gap-2 text-sm text-muted-foreground">
          <Switch checked={batch} onCheckedChange={setBatch} /> Batch mode
        </label>
      }
    >
      {!batch ? (
        <div className="grid gap-3">
          <div className="grid gap-1.5">
            <Label>Encoding</Label>
            <Tabs value={mode} onValueChange={(value) => setMode(value as typeof mode)}>
              <TabsList><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger><TabsTrigger value="base64">Base64</TabsTrigger></TabsList>
            </Tabs>
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="publish-body">Message body (single)</Label>
            <Textarea id="publish-body" value={raw} onChange={(event) => setRaw(event.target.value)} rows={4} spellCheck={false} className="font-mono text-xs" />
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-muted-foreground">{draft.bytes} bytes</span>
            {draft.error && <span className="text-xs text-destructive">{draft.error}</span>}
            <Button className="ml-auto" onClick={() => void publishOne()} disabled={busy || !draft.base64 || Boolean(draft.error)}><Send className="size-4 mr-1" />{busy ? "Publishing…" : "Publish"}</Button>
          </div>
        </div>
      ) : (
        <div className="grid gap-3">
          <p className="text-xs text-muted-foreground">One message per line, up to 100. Batch publishes are capacity-checked atomically before any write.</p>
          <div className="grid gap-1.5">
            <Label htmlFor="publish-batch">One message per line (batch)</Label>
            <Textarea id="publish-batch" value={batchLines} onChange={(event) => setBatchLines(event.target.value)} rows={6} spellCheck={false} className="font-mono text-xs" />
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-muted-foreground">{batchLines.split("\n").filter((line) => line.trim().length > 0).length} messages</span>
            <Button className="ml-auto" onClick={() => void publishBatch()} disabled={busy || batchLines.trim().length === 0}><Send className="size-4 mr-1" />{busy ? "Publishing batch…" : "Publish batch"}</Button>
          </div>
        </div>
      )}
      {error && <ErrorBanner error={error} />}
    </Section>
  );
}

type LeaseReceipt = { detail: DeliveryDetail | null; receipt: DeliveryReceipt; acked: boolean; nacked: boolean };

function DeliveriesTab({ profileId, queueId, onChanged }: { profileId: string; queueId: string; onChanged: () => void }) {
  const [leases, setLeases] = useState<LeaseReceipt[]>([]);
  const [visibilityMs, setVisibilityMs] = useState("30000");
  const [nackRequeue, setNackRequeue] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const consume = async () => {
    setBusy(true); setError(null);
    try {
      const response = await admin<{ data: DeliveryReceipt | null }>(profileId, `queues/${queueId}/deliveries`, {
        method: "POST", idempotencyKey: newIdempotencyKey(),
        body: { ...(visibilityMs ? { visibility_ms: Number(visibilityMs) } : {}) }
      });
      if (!response.json.data) { toast.info("No ready messages"); return; }
      const receipt = response.json.data;
      let detail: DeliveryDetail | null = null;
      try {
        const detailResponse = await admin<{ data: DeliveryDetail }>(profileId, `queues/${queueId}/deliveries/${receipt.delivery_id}?include=body`);
        detail = detailResponse.json.data;
      } catch { detail = null; }
      setLeases((current) => [{ detail, receipt, acked: false, nacked: false }, ...current]);
      onChanged();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const act = async (deliveryId: string, kind: "ack" | "nack") => {
    setError(null);
    try {
      await admin(profileId, `queues/${queueId}/deliveries/${deliveryId}:${kind}`, {
        method: "POST", idempotencyKey: newIdempotencyKey(),
        body: kind === "nack" ? { requeue: nackRequeue } : {}
      });
      setLeases((current) => current.map((lease) => lease.receipt.delivery_id === deliveryId ? { ...lease, acked: kind === "ack", nacked: kind === "nack" } : lease));
      toast.success(kind === "ack" ? "Acknowledged" : "Nacked");
      onChanged();
    } catch (reason) {
      if (reason instanceof ApiError && (reason.code === "delivery_expired" || reason.status === 410)) {
        setLeases((current) => current.map((lease) => lease.receipt.delivery_id === deliveryId ? { ...lease, acked: true, nacked: true } : lease));
        toast.error("Delivery lease expired — receipt is immutable");
      } else { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    }
  };

  return (
    <Section title="Consume workspace">
      <div className="grid gap-3">
        <p className="text-xs text-muted-foreground">Advanced administrative consuming. Production workers should use client libraries or durable consumers.</p>
        <div className="flex flex-wrap items-end gap-4">
          <div className="grid gap-1.5">
            <Label htmlFor="visibility">Visibility (ms)</Label>
            <Input id="visibility" value={visibilityMs} onChange={(event) => setVisibilityMs(event.target.value)} inputMode="numeric" className="w-36" />
          </div>
          <label className="flex items-center gap-2 pb-2.5 text-sm">
            <Switch checked={nackRequeue} onCheckedChange={setNackRequeue} /> NACK requeues
          </label>
          <Button className="ml-auto" onClick={() => void consume()} disabled={busy}><Inbox className="size-4 mr-1" />{busy ? "Consuming…" : "Consume one"}</Button>
        </div>
        {error && <ErrorBanner error={error} />}
        {leases.length === 0 && <p className="text-sm text-muted-foreground">No active administrative deliveries.</p>}
        <div className="grid gap-3">
          {leases.map((lease) => (
            <div key={lease.receipt.delivery_id} className="rounded-none border p-3 grid gap-2">
              <div className="flex flex-wrap items-center gap-2">
                <CopyId id={lease.receipt.delivery_id} className="max-w-64" />
                <Badge variant="outline" className="font-mono text-xs">msg {lease.receipt.message_id}</Badge>
                {lease.acked && <StateBadge state="succeeded" />}
                {lease.nacked && <StateBadge state="failed" />}
                {!lease.acked && !lease.nacked && (
                  <div className="ml-auto flex gap-2">
                    <Button size="sm" variant="outline" onClick={() => void act(lease.receipt.delivery_id, "nack")}>NACK</Button>
                    <Button size="sm" onClick={() => void act(lease.receipt.delivery_id, "ack")}>ACK</Button>
                  </div>
                )}
              </div>
              {lease.detail?.body && <BinaryValue value={lease.detail.body} compact />}
            </div>
          ))}
        </div>
      </div>
    </Section>
  );
}

function ConsumersTab({ profileId, queueId }: { profileId: string; queueId: string }) {
  const [consumers, setConsumers] = useState<QueueConsumer[]>([]);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [confirmId, setConfirmId] = useState<string | null>(null);
  const [confirmError, setConfirmError] = useState<Error | null>(null);

  const load = useCallback(async () => setConsumers((await list<QueueConsumer>(profileId, "queue-consumers")).data), [profileId]);
  useEffect(() => { void load(); }, [load]);

  const register = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, "queue-consumers", { method: "POST", idempotencyKey: newIdempotencyKey(), body: { name: name.trim() } });
      toast.success(`Consumer ${name.trim()} registered`);
      setName("");
      await load();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const unregister = async (consumerId: string) => {
    setBusy(true); setConfirmError(null);
    try {
      const detail = await admin<{ data: { revision?: number } }>(profileId, `queue-consumers/${consumerId}`);
      await admin(profileId, `queue-consumers/${consumerId}`, {
        method: "DELETE", idempotencyKey: newIdempotencyKey(),
        ifMatch: detail.etag ?? undefined, confirm: consumerId, body: {}
      });
      toast.success("Consumer unregistered; in-flight messages requeued");
      setConfirmId(null);
      await load();
    } catch (reason) { setConfirmError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const consumeThrough = async (consumerId: string) => {
    setError(null);
    try {
      const response = await admin<{ data: { delivery_id: string; message_id: number } | null }>(profileId, `queue-consumers/${consumerId}/deliveries`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: { queue_id: queueId }
      });
      if (!response.json.data) { toast.info("No ready messages"); return; }
      toast.success(`Delivery ${response.json.data.delivery_id} reserved for this consumer — ACK through Deliveries is not cross-linked; use the API for consumer receipts`, { duration: 6000 });
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
  };

  const connectionContext = <ConnectionContextLine profileId={profileId} />;

  return (
    <Section title="Durable consumers">
      <div className="grid gap-3">
        <div className="flex flex-wrap items-end gap-2">
          <div className="grid gap-1.5">
            <Label htmlFor="consumer-name">Consumer name</Label>
            <Input id="consumer-name" value={name} onChange={(event) => setName(event.target.value)} placeholder="worker-1" spellCheck={false} className="w-64" />
          </div>
          <Button onClick={() => void register()} disabled={busy || name.trim().length === 0}>{busy ? "Registering…" : "Register"}</Button>
        </div>
        {error && <ErrorBanner error={error} />}
        <Table>
          <TableHeader><TableRow><TableHead>Name</TableHead><TableHead>Consumer ID</TableHead><TableHead className="text-right">Actions</TableHead></TableRow></TableHeader>
          <TableBody>
            {consumers.map((consumer) => (
              <TableRow key={consumer.id}>
                <TableCell className="font-medium">{consumer.name}</TableCell>
                <TableCell><CopyId id={consumer.id} /></TableCell>
                <TableCell className="text-right">
                  <div className="flex justify-end gap-2">
                    <Button size="sm" variant="outline" onClick={() => void consumeThrough(consumer.id)}>Consume</Button>
                    <Button size="sm" variant="ghost" className="text-destructive" onClick={() => setConfirmId(consumer.id)}>Unregister</Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
            {consumers.length === 0 && <TableRow><TableCell colSpan={3} className="text-center text-muted-foreground py-6">No durable consumers registered.</TableCell></TableRow>}
          </TableBody>
        </Table>
      </div>
      {confirmId && (
        <ConfirmDestructive
          open onOpenChange={(open) => { if (!open) { setConfirmId(null); setConfirmError(null); } }} confirmId={confirmId} inFlight={busy}
          title="Unregister durable consumer"
          description="Durably unregisters this consumer and requeues its in-flight messages."
          context={connectionContext}
          error={confirmError}
          confirmLabel="Unregister consumer"
          onConfirm={() => void unregister(confirmId)}
        />
      )}
    </Section>
  );
}
