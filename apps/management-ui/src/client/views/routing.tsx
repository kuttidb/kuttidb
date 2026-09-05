import { useCallback, useEffect, useState } from "react";
import { Plus, Trash2, Send, Shuffle, ArrowDownUp } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ConfirmDestructive } from "@/components/confirm";
import { ErrorBanner } from "@/components/error-banner";
import { CopyId, ConnectionContextLine, DetailGrid, EmptyState, LastRefreshed, PageHeader, Section, Skeleton } from "@/components/shared";
import { admin, list, newIdempotencyKey } from "@/lib/api";
import { nameFromId } from "@/lib/codec";
import type { QueueSummary, RouteEntry, RouterSummary } from "@/lib/types";

const modeBadgeVariant: Record<string, "info" | "warning" | "neutral"> = {
  exact: "info",
  broadcast: "warning",
  pattern: "neutral"
};

function ModeBadge({ mode }: { mode: string }) {
  return <Badge variant={modeBadgeVariant[mode] ?? "neutral"} className="font-mono text-xs">{mode}</Badge>;
}

export function RoutingView({ profileId, onOpenRouter }: { profileId: string; onOpenRouter: (routerId: string) => void }) {
  const [routers, setRouters] = useState<RouterSummary[]>([]);
  const [declareOpen, setDeclareOpen] = useState(false);
  const { lastUpdated, error, stale, refresh } = usePolling(
    useCallback(async () => setRouters((await list<RouterSummary>(profileId, "routing/routers")).data), [profileId]),
    15_000
  );
  const loadedOnce = lastUpdated !== null || error !== null;
  return (
    <div>
      <PageHeader
        title="Routing"
        description="Routers bind queues to exact, broadcast, or pattern keys. Metrics are process-lifetime."
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <Button size="sm" onClick={() => setDeclareOpen(true)}><Plus className="size-4" />Declare router</Button>
          </>
        }
      />
      {error && routers.length === 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {!loadedOnce ? (
        <div className="grid gap-2" aria-busy="true">
          {Array.from({ length: 3 }, (_, index) => <Skeleton key={index} className="h-11 w-full" />)}
          <p className="text-sm text-muted-foreground">Checking routers…</p>
        </div>
      ) : routers.length === 0 ? (
        <div className="border-t border-rule-strong pt-2">
          <EmptyState
            title="No routers declared."
            hint="Declare a router to bind queues to routing keys."
            action={<Button size="sm" onClick={() => setDeclareOpen(true)}><Plus className="size-4" />Declare router</Button>}
          />
        </div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead><TableHead>Mode</TableHead><TableHead className="text-right">Routes</TableHead>
              <TableHead className="text-right">Published</TableHead><TableHead className="text-right">Unroutable</TableHead><TableHead>Router ID</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {routers.map((router) => (
              <TableRow key={router.id} className="cursor-pointer" onClick={() => onOpenRouter(router.id)}>
                <TableCell>
                  <a
                    href={`#/c/${encodeURIComponent(profileId)}/routing/${encodeURIComponent(router.id)}`}
                    className="text-left font-medium hover:underline"
                    onClick={(event) => event.stopPropagation()}
                  >
                    {router.name}
                  </a>
                </TableCell>
                <TableCell><ModeBadge mode={router.mode} /></TableCell>
                <TableCell className="text-right tabular-nums">{router.route_count}</TableCell>
                <TableCell className="text-right tabular-nums">{router.publish_attempt_count}</TableCell>
                <TableCell className="text-right tabular-nums">{router.unroutable_count}</TableCell>
                <TableCell><CopyId id={router.id} /></TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
      <div className="mt-6">
        <DefaultPublishSection profileId={profileId} onPublished={refresh} />
      </div>
      <DeclareRouterDialog open={declareOpen} onOpenChange={setDeclareOpen} profileId={profileId} onDone={refresh} />
    </div>
  );
}

function DeclareRouterDialog({ open, onOpenChange, profileId, onDone }: { open: boolean; onOpenChange: (open: boolean) => void; profileId: string; onDone: () => void }) {
  const [name, setName] = useState("");
  const [mode, setMode] = useState("exact");
  const [durable, setDurable] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const submit = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, "routing/routers", { method: "POST", idempotencyKey: newIdempotencyKey(), body: { name: name.trim(), mode, durable } });
      toast.success(`Router ${name.trim()} declared`);
      onOpenChange(false); setName("");
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader><DialogTitle>Declare router</DialogTitle></DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="grid gap-1.5"><Label htmlFor="router-name">Name</Label><Input id="router-name" value={name} onChange={(event) => setName(event.target.value)} spellCheck={false} /></div>
          <div className="grid gap-1.5"><Label htmlFor="router-mode">Mode</Label>
            <Select value={mode} onValueChange={setMode}>
              <SelectTrigger id="router-mode"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="exact">exact</SelectItem>
                <SelectItem value="broadcast">broadcast</SelectItem>
                <SelectItem value="pattern">pattern</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <label className="flex items-center gap-2 text-sm">
            <Switch checked={durable} onCheckedChange={setDurable} aria-label="Durable" /> Durable
          </label>
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

function DefaultPublishSection({ profileId, onPublished }: { profileId: string; onPublished: () => void }) {
  const [queues, setQueues] = useState<QueueSummary[]>([]);
  const [queueId, setQueueId] = useState<string | null>(null);
  const [raw, setRaw] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  useEffect(() => {
    void (async () => {
      try { setQueues((await list<QueueSummary>(profileId, "queues")).data); } catch { /* list stays empty */ }
    })();
  }, [profileId]);
  const submit = async () => {
    if (!queueId) return;
    setBusy(true); setError(null);
    try {
      const encoded = new TextEncoder().encode(raw);
      let binary = "";
      for (const byte of encoded) binary += String.fromCharCode(byte);
      await admin(profileId, "routing/default/messages", {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: { queue_id: queueId, body: btoa(binary) }
      });
      toast.success("Published through default routing");
      setRaw("");
      onPublished();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <Section title="Default routing publish">
      <div className="grid gap-3">
        <div className="flex flex-wrap items-end gap-2">
          <div className="grid gap-1.5">
            <Label htmlFor="default-publish-queue">Target queue</Label>
            <Select value={queueId ?? ""} onValueChange={setQueueId}>
              <SelectTrigger id="default-publish-queue" className="w-64"><SelectValue placeholder="Choose a queue" /></SelectTrigger>
              <SelectContent>
                {queues.map((queue) => <SelectItem key={queue.id} value={queue.id}>{queue.name}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
          <div className="grid flex-1 gap-1.5">
            <Label htmlFor="default-publish-body">Message body (text)</Label>
            <Input id="default-publish-body" value={raw} onChange={(event) => setRaw(event.target.value)} spellCheck={false} className="max-w-md" />
          </div>
          <Button onClick={() => void submit()} disabled={busy || !queueId || raw.length === 0}><Send className="size-4" />{busy ? "Publishing…" : "Publish"}</Button>
        </div>
        {error && <ErrorBanner error={error} />}
      </div>
    </Section>
  );
}

export function RouterDetailView({ profileId, routerId, onBack }: { profileId: string; routerId: string; onBack: () => void }) {
  const [detail, setDetail] = useState<RouterSummary | null>(null);
  const [routes, setRoutes] = useState<RouteEntry[]>([]);
  const [routers, setRouters] = useState<RouterSummary[]>([]);
  const [queues, setQueues] = useState<QueueSummary[]>([]);
  const [etag, setEtag] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [routeOpen, setRouteOpen] = useState(false);
  const [publishOpen, setPublishOpen] = useState(false);
  const [deleteRouteId, setDeleteRouteId] = useState<string | null>(null);
  const [deleteRouterOpen, setDeleteRouterOpen] = useState(false);
  /** Revision captured when the confirmation opens, not the polling snapshot. */
  const [confirmEtag, setConfirmEtag] = useState<string | null>(null);
  const [dialogError, setDialogError] = useState<Error | null>(null);
  const [busy, setBusy] = useState(false);

  const loader = useCallback(async () => {
    const [routerResponse, routesResponse] = await Promise.all([
      admin<{ data: RouterSummary }>(profileId, `routing/routers/${routerId}`),
      list<RouteEntry>(profileId, `routing/routers/${routerId}/routes`)
    ]);
    setDetail(routerResponse.json.data);
    setEtag(routerResponse.etag);
    setRoutes(routesResponse.data);
  }, [profileId, routerId]);
  const { lastUpdated, error: pollError, stale, refresh } = usePolling(loader, 15_000);
  useEffect(() => {
    void (async () => {
      try {
        setRouters((await list<RouterSummary>(profileId, "routing/routers")).data);
        setQueues((await list<QueueSummary>(profileId, "queues")).data);
      } catch { /* optional lookups */ }
    })();
  }, [profileId]);

  // Confirmation-time revision for the destructive dialogs: fetch the current
  // router ETag when a confirmation opens.
  const destructiveOpen = deleteRouteId !== null || deleteRouterOpen;
  useEffect(() => {
    if (!destructiveOpen) return;
    let cancelled = false;
    setConfirmEtag(null);
    admin<{ data: RouterSummary }>(profileId, `routing/routers/${routerId}`)
      .then((response) => { if (!cancelled) setConfirmEtag(response.etag); })
      .catch(() => { /* the mutation will surface the real precondition error */ });
    return () => { cancelled = true; };
  }, [destructiveOpen, profileId, routerId]);

  const decodedName = detail ? nameFromId(detail.id) : null;
  const connectionContext = (
    <>
      <ConnectionContextLine profileId={profileId} />
      {decodedName && <span>Router: <span className="font-medium text-foreground">{decodedName}</span></span>}
    </>
  );

  const setAlternate = async (alternate: string) => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `routing/routers/${routerId}`, { method: "PATCH", idempotencyKey: newIdempotencyKey(), ifMatch: etag ?? undefined, body: { alternate_router_id: alternate } });
      toast.success("Alternate router set");
      refresh();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const deleteRoute = async (routeId: string) => {
    setBusy(true); setDialogError(null);
    try {
      await admin(profileId, `routing/routers/${routerId}/routes/${routeId}`, { method: "DELETE", idempotencyKey: newIdempotencyKey(), ifMatch: confirmEtag ?? etag ?? undefined, confirm: routeId, body: {} });
      toast.success("Route removed");
      setDeleteRouteId(null);
      refresh();
    } catch (reason) { setDialogError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const deleteRouter = async () => {
    setBusy(true); setDialogError(null);
    try {
      await admin(profileId, `routing/routers/${routerId}`, { method: "DELETE", idempotencyKey: newIdempotencyKey(), ifMatch: confirmEtag ?? etag ?? undefined, confirm: routerId, body: {} });
      toast.success("Router deleted");
      onBack();
    } catch (reason) { setDialogError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const viewError = error ?? pollError;

  return (
    <div>
      <PageHeader
        title={decodedName ?? routerId}
        breadcrumb={
          <button type="button" className="text-sm text-muted-foreground hover:text-foreground hover:underline" onClick={onBack}>
            Routing
          </button>
        }
        description={<CopyId id={routerId} label={`Copy router ID ${routerId}`} />}
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <Button variant="outline" size="sm" onClick={() => setPublishOpen(true)}><Send className="size-4" />Publish</Button>
            <Button size="sm" onClick={() => setRouteOpen(true)}><Plus className="size-4" />Bind route</Button>
            <Button variant="ghost" size="icon" className="text-destructive" onClick={() => setDeleteRouterOpen(true)} aria-label="Delete router" title="Delete router"><Trash2 className="size-4" /></Button>
          </>
        }
      />
      {viewError && <ErrorBanner error={viewError} onRetry={refresh} className="mb-4" />}
      {detail && (
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-3 lg:gap-8">
          <div className="border-t border-rule-strong pt-3">
            <h2 className="mb-3 text-base font-semibold tracking-[-0.015em]">Router</h2>
            <DetailGrid rows={[
              { label: "Name", value: detail.name },
              { label: "Mode", value: <ModeBadge mode={detail.mode} /> },
              { label: "Durable", value: String(detail.durable) },
              { label: "Revision (ETag)", value: etag ?? `r-${detail.revision}`, mono: true },
              { label: "Publish attempts", value: detail.publish_attempt_count },
              { label: "Unroutable", value: detail.unroutable_count }
            ]} />
          </div>
          <div className="border-t border-rule-strong pt-3">
            <h2 className="mb-3 flex items-center gap-2 text-base font-semibold tracking-[-0.015em]"><Shuffle className="size-4 text-muted-foreground" />Alternate router</h2>
            <p className="mb-2 text-xs text-muted-foreground">Used when this router cannot route a message. Server validation is authoritative; obvious client-side cycles are prevented.</p>
            <Select value={detail.alternate_router ?? "none"} onValueChange={(value) => { if (value === "none") return; void setAlternate(value); }}>
              <SelectTrigger aria-label="Alternate router"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="none">— none —</SelectItem>
                {routers.filter((router) => router.id !== routerId).map((router) => (
                  <SelectItem key={router.id} value={router.id}>{router.name} ({router.mode})</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="border-t border-rule-strong pt-3">
            <h2 className="mb-3 flex items-center gap-2 text-base font-semibold tracking-[-0.015em]"><ArrowDownUp className="size-4 text-muted-foreground" />Topology</h2>
            <p className="text-sm text-muted-foreground">{routes.length} route(s) bound. {detail.mode === "broadcast" ? "Every bound queue receives each publish." : detail.mode === "exact" ? "Only the exact routing key matches." : "Pattern keys match."}</p>
          </div>
        </div>
      )}
      <div className="mt-6 border-t border-rule-strong pt-3">
        <h2 className="mb-3 text-base font-semibold tracking-[-0.015em]">Routes</h2>
        <Table>
          <TableHeader><TableRow><TableHead>Routing key</TableHead><TableHead>Queue</TableHead><TableHead>Route ID</TableHead><TableHead className="text-right">Action</TableHead></TableRow></TableHeader>
          <TableBody>
            {routes.map((route) => (
              <TableRow key={route.route_id}>
                <TableCell className="font-mono text-xs">{route.routing_key.value}</TableCell>
                <TableCell>{route.queue.name}</TableCell>
                <TableCell><CopyId id={route.route_id} /></TableCell>
                <TableCell className="text-right">
                  <Button size="sm" variant="ghost" className="text-destructive" onClick={() => setDeleteRouteId(route.route_id)}>Remove</Button>
                </TableCell>
              </TableRow>
            ))}
            {routes.length === 0 && <TableRow><TableCell colSpan={4} className="py-6 text-center text-muted-foreground">No routes bound.</TableCell></TableRow>}
          </TableBody>
        </Table>
      </div>

      {routeOpen && (
        <BindRouteDialog open onOpenChange={setRouteOpen} profileId={profileId} routerId={routerId} queues={queues} onDone={refresh} />
      )}
      {publishOpen && (
        <RouterPublishDialog open onOpenChange={setPublishOpen} profileId={profileId} routerId={routerId} onDone={refresh} />
      )}
      {deleteRouteId && (
        <ConfirmDestructive
          open onOpenChange={() => { setDeleteRouteId(null); setDialogError(null); }} confirmId={deleteRouteId} inFlight={busy}
          title="Remove route" description="Durably removes this exact route binding from the router."
          context={connectionContext}
          error={dialogError}
          confirmLabel="Remove route"
          onConfirm={() => void deleteRoute(deleteRouteId)}
        />
      )}
      <ConfirmDestructive
        open={deleteRouterOpen} onOpenChange={(open) => { setDeleteRouterOpen(open); if (!open) setDialogError(null); }} confirmId={routerId} inFlight={busy}
        title="Delete router" description="Only an empty router (no routes, no alternate-router dependents) can be deleted."
        context={connectionContext}
        error={dialogError}
        confirmLabel="Delete router"
        onConfirm={() => void deleteRouter()}
      />
    </div>
  );
}

function BindRouteDialog({ open, onOpenChange, profileId, routerId, queues, onDone }: {
  open: boolean; onOpenChange: (open: boolean) => void; profileId: string; routerId: string; queues: QueueSummary[]; onDone: () => void;
}) {
  const [queueId, setQueueId] = useState<string | null>(null);
  const [key, setKey] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const submit = async () => {
    if (!queueId) return;
    setBusy(true); setError(null);
    try {
      await admin(profileId, `routing/routers/${routerId}/routes`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: { queue_id: queueId, routing_key: key.trim() }
      });
      toast.success("Route bound");
      onOpenChange(false); setKey("");
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader><DialogTitle>Bind queue to router</DialogTitle></DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="grid gap-1.5"><Label htmlFor="bind-queue">Queue</Label>
            <Select value={queueId ?? ""} onValueChange={setQueueId}>
              <SelectTrigger id="bind-queue"><SelectValue placeholder="Choose queue" /></SelectTrigger>
              <SelectContent>{queues.map((queue) => <SelectItem key={queue.id} value={queue.id}>{queue.name}</SelectItem>)}</SelectContent>
            </Select>
          </div>
          <div className="grid gap-1.5"><Label htmlFor="route-key">Routing key</Label>
            <Input id="route-key" value={key} onChange={(event) => setKey(event.target.value)} spellCheck={false} className="font-mono" /></div>
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={() => void submit()} disabled={busy || !queueId}>{busy ? "Binding…" : "Bind route"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function RouterPublishDialog({ open, onOpenChange, profileId, routerId, onDone }: {
  open: boolean; onOpenChange: (open: boolean) => void; profileId: string; routerId: string; onDone: () => void;
}) {
  const [key, setKey] = useState("");
  const [raw, setRaw] = useState("");
  const [result, setResult] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const submit = async () => {
    setBusy(true); setError(null); setResult(null);
    try {
      const encoded = new TextEncoder().encode(raw);
      let binary = "";
      for (const byte of encoded) binary += String.fromCharCode(byte);
      const response = await admin<{ data: { routed_queue_count: number; outcome: string } }>(profileId, `routing/routers/${routerId}/messages`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: { routing_key: key.trim(), body: btoa(binary) }
      });
      setResult(`${response.json.data.outcome} → ${response.json.data.routed_queue_count} queue(s)`);
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader><DialogTitle>Publish through router</DialogTitle>
          <DialogDescription>A publish result reports routing, not delivery. Routed count and unroutable outcome are process counters.</DialogDescription></DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="grid gap-1.5"><Label htmlFor="publish-key">Routing key</Label>
            <Input id="publish-key" value={key} onChange={(event) => setKey(event.target.value)} spellCheck={false} className="font-mono" /></div>
          <div className="grid gap-1.5"><Label htmlFor="publish-body">Body (text)</Label>
            <Input id="publish-body" value={raw} onChange={(event) => setRaw(event.target.value)} spellCheck={false} /></div>
          {error && <ErrorBanner error={error} />}
          {result && <p className="text-sm font-medium">{result}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Close</Button>
          <Button onClick={() => void submit()} disabled={busy || raw.length === 0}>{busy ? "Publishing…" : "Publish"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
