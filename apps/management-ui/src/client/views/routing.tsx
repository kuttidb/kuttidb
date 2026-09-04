import { useCallback, useEffect, useState } from "react";
import { Plus, Trash2, Send, Shuffle, ArrowDownUp } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ConfirmDestructive } from "@/components/confirm";
import { ErrorBanner } from "@/components/error-banner";
import { CopyId, DetailGrid, LastRefreshed, PageHeader } from "@/components/shared";
import { admin, list, newIdempotencyKey } from "@/lib/api";
import { idFromName, nameFromId } from "@/lib/codec";
import type { QueueSummary, RouteEntry, RouterSummary } from "@/lib/types";

export function RoutingView({ profileId, onOpenRouter }: { profileId: string; onOpenRouter: (routerId: string) => void }) {
  const [routers, setRouters] = useState<RouterSummary[]>([]);
  const [declareOpen, setDeclareOpen] = useState(false);
  const { lastUpdated, error, refresh } = usePolling(
    useCallback(async () => setRouters((await list<RouterSummary>(profileId, "routing/routers")).data), [profileId]),
    15_000
  );
  return (
    <div>
      <PageHeader
        title="Routing"
        description="Routers bind queues to exact, broadcast, or pattern keys. Metrics are process-lifetime."
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} />
            <Button size="sm" onClick={() => setDeclareOpen(true)}><Plus className="size-4 mr-1" />Declare router</Button>
          </>
        }
      />
      {error && routers.length === 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      <Card>
        <CardContent className="p-0">
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
                  <TableCell className="font-medium">{router.name}</TableCell>
                  <TableCell><ModeBadge mode={router.mode} /></TableCell>
                  <TableCell className="text-right tabular-nums">{router.route_count}</TableCell>
                  <TableCell className="text-right tabular-nums">{router.publish_attempt_count}</TableCell>
                  <TableCell className="text-right tabular-nums">{router.unroutable_count}</TableCell>
                  <TableCell><CopyId id={router.id} /></TableCell>
                </TableRow>
              ))}
              {routers.length === 0 && <TableRow><TableCell colSpan={6} className="text-center text-muted-foreground py-8">No routers declared.</TableCell></TableRow>}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <DefaultPublishCard profileId={profileId} onPublished={refresh} />
      <DeclareRouterDialog open={declareOpen} onOpenChange={setDeclareOpen} profileId={profileId} onDone={refresh} />
    </div>
  );
}

function ModeBadge({ mode }: { mode: string }) {
  const styles = mode === "exact" ? "bg-chart-2/15 text-chart-2 border-transparent" : mode === "broadcast" ? "bg-chart-4/15 text-chart-4 border-transparent" : "bg-chart-5/15 text-chart-5 border-transparent";
  return <Badge variant="outline" className={`font-mono text-[11px] ${styles}`}>{mode}</Badge>;
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
          <div className="grid gap-2"><Label htmlFor="router-name">Name</Label><Input id="router-name" value={name} onChange={(event) => setName(event.target.value)} spellCheck={false} /></div>
          <div className="grid gap-2"><Label>Mode</Label>
            <Select value={mode} onValueChange={setMode}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="exact">exact</SelectItem>
                <SelectItem value="broadcast">broadcast</SelectItem>
                <SelectItem value="pattern">pattern</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={durable} onChange={(event) => setDurable(event.target.checked)} className="size-4" /> Durable</label>
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

function DefaultPublishCard({ profileId, onPublished }: { profileId: string; onPublished: () => void }) {
  const [queues, setQueues] = useState<QueueSummary[]>([]);
  const [queueId, setQueueId] = useState<string | null>(null);
  const [raw, setRaw] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  useEffect(() => {
    void (async () => {
      try { setQueues((await list<QueueSummary>(profileId, "queues")).data); } catch { /* overview empty state */ }
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
    <Card className="mt-4">
      <CardHeader className="pb-2"><CardTitle className="text-sm">Default routing publish</CardTitle></CardHeader>
      <CardContent className="grid gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <Select value={queueId ?? ""} onValueChange={setQueueId}>
            <SelectTrigger className="w-64"><SelectValue placeholder="Target queue" /></SelectTrigger>
            <SelectContent>
              {queues.map((queue) => <SelectItem key={queue.id} value={queue.id}>{queue.name}</SelectItem>)}
            </SelectContent>
          </Select>
          <Input value={raw} onChange={(event) => setRaw(event.target.value)} placeholder="Message body (text)" spellCheck={false} className="max-w-md" />
          <Button onClick={() => void submit()} disabled={busy || !queueId || raw.length === 0}><Send className="size-4 mr-1" />Publish</Button>
        </div>
        {error && <ErrorBanner error={error} />}
      </CardContent>
    </Card>
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
  const { lastUpdated, refresh } = usePolling(loader, 15_000);
  useEffect(() => {
    void (async () => {
      try {
        setRouters((await list<RouterSummary>(profileId, "routing/routers")).data);
        setQueues((await list<QueueSummary>(profileId, "queues")).data);
      } catch { /* optional lookups */ }
    })();
  }, [profileId]);

  const decodedName = detail ? nameFromId(detail.id) : null;

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
    setBusy(true); setError(null);
    try {
      await admin(profileId, `routing/routers/${routerId}/routes/${routeId}`, { method: "DELETE", idempotencyKey: newIdempotencyKey(), ifMatch: etag ?? undefined, confirm: routeId, body: {} });
      toast.success("Route removed");
      setDeleteRouteId(null);
      refresh();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const deleteRouter = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `routing/routers/${routerId}`, { method: "DELETE", idempotencyKey: newIdempotencyKey(), ifMatch: etag ?? undefined, confirm: routerId, body: {} });
      toast.success("Router deleted");
      onBack();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  return (
    <div>
      <PageHeader
        title={decodedName ?? routerId}
        description={<span className="font-mono text-xs">{routerId}</span>}
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} />
            <Button variant="outline" size="sm" onClick={() => setPublishOpen(true)}><Send className="size-4 mr-1" />Publish</Button>
            <Button size="sm" onClick={() => setRouteOpen(true)}><Plus className="size-4 mr-1" />Bind route</Button>
            <Button variant="ghost" size="icon" className="text-destructive" onClick={() => setDeleteRouterOpen(true)}><Trash2 className="size-4" /></Button>
          </>
        }
      />
      {error && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {detail && (
        <div className="grid gap-4 lg:grid-cols-3 mb-4">
          <Card>
            <CardHeader className="pb-2"><CardTitle className="text-sm">Router</CardTitle></CardHeader>
            <CardContent>
              <DetailGrid rows={[
                { label: "Name", value: detail.name },
                { label: "Mode", value: <ModeBadge mode={detail.mode} /> },
                { label: "Durable", value: String(detail.durable) },
                { label: "Revision (ETag)", value: etag ?? `r-${detail.revision}`, mono: true },
                { label: "Publish attempts", value: detail.publish_attempt_count },
                { label: "Unroutable", value: detail.unroutable_count }
              ]} />
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2"><CardTitle className="text-sm flex items-center gap-2"><Shuffle className="size-4 text-muted-foreground" />Alternate router</CardTitle></CardHeader>
            <CardContent>
              <p className="text-xs text-muted-foreground mb-2">Used when this router cannot route a message. Server validation is authoritative; obvious client-side cycles are prevented.</p>
              <Select value={detail.alternate_router ?? "none"} onValueChange={(value) => { if (value === "none") return; void setAlternate(value); }}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">— none —</SelectItem>
                  {routers.filter((router) => router.id !== routerId).map((router) => (
                    <SelectItem key={router.id} value={router.id}>{router.name} ({router.mode})</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2"><CardTitle className="text-sm flex items-center gap-2"><ArrowDownUp className="size-4 text-muted-foreground" />Topology</CardTitle></CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">{routes.length} route(s) bound. {detail.mode === "broadcast" ? "Every bound queue receives each publish." : detail.mode === "exact" ? "Only the exact routing key matches." : "Pattern keys match."}</p>
            </CardContent>
          </Card>
        </div>
      )}
      <Card>
        <CardHeader className="pb-2"><CardTitle className="text-sm">Routes</CardTitle></CardHeader>
        <CardContent className="p-0">
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
              {routes.length === 0 && <TableRow><TableCell colSpan={4} className="text-center text-muted-foreground py-6">No routes bound.</TableCell></TableRow>}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {routeOpen && (
        <BindRouteDialog open onOpenChange={setRouteOpen} profileId={profileId} routerId={routerId} queues={queues} onDone={refresh} />
      )}
      {publishOpen && (
        <RouterPublishDialog open onOpenChange={setPublishOpen} profileId={profileId} routerId={routerId} onDone={refresh} />
      )}
      {deleteRouteId && (
        <ConfirmDestructive
          open onOpenChange={() => setDeleteRouteId(null)} confirmId={deleteRouteId} inFlight={busy}
          title="Remove route" description="Durably removes this exact route binding from the router."
          onConfirm={() => void deleteRoute(deleteRouteId)}
        />
      )}
      <ConfirmDestructive
        open={deleteRouterOpen} onOpenChange={setDeleteRouterOpen} confirmId={routerId} inFlight={busy}
        title="Delete router" description="Only an empty router (no routes, no alternate-router dependents) can be deleted."
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
          <div className="grid gap-2"><Label>Queue</Label>
            <Select value={queueId ?? ""} onValueChange={setQueueId}>
              <SelectTrigger><SelectValue placeholder="Choose queue" /></SelectTrigger>
              <SelectContent>{queues.map((queue) => <SelectItem key={queue.id} value={queue.id}>{queue.name}</SelectItem>)}</SelectContent>
            </Select>
          </div>
          <div className="grid gap-2"><Label htmlFor="route-key">Routing key</Label>
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
          <div className="grid gap-2"><Label htmlFor="publish-key">Routing key</Label>
            <Input id="publish-key" value={key} onChange={(event) => setKey(event.target.value)} spellCheck={false} className="font-mono" /></div>
          <div className="grid gap-2"><Label htmlFor="publish-body">Body (text)</Label>
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
