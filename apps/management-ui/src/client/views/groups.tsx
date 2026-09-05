import { useCallback, useEffect, useState } from "react";
import { HeartPulse, LogIn, LogOut, RotateCcw, Timer, Users } from "lucide-react";
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
import { ConnectionContextLine, CopyId, DetailGrid, EmptyState, LastRefreshed, PageHeader, Section, Skeleton, StateBadge, StatusDot } from "@/components/shared";
import { admin, ApiError, list, newIdempotencyKey } from "@/lib/api";
import { formatDuration } from "@/lib/format";
import type { ConsumerGroupDetail, ConsumerGroupListItem, GroupMember, GroupOffsetsEntry, StreamSummary } from "@/lib/types";

/** Built from streams × per-stream groups; the global endpoint hides the stream ID. */
export function ConsumerGroupsView({ profileId, onOpenGroup }: { profileId: string; onOpenGroup: (streamId: string, groupId: string) => void }) {
  const [rows, setRows] = useState<{ stream: StreamSummary; group: ConsumerGroupListItem }[]>([]);
  const { lastUpdated, error, stale, refresh } = usePolling(
    useCallback(async () => {
      const streams = (await list<StreamSummary>(profileId, "streams")).data;
      const all = await Promise.all(streams.map(async (stream) => {
        const groups = (await list<ConsumerGroupListItem>(profileId, `streams/${stream.id}/consumer-groups`)).data;
        return groups.map((group) => ({ stream, group }));
      }));
      setRows(all.flat());
    }, [profileId]),
    15_000
  );
  const loadedOnce = lastUpdated !== null || error !== null;
  return (
    <div>
      <PageHeader
        title="Consumer groups"
        description="Stream consumer groups with generation, membership, and lag."
        actions={<LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />}
      />
      {error && rows.length === 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {!loadedOnce ? (
        <div className="grid gap-2" aria-busy="true">
          {Array.from({ length: 3 }, (_, index) => <Skeleton key={index} className="h-11 w-full" />)}
          <p className="text-sm text-muted-foreground">Checking consumer groups…</p>
        </div>
      ) : rows.length === 0 ? (
        <div className="border-t border-rule-strong pt-2">
          <EmptyState title="No consumer groups yet." hint="Groups appear when members join via clients or management sessions." />
        </div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow><TableHead>Stream</TableHead><TableHead>Group</TableHead><TableHead className="text-right">Generation</TableHead><TableHead className="text-right">Members</TableHead></TableRow>
          </TableHeader>
          <TableBody>
            {rows.map(({ stream, group }) => (
              <TableRow key={`${stream.id}:${group.group}`} className="cursor-pointer" onClick={() => onOpenGroup(stream.id, group.group)}>
                <TableCell>
                  <a
                    href={`#/c/${encodeURIComponent(profileId)}/groups/${encodeURIComponent(stream.id)}/${encodeURIComponent(group.group)}`}
                    className="text-left font-medium hover:underline"
                    onClick={(event) => event.stopPropagation()}
                  >
                    {stream.name}
                  </a>
                </TableCell>
                <TableCell>
                  <a
                    href={`#/c/${encodeURIComponent(profileId)}/groups/${encodeURIComponent(stream.id)}/${encodeURIComponent(group.group)}`}
                    className="text-left font-mono text-xs hover:underline"
                    onClick={(event) => event.stopPropagation()}
                  >
                    {group.group}
                  </a>
                </TableCell>
                <TableCell className="text-right tabular-nums">g-{group.generation}</TableCell>
                <TableCell className="text-right tabular-nums">{group.active_member_count}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}

export function GroupDetailView({ profileId, streamId, groupId, onBack }: { profileId: string; streamId: string; groupId: string; onBack: () => void }) {
  const [detail, setDetail] = useState<ConsumerGroupDetail | null>(null);
  const [offsets, setOffsets] = useState<GroupOffsetsEntry[]>([]);
  const [members, setMembers] = useState<GroupMember[]>([]);
  const [etag, setEtag] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [commitTarget, setCommitTarget] = useState<GroupOffsetsEntry | null>(null);
  const [resetOpen, setResetOpen] = useState(false);
  const [session, setSession] = useState<{ session_id: string; lease_ms: number; assigned: number[]; joinedAt: number } | null>(null);
  const [joinOpen, setJoinOpen] = useState(false);

  const loader = useCallback(async () => {
    const [detailResponse, offsetsResponse] = await Promise.all([
      admin<{ data: ConsumerGroupDetail }>(profileId, `streams/${streamId}/consumer-groups/${groupId}`),
      list<GroupOffsetsEntry>(profileId, `streams/${streamId}/consumer-groups/${groupId}/offsets`)
    ]);
    setDetail(detailResponse.json.data);
    setEtag(detailResponse.etag);
    setOffsets(offsetsResponse.data);
    try { setMembers((await list<GroupMember>(profileId, `streams/${streamId}/consumer-groups/${groupId}/members`)).data); } catch { setMembers([]); }
  }, [profileId, streamId, groupId]);
  const { lastUpdated, error: pollError, stale, refresh } = usePolling(loader, 10_000);

  const viewError = error ?? pollError;

  return (
    <div>
      <PageHeader
        title={detail?.group ?? groupId}
        breadcrumb={
          <button type="button" className="text-sm text-muted-foreground hover:text-foreground hover:underline" onClick={onBack}>
            Consumer groups
          </button>
        }
        description={<span className="font-mono text-xs">stream {streamId} · group {groupId}</span>}
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <Button variant="outline" size="sm" onClick={() => setJoinOpen(true)}><LogIn className="size-4" />Management session</Button>
            <Button variant="outline" size="sm" onClick={() => setResetOpen(true)}><RotateCcw className="size-4" />Reset offsets</Button>
          </>
        }
      />
      {viewError && <ErrorBanner error={viewError} onRetry={refresh} className="mb-4" />}
      {detail && (
        <>
          <div className="grid grid-cols-1 gap-6 lg:grid-cols-[minmax(280px,1fr)_2fr] lg:gap-8">
            <div className="border-t border-rule-strong pt-3">
              <h2 className="mb-3 text-base font-semibold tracking-[-0.015em]">Group</h2>
              <DetailGrid rows={[
                { label: "Group", value: detail.group },
                { label: "Group ID", value: <CopyId id={groupId} />, mono: true },
                { label: "Generation (ETag)", value: etag ?? `g-${detail.generation}`, mono: true },
                { label: "Active members", value: detail.active_member_count }
              ]} />
            </div>
            <div className="border-t border-rule-strong pt-3">
              <h2 className="mb-3 text-base font-semibold tracking-[-0.015em]">Offsets &amp; lag</h2>
              <Table>
                <TableHeader><TableRow><TableHead>Partition</TableHead><TableHead className="text-right">Offset</TableHead><TableHead className="text-right">High-water</TableHead><TableHead className="text-right">Lag</TableHead><TableHead /></TableRow></TableHeader>
                <TableBody>
                  {offsets.map((entry) => (
                    <TableRow key={entry.partition}>
                      <TableCell className="font-mono">{entry.partition}</TableCell>
                      <TableCell className="text-right tabular-nums">{entry.offset}</TableCell>
                      <TableCell className="text-right tabular-nums">{entry.high_water_offset ?? "—"}</TableCell>
                      <TableCell className="text-right tabular-nums">{entry.lag}</TableCell>
                      <TableCell className="text-right">
                        <Button size="sm" variant="outline" onClick={() => setCommitTarget(entry)}>Commit</Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
          <div className="mt-6 border-t border-rule-strong pt-3">
            <h2 className="mb-3 flex items-center gap-2 text-base font-semibold tracking-[-0.015em]"><Users className="size-4 text-muted-foreground" />Members <span className="text-sm font-normal text-muted-foreground">(privacy-safe snapshot)</span></h2>
            {members.length === 0
              ? <p className="text-sm text-muted-foreground">No active members.</p>
              : (
                <ul className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                  {members.map((member) => (
                    <li key={member.member_id} className="flex items-center gap-3 border px-3 py-2 text-sm">
                      <span className="font-mono text-xs">{member.member_id}</span>
                      <Badge variant="neutral" className="text-xs">{member.assigned_partition_count} partition(s)</Badge>
                      <span className="ml-auto flex items-center gap-1 text-xs text-muted-foreground"><Timer className="size-3" />{formatDuration(member.lease_remaining_ms)}</span>
                    </li>
                  ))}
                </ul>
              )}
          </div>
        </>
      )}
      {session && (
        <div className="mt-6">
          <SessionWorkspace
            profileId={profileId} streamId={streamId} groupId={groupId} session={session}
            onLeft={() => { setSession(null); refresh(); }} onRefreshed={refresh}
          />
        </div>
      )}

      {commitTarget && detail && (
        <CommitOffsetDialog
          open onOpenChange={() => setCommitTarget(null)} profileId={profileId} streamId={streamId} groupId={groupId}
          entry={commitTarget} generationEtag={etag ?? `g-${detail.generation}`} onDone={refresh}
        />
      )}
      {resetOpen && detail && (
        <ResetOffsetsDialog
          open onOpenChange={() => setResetOpen(false)} profileId={profileId} streamId={streamId} groupId={groupId}
          groupLabel={detail.group} activeMembers={detail.active_member_count} generationEtag={etag ?? `g-${detail.generation}`}
          onDone={refresh}
        />
      )}
      {joinOpen && detail && (
        <JoinSessionDialog
          open onOpenChange={() => setJoinOpen(false)} profileId={profileId} streamId={streamId} groupId={groupId}
          groupLabel={detail.group} onJoined={(joined) => { setSession(joined); setJoinOpen(false); refresh(); }}
        />
      )}
    </div>
  );
}

function CommitOffsetDialog({ open, onOpenChange, profileId, streamId, groupId, entry, generationEtag, onDone }: {
  open: boolean; onOpenChange: (open: boolean) => void; profileId: string; streamId: string; groupId: string;
  entry: GroupOffsetsEntry; generationEtag: string; onDone: () => void;
}) {
  const [offset, setOffset] = useState(String(entry.offset));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const submit = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `streams/${streamId}/consumer-groups/${groupId}/offsets/${entry.partition}`, {
        method: "PUT", idempotencyKey: newIdempotencyKey(), ifMatch: generationEtag, body: { offset: Math.max(0, Number(offset) || 0) }
      });
      toast.success(`Committed offset for partition ${entry.partition}`);
      onOpenChange(false);
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader><DialogTitle>Commit offset — partition {entry.partition}</DialogTitle>
          <DialogDescription>Administrative commit with the current generation {generationEtag}. It never joins or changes membership.</DialogDescription></DialogHeader>
        <div className="grid gap-1.5 py-2">
          <Label htmlFor="commit-offset">Offset</Label>
          <Input id="commit-offset" inputMode="numeric" value={offset} onChange={(event) => setOffset(event.target.value)} className="font-mono" />
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={() => void submit()} disabled={busy}>{busy ? "Committing…" : "Commit"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ResetOffsetsDialog({ open, onOpenChange, profileId, streamId, groupId, groupLabel, activeMembers, generationEtag, onDone }: {
  open: boolean; onOpenChange: (open: boolean) => void; profileId: string; streamId: string; groupId: string;
  groupLabel: string; activeMembers: number; generationEtag: string; onDone: () => void;
}) {
  const [strategy, setStrategy] = useState("earliest");
  const [offset, setOffset] = useState("0");
  const [delta, setDelta] = useState("0");
  const [force, setForce] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [confirmError, setConfirmError] = useState<Error | null>(null);
  const submit = async () => {
    setBusy(true); setConfirmError(null);
    try {
      await admin(profileId, `streams/${streamId}/consumer-groups/${groupId}:reset-offsets`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), ifMatch: generationEtag, confirm: groupId,
        body: {
          strategy,
          ...(strategy === "absolute" ? { offset: Math.max(0, Number(offset) || 0) } : {}),
          ...(strategy === "relative" ? { delta: Number(delta) || 0 } : {}),
          ...(force ? { force: true } : {})
        }
      });
      toast.success("Offsets reset for every partition");
      setConfirmOpen(false); onOpenChange(false);
      onDone();
    } catch (reason) { setConfirmError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader><DialogTitle>Reset offsets</DialogTitle>
            <DialogDescription>Resets every partition. Active groups require force and exact confirmation.</DialogDescription></DialogHeader>
          <div className="grid gap-4 py-2">
            <div className="grid gap-1.5"><Label htmlFor="reset-strategy">Strategy</Label>
              <Select value={strategy} onValueChange={setStrategy}>
                <SelectTrigger id="reset-strategy"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="earliest">earliest</SelectItem>
                  <SelectItem value="latest">latest</SelectItem>
                  <SelectItem value="absolute">absolute</SelectItem>
                  <SelectItem value="relative">relative</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {strategy === "absolute" && <div className="grid gap-1.5"><Label htmlFor="reset-offset">Offset</Label><Input id="reset-offset" inputMode="numeric" value={offset} onChange={(event) => setOffset(event.target.value)} className="font-mono" /></div>}
            {strategy === "relative" && <div className="grid gap-1.5"><Label htmlFor="reset-delta">Delta (negative to rewind)</Label><Input id="reset-delta" inputMode="numeric" value={delta} onChange={(event) => setDelta(event.target.value)} className="font-mono" /></div>}
            {activeMembers > 0 && (
              <label className="flex items-center gap-2 text-sm">
                <Switch checked={force} onCheckedChange={setForce} aria-label={`Force reset with ${activeMembers} active members`} /> force with {activeMembers} active member(s)
              </label>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
            <Button variant="destructive" onClick={() => setConfirmOpen(true)}>Continue…</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <ConfirmDestructive
        open={confirmOpen} onOpenChange={setConfirmOpen} confirmId={groupId} inFlight={busy}
        title={`Reset offsets for ${groupLabel}`}
        description={`Applies ${strategy} to every partition of this group.`}
        context={<ConnectionContextLine profileId={profileId} />}
        error={confirmError}
        confirmLabel="Reset offsets"
        onConfirm={() => void submit()}
      />
    </>
  );
}

function JoinSessionDialog({ open, onOpenChange, profileId, streamId, groupId, groupLabel, onJoined }: {
  open: boolean; onOpenChange: (open: boolean) => void; profileId: string; streamId: string; groupId: string; groupLabel: string;
  onJoined: (session: { session_id: string; lease_ms: number; assigned: number[]; joinedAt: number }) => void;
}) {
  const [leaseMs, setLeaseMs] = useState("30000");
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [confirmError, setConfirmError] = useState<Error | null>(null);
  const submit = async () => {
    setBusy(true); setConfirmError(null);
    try {
      const response = await admin<{ data: { session_id: string; lease_ms: number; assigned_partitions: number[] } }>(
        profileId, `streams/${streamId}/consumer-groups/${groupId}/sessions`,
        { method: "POST", idempotencyKey: newIdempotencyKey(), confirm: groupId, body: { lease_ms: Number(leaseMs) || 30000 } }
      );
      onJoined({ session_id: response.json.data.session_id, lease_ms: response.json.data.lease_ms, assigned: response.json.data.assigned_partitions ?? [], joinedAt: Date.now() });
      toast.success("Management session joined");
    } catch (reason) { setConfirmError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };
  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader><DialogTitle>Join via management session</DialogTitle>
            <DialogDescription>Joining may rebalance active members of {groupLabel}. The session lease expires server-side regardless of this browser.</DialogDescription></DialogHeader>
          <div className="grid gap-4 py-2">
            <div className="grid gap-1.5"><Label htmlFor="session-lease">Lease ms</Label><Input id="session-lease" inputMode="numeric" value={leaseMs} onChange={(event) => setLeaseMs(event.target.value)} className="font-mono" /></div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
            <Button onClick={() => setConfirmOpen(true)}>Continue…</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <ConfirmDestructive
        open={confirmOpen} onOpenChange={setConfirmOpen} confirmId={groupId} inFlight={busy}
        title="Join consumer group"
        description="Confirms a management session join that can rebalance members."
        context={<ConnectionContextLine profileId={profileId} />}
        error={confirmError}
        confirmLabel="Join group"
        onConfirm={() => void submit()}
      />
    </>
  );
}

function SessionWorkspace({ profileId, streamId, groupId, session, onLeft, onRefreshed }: {
  profileId: string; streamId: string; groupId: string;
  session: { session_id: string; lease_ms: number; assigned: number[]; joinedAt: number };
  onLeft: () => void; onRefreshed: () => void;
}) {
  const [remaining, setRemaining] = useState(session.lease_ms);
  const [state, setState] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [busy, setBusy] = useState(false);
  const [offset, setOffset] = useState("0");
  const [maxRecords, setMaxRecords] = useState("10");
  const [records, setRecords] = useState<{ offset: number; body?: { data: string } | undefined }[]>([]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      setRemaining(session.lease_ms - (Date.now() - session.joinedAt));
    }, 500);
    return () => window.clearInterval(timer);
  }, [session]);

  const heartbeat = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `streams/${streamId}/consumer-groups/${groupId}/sessions/${session.session_id}:heartbeat`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: {}
      });
      toast.success("Lease refreshed");
      onRefreshed();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const leave = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `streams/${streamId}/consumer-groups/${groupId}/sessions/${session.session_id}:leave`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: {}
      });
      toast.success("Left the group; assignments released");
      onLeft();
    } catch (reason) {
      setError(reason instanceof Error ? reason : new Error(String(reason)));
    } finally { setBusy(false); }
  };

  const fetchRecords = async () => {
    setBusy(true); setError(null); setState(null);
    try {
      const response = await list<{ offset: number; body?: { data: string } }>(
        profileId,
        `streams/${streamId}/consumer-groups/${groupId}/sessions/${session.session_id}/records?partition=${session.assigned[0] ?? 0}&offset=${Number(offset) || 0}&max_records=${Number(maxRecords) || 10}&max_bytes=131072`
      );
      setRecords(response.data);
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 410) setState("expired");
      setError(reason instanceof Error ? reason : new Error(String(reason)));
    } finally { setBusy(false); }
  };

  const expired = remaining <= 0 || state === "expired";
  return (
    <Section title="Management session">
      <div className={`grid gap-3 border bg-card p-4 ${expired ? "border-warning/50" : ""}`}>
        <div className="flex flex-wrap items-center gap-2">
          <CopyId id={session.session_id} />
          <StateBadge state={expired ? "expired" : "active"} />
          <Badge variant="outline" className="text-xs">lease {expired ? "expired" : formatDuration(remaining)}</Badge>
          <Badge variant="outline" className="text-xs">assigned: {session.assigned.join(", ") || "none"}</Badge>
          <div className="ml-auto flex gap-2">
            <Button size="sm" variant="outline" onClick={() => void heartbeat()} disabled={busy || expired}>Heartbeat</Button>
            <Button size="sm" variant="destructive" onClick={() => void leave()} disabled={busy || expired}><LogOut className="size-3.5" />Leave</Button>
          </div>
        </div>
        <p className="text-xs text-muted-foreground">Correctness relies on server lease expiry, not browser cleanup. Fetching reads only assigned partitions.</p>
        <div className="flex flex-wrap items-end gap-2">
          <div className="grid gap-1.5"><Label htmlFor="session-offset">Offset</Label>
            <Input id="session-offset" value={offset} onChange={(event) => setOffset(event.target.value)} inputMode="numeric" className="w-28 font-mono" /></div>
          <div className="grid gap-1.5"><Label htmlFor="session-max">Max records</Label>
            <Input id="session-max" value={maxRecords} onChange={(event) => setMaxRecords(event.target.value)} inputMode="numeric" className="w-24" /></div>
          <Button size="sm" variant="outline" onClick={() => void fetchRecords()} disabled={busy || expired}>Fetch assigned</Button>
        </div>
        {error && <ErrorBanner error={error} />}
        {records.length > 0 && (
          <ul className="grid gap-1">
            {records.map((record) => (
              <li key={record.offset} className="flex gap-3 border px-2 py-1 font-mono text-xs">
                <span className="text-muted-foreground">offset {record.offset}</span>
                <span className="truncate">{record.body?.data ?? "(no body)"}</span>
              </li>
            ))}
          </ul>
        )}
        {expired && <p className="inline-flex items-center gap-1.5 text-xs text-warning"><StatusDot tone="warning" />Session lease expired — join again to fetch or commit.</p>}
      </div>
    </Section>
  );
}
