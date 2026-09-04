import { useCallback, useMemo, useState } from "react";
import { Plus, Trash2, Layers, KeyRound } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { BinaryValue, encodeDraft } from "@/components/binary-value";
import { ConfirmDestructive } from "@/components/confirm";
import { ErrorBanner } from "@/components/error-banner";
import { CursorPager, CopyId, LastRefreshed, PageHeader } from "@/components/shared";
import { admin, ApiError, list, newIdempotencyKey } from "@/lib/api";
import { base64FromUtf8, isValidB64uId, nameFromId, idFromName } from "@/lib/codec";
import { formatBytes, formatRelative } from "@/lib/format";
import type { KeyspaceEntry, KeyspaceEntrySummary, KeyspaceInfo } from "@/lib/types";

type ExpiresFilter = "all" | "present" | "none";

export function KeyspaceView({ profileId }: { profileId: string }) {
  const [keyspace, setKeyspace] = useState<KeyspaceInfo | null>(null);
  const [entries, setEntries] = useState<KeyspaceEntrySummary[]>([]);
  const [meta, setMeta] = useState<{ nextCursor: string | null; weak: boolean } | null>(null);
  const [backStack, setBackStack] = useState<string[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [prefix, setPrefix] = useState("");
  const [expires, setExpires] = useState<ExpiresFilter>("all");
  const [pageError, setPageError] = useState<Error | null>(null);
  const [putOpen, setPutOpen] = useState(false);
  const [batchOpen, setBatchOpen] = useState(false);
  const [claimsOpen, setClaimsOpen] = useState(false);
  const [selected, setSelected] = useState<KeyspaceEntry | null>(null);

  const prefixQuery = useMemo(() => (prefix.trim().length > 0 ? `&prefix=${encodeURIComponent(idFromName(prefix.trim()))}` : ""), [prefix]);
  const expiresQuery = expires === "all" ? "" : `&expires=${expires}`;
  const basePath = useCallback((cursorValue: string | null) =>
    `keyspaces/default/entries?limit=50${prefixQuery}${expiresQuery}${cursorValue ? `&cursor=${encodeURIComponent(cursorValue)}` : ""}`,
  [prefixQuery, expiresQuery]);

  const loader = useCallback(async () => {
    const response = await list<KeyspaceEntrySummary>(profileId, basePath(cursor));
    setEntries(response.data);
    setMeta({ nextCursor: response.meta?.next_cursor ?? null, weak: response.meta?.weakly_consistent ?? false });
    const aggregate = await admin<{ data: KeyspaceInfo }>(profileId, "keyspaces/default");
    setKeyspace(aggregate.json.data);
  }, [profileId, basePath, cursor]);

  const { lastUpdated, error, refresh } = usePolling(loader, 20_000);
  const resetToFirstPage = () => { setBackStack([]); setCursor(null); refresh(); };

  return (
    <div>
      <PageHeader
        title="Keyspace"
        description="Default keyspace entries. Listing is metadata-only and weakly consistent; cursors expire after ten minutes and on server restart."
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => setClaimsOpen(true)}><KeyRound className="size-4 mr-1" />Claims</Button>
            <Button variant="outline" size="sm" onClick={() => setBatchOpen(true)}><Layers className="size-4 mr-1" />Batch</Button>
            <Button size="sm" onClick={() => setPutOpen(true)}><Plus className="size-4 mr-1" />Put entry</Button>
          </>
        }
      />
      {keyspace && (
        <div className="grid gap-3 sm:grid-cols-4 mb-4">
          <MiniStat label="entries" value={keyspace.entry_count} />
          <MiniStat label="live bytes" value={formatBytes(keyspace.live_bytes)} />
          <MiniStat label="expired" value={keyspace.expired_count} />
          <MiniStat label="revision" value={`k-${keyspace.revision}`} mono />
        </div>
      )}

      <div className="flex flex-wrap items-center gap-2 mb-3">
        <Input value={prefix} onChange={(event) => setPrefix(event.target.value)} placeholder="Prefix filter (plain text)" className="w-56" />
        <Select value={expires} onValueChange={(value) => { setExpires(value as ExpiresFilter); setBackStack([]); setCursor(null); }}>
          <SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All entries</SelectItem>
            <SelectItem value="present">With expiry</SelectItem>
            <SelectItem value="none">Without expiry</SelectItem>
          </SelectContent>
        </Select>
        <Button variant="ghost" size="sm" onClick={resetToFirstPage}>Apply</Button>
        <div className="ml-auto"><LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} /></div>
      </div>

      {error && entries.length === 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Key</TableHead>
                <TableHead>Entry ID</TableHead>
                <TableHead className="text-right">Size</TableHead>
                <TableHead className="text-right">Expires</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {entries.map((entry) => (
                <TableRow key={entry.id} className="cursor-pointer" onClick={() => void openEntry(profileId, entry.id, setSelected, setPageError)}>
                  <TableCell className="font-medium">{entry.key}</TableCell>
                  <TableCell><CopyId id={entry.id} /></TableCell>
                  <TableCell className="text-right tabular-nums">{formatBytes(entry.value_size)}</TableCell>
                  <TableCell className="text-right text-muted-foreground">
                    {entry.expires_at === null ? "never" : `${formatRelative(entry.expires_at * 1000)}${entry.remaining_ttl_ms !== null ? ` (${Math.round((entry.remaining_ttl_ms ?? 0) / 1000)}s)` : ""}`}
                  </TableCell>
                </TableRow>
              ))}
              {entries.length === 0 && <TableRow><TableCell colSpan={4} className="text-center text-muted-foreground py-8">No entries match.</TableCell></TableRow>}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
      <div className="mt-3">
        <CursorPager
          nextCursor={meta?.nextCursor}
          backStack={backStack}
          weaklyConsistent={meta?.weak}
          onBack={() => {
            const previous = backStack[backStack.length - 1];
            if (previous === undefined) return;
            setBackStack((stack) => stack.slice(0, -1));
            setCursor(previous);
          }}
          onNext={() => {
            if (!meta?.nextCursor) return;
            setBackStack((stack) => [...stack, cursor ?? ""]);
            setCursor(meta.nextCursor);
          }}
        />
      </div>
      {pageError && <ErrorBanner error={pageError} className="mt-3" />}

      <PutEntryDialog open={putOpen} onOpenChange={setPutOpen} profileId={profileId} onDone={() => { resetToFirstPage(); }} />
      <BatchDialog open={batchOpen} onOpenChange={setBatchOpen} profileId={profileId} onDone={resetToFirstPage} />
      <ClaimsDialog open={claimsOpen} onOpenChange={setClaimsOpen} profileId={profileId} />
      <EntryDialog entry={selected} onClose={() => setSelected(null)} profileId={profileId} onDeleted={resetToFirstPage} />
    </div>
  );
}
async function openEntry(profileId: string, id: string, setSelected: (entry: KeyspaceEntry) => void, setError: (error: Error) => void) {
  try {
    const response = await admin<{ data: KeyspaceEntry }>(profileId, `keyspaces/default/entries/${id}`);
    setSelected(response.json.data);
  } catch (reason) {
    setError(reason instanceof Error ? reason : new Error(String(reason)));
  }
}

function MiniStat({ label, value, mono = false }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="rounded-xl border bg-card px-4 py-3">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={`text-lg font-semibold tabular-nums ${mono ? "font-mono text-base" : ""}`}>{value}</p>
    </div>
  );
}

type ValueMode = "text" | "json" | "base64";

export function PutEntryDialog({ open, onOpenChange, profileId, existingKey, onDone }: {
  open: boolean; onOpenChange: (open: boolean) => void; profileId: string; existingKey?: string; onDone: () => void;
}) {
  const [keyText, setKeyText] = useState(existingKey ?? "");
  const [useRawId, setUseRawId] = useState(false);
  const [mode, setMode] = useState<ValueMode>("text");
  const [raw, setRaw] = useState("");
  const [ttlSeconds, setTtlSeconds] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const draft = encodeDraft(mode, raw);
  const entryId = useRawId ? keyText.trim() : keyText.trim().length > 0 ? idFromName(keyText.trim()) : "";
  const idValid = isValidB64uId(entryId);

  const submit = async () => {
    if (!idValid || draft.error || draft.base64.length === 0) return;
    setBusy(true); setError(null);
    try {
      await admin(profileId, `keyspaces/default/entries/${entryId}`, {
        method: "PUT",
        idempotencyKey: newIdempotencyKey(),
        body: { value: draft.base64, ...(ttlSeconds.trim().length > 0 ? { ttl_ms: Math.round(Number(ttlSeconds) * 1000) } : {}) }
      });
      toast.success(`Put ${keyText.trim()}`);
      onOpenChange(false);
      setRaw(""); setTtlSeconds("");
      onDone();
    } catch (reason) {
      setError(reason instanceof Error ? reason : new Error(String(reason)));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Put entry</DialogTitle>
          <DialogDescription>Durable put through the Management API. Values are stored as bytes; the byte count is exact.</DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="grid gap-2">
            <div className="flex items-center justify-between">
              <Label htmlFor="entry-key">Key</Label>
              <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <Switch checked={useRawId} onCheckedChange={setUseRawId} className="scale-90" /> raw b64u ID
              </label>
            </div>
            <Input id="entry-key" value={keyText} onChange={(event) => setKeyText(event.target.value)} placeholder="greeting" spellCheck={false} />
            {entryId.length > 0 && idValid && <p className="text-xs font-mono text-muted-foreground">{entryId}</p>}
            {entryId.length > 0 && !idValid && <p className="text-xs text-destructive">Not a valid opaque entry ID.</p>}
          </div>
          <Tabs value={mode} onValueChange={(value) => setMode(value as ValueMode)}>
            <TabsList><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger><TabsTrigger value="base64">Base64</TabsTrigger></TabsList>
          </Tabs>
          <Textarea value={raw} onChange={(event) => setRaw(event.target.value)} rows={4} spellCheck={false} placeholder={mode === "base64" ? "aGVsbG8=" : "hello"} className="font-mono text-xs" />
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <span>{draft.bytes} bytes</span>
            {draft.error && <span className="text-destructive">{draft.error}</span>}
            <Label className="ml-auto flex items-center gap-2 font-normal">TTL seconds
              <Input value={ttlSeconds} onChange={(event) => setTtlSeconds(event.target.value)} inputMode="numeric" placeholder="—" className="w-24 h-8" />
            </Label>
          </div>
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={() => void submit()} disabled={busy || !idValid || !draft.base64 || Boolean(draft.error)}>{busy ? "Writing…" : "Put durably"}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EntryDialog({ entry, onClose, profileId, onDeleted }: { entry: KeyspaceEntry | null; onClose: () => void; profileId: string; onDeleted: () => void }) {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  if (!entry) return null;
  const decodedName = nameFromId(entry.id);

  const remove = async () => {
    setBusy(true); setError(null);
    try {
      await admin(profileId, `keyspaces/default/entries/${entry.id}`, { method: "DELETE", idempotencyKey: newIdempotencyKey(), body: {} });
      toast.success(`Deleted ${decodedName ?? entry.id}`);
      setConfirmOpen(false);
      onClose();
      onDeleted();
    } catch (reason) {
      setError(reason instanceof Error ? reason : new Error(String(reason)));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog open onOpenChange={(open) => { if (!open) onClose(); }}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle className="break-all">{decodedName ?? entry.key}</DialogTitle>
          <DialogDescription className="font-mono text-xs break-all">{entry.id}</DialogDescription>
        </DialogHeader>
        <div className="grid gap-3 py-2">
          <BinaryValue value={entry.value} />
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="destructive" onClick={() => setConfirmOpen(true)}><Trash2 className="size-4 mr-1" />Delete</Button>
          <Button variant="outline" onClick={onClose}>Close</Button>
        </DialogFooter>
        <ConfirmDestructive
          open={confirmOpen} onOpenChange={setConfirmOpen} confirmId={entry.id} inFlight={busy}
          title="Delete keyspace entry"
          description="Removes this exact entry durably. The decoded name is only a preview — the opaque ID is authoritative."
          onConfirm={() => void remove()}
        />
      </DialogContent>
    </Dialog>
  );
}

function BatchDialog({ open, onOpenChange, profileId, onDone }: { open: boolean; onOpenChange: (open: boolean) => void; profileId: string; onDone: () => void }) {
  const [tab, setTab] = useState("put");
  const [putLines, setPutLines] = useState("key=value\nkey2=value2");
  const [deleteLines, setDeleteLines] = useState("");
  const [getLines, setGetLines] = useState("");
  const [ttlSeconds, setTtlSeconds] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [result, setResult] = useState<string | null>(null);

  const parsePutLine = (line: string): { entry_id: string; value: string } | { error: string } => {
    const index = line.indexOf("=");
    if (index <= 0) return { error: "expected key=value" };
    const key = line.slice(0, index).trim();
    const value = line.slice(index + 1).trim();
    return { entry_id: idFromName(key), value: base64FromUtf8(value) };
  };

  const runPut = async () => {
    const parsed = putLines.split("\n").map((line) => line.trim()).filter(Boolean).map(parsePutLine);
    const invalid = parsed.find((item) => "error" in item);
    if (invalid && "error" in invalid) { setError(new ApiError("validation_failed", invalid.error, 400)); return; }
    setBusy(true); setError(null); setResult(null);
    try {
      const response = await admin<{ data: unknown }>(profileId, "keyspaces/default/entries:batch-put", {
        method: "POST", idempotencyKey: newIdempotencyKey(),
        body: { entries: parsed, ...(ttlSeconds.trim().length > 0 ? {} : {}) }
      });
      setResult(JSON.stringify(response.json, null, 2));
      toast.success("Batch put applied (non-atomic)");
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const runDelete = async () => {
    const ids = deleteLines.split("\n").map((line) => line.trim()).filter(Boolean).map((line) => isValidB64uId(line) ? line : idFromName(line));
    if (ids.length === 0) return;
    setBusy(true); setError(null); setResult(null);
    try {
      const response = await admin<{ data: unknown }>(profileId, "keyspaces/default/entries:batch-delete", {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: { entry_ids: ids }
      });
      setResult(JSON.stringify(response.json, null, 2));
      toast.success("Batch delete applied (non-atomic)");
      onDone();
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const runGet = async () => {
    const ids = getLines.split("\n").map((line) => line.trim()).filter(Boolean).map((line) => isValidB64uId(line) ? line : idFromName(line));
    if (ids.length === 0) return;
    setBusy(true); setError(null); setResult(null);
    try {
      const response = await admin<{ data: unknown }>(profileId, "keyspaces/default/entries:batch-get", {
        method: "POST", body: { entry_ids: ids }
      });
      setResult(JSON.stringify(response.json, null, 2));
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Batch operations</DialogTitle>
          <DialogDescription>Up to 100 exact mutations per batch. Current batches validate everything first and explicitly report atomic: false.</DialogDescription>
        </DialogHeader>
        <Tabs value={tab} onValueChange={setTab}>
          <TabsList><TabsTrigger value="put">Batch put</TabsTrigger><TabsTrigger value="delete">Batch delete</TabsTrigger><TabsTrigger value="get">Batch get</TabsTrigger></TabsList>
          <TabsContent value="put" className="grid gap-3 pt-2">
            <Textarea value={putLines} onChange={(event) => setPutLines(event.target.value)} rows={5} className="font-mono text-xs" placeholder={"key=value\nkey2=value2"} spellCheck={false} />
            <div className="flex items-center gap-2">
              <Input value={ttlSeconds} onChange={(event) => setTtlSeconds(event.target.value)} placeholder="TTL seconds (optional)" inputMode="numeric" className="w-48 h-8" />
              <Button onClick={() => void runPut()} disabled={busy}>{busy ? "Applying…" : "Apply batch put"}</Button>
            </div>
          </TabsContent>
          <TabsContent value="delete" className="grid gap-3 pt-2">
            <Textarea value={deleteLines} onChange={(event) => setDeleteLines(event.target.value)} rows={5} className="font-mono text-xs" placeholder="one key or b64u:… ID per line" spellCheck={false} />
            <Button variant="destructive" onClick={() => void runDelete()} disabled={busy || deleteLines.trim().length === 0}>{busy ? "Deleting…" : "Delete batch"}</Button>
          </TabsContent>
          <TabsContent value="get" className="grid gap-3 pt-2">
            <Textarea value={getLines} onChange={(event) => setGetLines(event.target.value)} rows={5} className="font-mono text-xs" placeholder="one key or b64u:… ID per line" spellCheck={false} />
            <Button variant="outline" onClick={() => void runGet()} disabled={busy || getLines.trim().length === 0}>{busy ? "Reading…" : "Read batch"}</Button>
          </TabsContent>
        </Tabs>
        {error && <ErrorBanner error={error} className="mt-2" />}
        {result && <pre className="font-mono text-xs bg-muted rounded-md p-2 max-h-48 overflow-auto mt-2">{result}</pre>}
      </DialogContent>
    </Dialog>
  );
}

function ClaimsDialog({ open, onOpenChange, profileId }: { open: boolean; onOpenChange: (open: boolean) => void; profileId: string }) {
  const [key, setKey] = useState("");
  const [leaseMs, setLeaseMs] = useState("30000");
  const [claim, setClaim] = useState<{ claim_id: string; lease?: number } | null>(null);
  const [claimState, setClaimState] = useState<string | null>(null);
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const entryId = key.trim().length > 0 ? idFromName(key.trim()) : "";

  const createClaim = async () => {
    setBusy(true); setError(null);
    try {
      const response = await admin<{ data: { claim_id: string } }>(profileId, "keyspaces/default/claims", {
        method: "POST", idempotencyKey: newIdempotencyKey(),
        body: { entry_id: entryId, ...(leaseMs ? { lease_ms: Number(leaseMs) } : {}) }
      });
      setClaim(response.json.data); setClaimState("active");
      toast.success(`Claim ${response.json.data.claim_id} acquired`);
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const completeClaim = async () => {
    if (!claim) return;
    setBusy(true); setError(null);
    try {
      await admin(profileId, `keyspaces/default/claims/${claim.claim_id}:complete`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: { value: base64FromUtf8(value) }
      });
      toast.success("Claim completed with value");
      setClaim(null); setClaimState(null);
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const releaseClaim = async () => {
    if (!claim) return;
    setBusy(true); setError(null);
    try {
      await admin(profileId, `keyspaces/default/claims/${claim.claim_id}:release`, { method: "POST", idempotencyKey: newIdempotencyKey(), body: {} });
      toast.success("Claim released");
      setClaim(null); setClaimState(null);
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  const getOrRefresh = async () => {
    setBusy(true); setError(null);
    try {
      const response = await admin<{ data: Record<string, unknown> }>(profileId, `keyspaces/default/entries/${entryId}:get-or-refresh`, {
        method: "POST", idempotencyKey: newIdempotencyKey(), body: { ...(leaseMs ? { lease_ms: Number(leaseMs) } : {}) }
      });
      const outcome = response.status === 201 ? "claimed" : "value";
      setClaimState(JSON.stringify(response.json.data, null, 2));
      toast.success(`get-or-refresh: ${outcome}`);
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>Single-flight claims</DialogTitle>
          <DialogDescription>Advanced diagnostics. A claim leases native ownership of one key; expired or completed claims are never reusable.</DialogDescription>
        </DialogHeader>
        <div className="grid gap-3 py-2">
          <div className="flex gap-2">
            <Input value={key} onChange={(event) => setKey(event.target.value)} placeholder="Key" spellCheck={false} />
            <Input value={leaseMs} onChange={(event) => setLeaseMs(event.target.value)} inputMode="numeric" placeholder="lease ms" className="w-28" />
          </div>
          <p className="text-xs font-mono text-muted-foreground">{entryId || "b64u:…"}</p>
          <div className="flex flex-wrap gap-2">
            <Button size="sm" onClick={() => void createClaim()} disabled={busy || !entryId}>Create claim</Button>
            <Button size="sm" variant="outline" onClick={() => void getOrRefresh()} disabled={busy || !entryId}>Get or refresh</Button>
          </div>
          {claim && (
            <div className="rounded-lg border p-3 grid gap-2">
              <p className="font-mono text-xs">{claim.claim_id}</p>
              <Input value={value} onChange={(event) => setValue(event.target.value)} placeholder="Value (text) to complete with" spellCheck={false} />
              <div className="flex gap-2">
                <Button size="sm" onClick={() => void completeClaim()} disabled={busy}>Complete</Button>
                <Button size="sm" variant="outline" onClick={() => void releaseClaim()} disabled={busy}>Release</Button>
              </div>
            </div>
          )}
          {error && <ErrorBanner error={error} />}
          {claimState && claim?.claim_id === undefined && <pre className="font-mono text-xs bg-muted rounded-md p-2 max-h-40 overflow-auto">{claimState}</pre>}
        </div>
      </DialogContent>
    </Dialog>
  );
}
