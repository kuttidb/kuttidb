import { useCallback, useMemo, useState } from "react";
import { Plus, Trash2, Layers, KeyRound } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Button } from "@/components/ui/button";
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
import { ConnectionContextLine, CursorPager, CopyId, LastRefreshed, PageHeader, Section, Skeleton } from "@/components/shared";
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

  const { lastUpdated, error, stale, refresh } = usePolling(loader, 20_000);
  const resetToFirstPage = () => { setBackStack([]); setCursor(null); refresh(); };
  const loadedOnce = lastUpdated !== null || error !== null;
  const filtered = prefix.trim().length > 0 || expires !== "all";

  return (
    <div>
      <PageHeader
        title="Keyspace"
        description="Default keyspace entries. Listing is metadata-only and weakly consistent; cursors expire after ten minutes and on server restart."
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <Button variant="outline" size="sm" onClick={() => setClaimsOpen(true)}><KeyRound className="size-4" />Claims</Button>
            <Button variant="outline" size="sm" onClick={() => setBatchOpen(true)}><Layers className="size-4" />Batch</Button>
            <Button size="sm" onClick={() => setPutOpen(true)}><Plus className="size-4" />Put entry</Button>
          </>
        }
      />
      {keyspace && (
        <div className="mb-4 border-y border-rule-strong">
          <dl className="flex flex-wrap gap-x-8 gap-y-1 py-3 text-sm">
            <div className="flex gap-2"><dt className="text-muted-foreground">Entries</dt><dd className="font-medium tabular-nums">{keyspace.entry_count}</dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">Live bytes</dt><dd className="font-medium tabular-nums">{formatBytes(keyspace.live_bytes)}</dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">Expired</dt><dd className="font-medium tabular-nums">{keyspace.expired_count}</dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">Revision</dt><dd className="font-mono text-xs">k-{keyspace.revision}</dd></div>
          </dl>
        </div>
      )}

      <div className="mb-3 flex flex-wrap items-end gap-2">
        <div className="grid gap-1.5">
          <Label htmlFor="keyspace-prefix">Prefix search (server-side)</Label>
          <Input id="keyspace-prefix" value={prefix} onChange={(event) => setPrefix(event.target.value)} placeholder="report:" className="w-56" spellCheck={false} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="keyspace-expires">Expiry</Label>
          <Select value={expires} onValueChange={(value) => { setExpires(value as ExpiresFilter); setBackStack([]); setCursor(null); }}>
            <SelectTrigger id="keyspace-expires" className="w-40"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All entries</SelectItem>
              <SelectItem value="present">With expiry</SelectItem>
              <SelectItem value="none">Without expiry</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <Button variant="ghost" size="sm" onClick={resetToFirstPage}>Apply</Button>
      </div>

      {error && entries.length === 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {error && stale && entries.length > 0 && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {!loadedOnce ? (
        <div className="grid gap-2" aria-busy="true">
          {Array.from({ length: 5 }, (_, index) => <Skeleton key={index} className="h-11 w-full" />)}
          <p className="text-sm text-muted-foreground">Checking entries…</p>
        </div>
      ) : entries.length === 0 ? (
        <div className="border-t border-rule-strong pt-2">
          {filtered ? (
            <div className="grid justify-items-start gap-1.5 px-4 py-10">
              <p className="text-sm font-medium">No keys match this prefix.</p>
              <p className="text-sm text-muted-foreground">Adjust the prefix or expiry filter and apply again.</p>
              <Button variant="outline" size="sm" className="mt-2" onClick={() => { setPrefix(""); setExpires("all"); resetToFirstPage(); }}>
                Clear filter
              </Button>
            </div>
          ) : (
            <div className="grid justify-items-start gap-1.5 px-4 py-10">
              <p className="text-sm font-medium">No entries yet.</p>
              <p className="text-sm text-muted-foreground">Put a durable key to start using the keyspace.</p>
              <Button size="sm" className="mt-2" onClick={() => setPutOpen(true)}><Plus className="size-4" />Put entry</Button>
            </div>
          )}
        </div>
      ) : (
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
                <TableCell>
                  <button
                    type="button"
                    className="text-left font-medium hover:underline"
                    onClick={(event) => { event.stopPropagation(); void openEntry(profileId, entry.id, setSelected, setPageError); }}
                  >
                    {entry.key}
                  </button>
                </TableCell>
                <TableCell><CopyId id={entry.id} /></TableCell>
                <TableCell className="text-right tabular-nums">{formatBytes(entry.value_size)}</TableCell>
                <TableCell className="text-right text-muted-foreground">
                  {entry.expires_at === null ? "No expiry" : `${formatRelative(entry.expires_at * 1000)}${entry.remaining_ttl_ms !== null ? ` (${Math.round((entry.remaining_ttl_ms ?? 0) / 1000)}s)` : ""}`}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
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
          <div className="grid gap-1.5">
            <div className="flex items-center justify-between">
              <Label htmlFor="entry-key">Key</Label>
              <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <Switch checked={useRawId} onCheckedChange={setUseRawId} aria-label="Use raw b64u ID" /> raw b64u ID
              </label>
            </div>
            <Input id="entry-key" value={keyText} onChange={(event) => setKeyText(event.target.value)} placeholder="greeting" spellCheck={false} aria-invalid={entryId.length > 0 && !idValid ? true : undefined} />
            {entryId.length > 0 && idValid && <p className="font-mono text-xs text-muted-foreground">{entryId}</p>}
            {entryId.length > 0 && !idValid && <p className="text-xs text-destructive">Not a valid opaque entry ID.</p>}
          </div>
          <div className="grid gap-1.5">
            <Label>Encoding</Label>
            <Tabs value={mode} onValueChange={(value) => setMode(value as ValueMode)}>
              <TabsList aria-label="Value encoding"><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger><TabsTrigger value="base64">Base64</TabsTrigger></TabsList>
            </Tabs>
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="entry-value">Value ({mode})</Label>
            <Textarea id="entry-value" value={raw} onChange={(event) => setRaw(event.target.value)} rows={4} spellCheck={false} className="font-mono text-xs" />
          </div>
          <div className="flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
            <span>{draft.bytes} bytes</span>
            {draft.error && <span className="text-destructive">{draft.error}</span>}
            <div className="ml-auto grid w-32 gap-1">
              <Label htmlFor="entry-ttl" className="text-xs">TTL seconds</Label>
              <Input id="entry-ttl" value={ttlSeconds} onChange={(event) => setTtlSeconds(event.target.value)} inputMode="numeric" className="h-8" />
            </div>
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
          <DialogDescription className="break-all font-mono text-xs">{entry.id}</DialogDescription>
        </DialogHeader>
        <div className="grid gap-3 py-2">
          <BinaryValue value={entry.value} />
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Close</Button>
          <Button variant="destructive" onClick={() => setConfirmOpen(true)}><Trash2 className="size-4" />Delete</Button>
        </DialogFooter>
        <ConfirmDestructive
          open={confirmOpen} onOpenChange={setConfirmOpen} confirmId={entry.id} inFlight={busy}
          title="Delete keyspace entry"
          description="Removes this exact entry durably. The decoded name is only a preview — the opaque ID is authoritative."
          context={<ConnectionContextLine profileId={profileId} />}
          error={error}
          confirmLabel="Delete entry"
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
            <div className="grid gap-1.5">
              <Label htmlFor="batch-put-lines">Entries, one key=value per line</Label>
              <Textarea id="batch-put-lines" value={putLines} onChange={(event) => setPutLines(event.target.value)} rows={5} className="font-mono text-xs" spellCheck={false} />
            </div>
            <div className="flex flex-wrap items-end gap-2">
              <div className="grid w-48 gap-1">
                <Label htmlFor="batch-ttl" className="text-xs">TTL seconds (optional)</Label>
                <Input id="batch-ttl" value={ttlSeconds} onChange={(event) => setTtlSeconds(event.target.value)} inputMode="numeric" className="h-8" />
              </div>
              <Button onClick={() => void runPut()} disabled={busy}>{busy ? "Applying…" : "Apply batch put"}</Button>
            </div>
          </TabsContent>
          <TabsContent value="delete" className="grid gap-3 pt-2">
            <div className="grid gap-1.5">
              <Label htmlFor="batch-delete-lines">Keys or b64u:… IDs, one per line</Label>
              <Textarea id="batch-delete-lines" value={deleteLines} onChange={(event) => setDeleteLines(event.target.value)} rows={5} className="font-mono text-xs" spellCheck={false} />
            </div>
            <div>
              <Button variant="destructive" onClick={() => void runDelete()} disabled={busy || deleteLines.trim().length === 0}>{busy ? "Deleting…" : "Delete batch"}</Button>
            </div>
          </TabsContent>
          <TabsContent value="get" className="grid gap-3 pt-2">
            <div className="grid gap-1.5">
              <Label htmlFor="batch-get-lines">Keys or b64u:… IDs, one per line</Label>
              <Textarea id="batch-get-lines" value={getLines} onChange={(event) => setGetLines(event.target.value)} rows={5} className="font-mono text-xs" spellCheck={false} />
            </div>
            <div>
              <Button variant="outline" onClick={() => void runGet()} disabled={busy || getLines.trim().length === 0}>{busy ? "Reading…" : "Read batch"}</Button>
            </div>
          </TabsContent>
        </Tabs>
        {error && <ErrorBanner error={error} className="mt-2" />}
        {result && <pre className="mt-2 max-h-48 overflow-auto bg-muted p-2 font-mono text-xs">{result}</pre>}
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
          <div className="flex flex-wrap gap-2">
            <div className="grid flex-1 gap-1.5">
              <Label htmlFor="claim-key">Key</Label>
              <Input id="claim-key" value={key} onChange={(event) => setKey(event.target.value)} spellCheck={false} />
            </div>
            <div className="grid w-28 gap-1.5">
              <Label htmlFor="claim-lease">Lease ms</Label>
              <Input id="claim-lease" value={leaseMs} onChange={(event) => setLeaseMs(event.target.value)} inputMode="numeric" />
            </div>
          </div>
          <p className="font-mono text-xs text-muted-foreground">{entryId || "b64u:…"}</p>
          <div className="flex flex-wrap gap-2">
            <Button size="sm" onClick={() => void createClaim()} disabled={busy || !entryId}>Create claim</Button>
            <Button size="sm" variant="outline" onClick={() => void getOrRefresh()} disabled={busy || !entryId}>Get or refresh</Button>
          </div>
          {claim && (
            <div className="grid gap-2 border p-3">
              <p className="font-mono text-xs">{claim.claim_id}</p>
              <div className="grid gap-1.5">
                <Label htmlFor="claim-complete-value">Value (text) to complete with</Label>
                <Input id="claim-complete-value" value={value} onChange={(event) => setValue(event.target.value)} spellCheck={false} />
              </div>
              <div className="flex gap-2">
                <Button size="sm" onClick={() => void completeClaim()} disabled={busy}>Complete</Button>
                <Button size="sm" variant="outline" onClick={() => void releaseClaim()} disabled={busy}>Release</Button>
              </div>
            </div>
          )}
          {error && <ErrorBanner error={error} />}
          {claimState && claim?.claim_id === undefined && <pre className="max-h-40 overflow-auto bg-muted p-2 font-mono text-xs">{claimState}</pre>}
        </div>
      </DialogContent>
    </Dialog>
  );
}
