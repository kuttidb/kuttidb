import { useCallback, useMemo, useState } from "react";
import { Plus, Pencil, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { usePolling } from "@/hooks/use-polling";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { BinaryValue, encodeDraft } from "@/components/binary-value";
import { ConfirmDestructive } from "@/components/confirm";
import { ErrorBanner } from "@/components/error-banner";
import { CopyId, CursorPager, DetailGrid, LastRefreshed, PageHeader, StateBadge, ConnectionContextLine } from "@/components/shared";
import { admin, ApiError, list, newIdempotencyKey } from "@/lib/api";
import { idFromName, isValidB64uId, nameFromId } from "@/lib/codec";
import { formatBytes } from "@/lib/format";
import { useConnections } from "@/state/connections";
import {
  durableKeyspaceSchema,
  durableEntrySchema,
  durableMutationReceiptSchema,
  jobCompletionCapability,
  stateEtag,
  versionFromEtag,
  type DurableEntry,
  type DurableEntryMeta,
  type DurableKeyspaceInfo
} from "@/lib/job-completion";

type PreconditionMode = "create" | "update";
type ValueMode = "text" | "json" | "base64";

/**
 * Durable state browser (the `durable` keyspace of atomic job completion).
 * Deliberately separate from the default cache: no eviction, no TTL, commit
 * authority in the Queue WAL. Every mutation is capability-gated, and older
 * or disabled servers get an explanation plus read-only controls — never a
 * fallback of separate writes.
 */
export function DurableStateBrowser({ profileId }: { profileId: string }) {
  const { capabilities, mutationsBlocked } = useConnections();
  const capability = jobCompletionCapability(capabilities.get(profileId));
  const [aggregate, setAggregate] = useState<DurableKeyspaceInfo | null>(null);
  const [entries, setEntries] = useState<DurableEntryMeta[]>([]);
  const [meta, setMeta] = useState<{ nextCursor: string | null; weak: boolean } | null>(null);
  const [backStack, setBackStack] = useState<string[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [prefix, setPrefix] = useState("");
  const [pageError, setPageError] = useState<Error | null>(null);
  const [putOpen, setPutOpen] = useState(false);
  const [selected, setSelected] = useState<DurableEntry | null>(null);
  const [selectedEtag, setSelectedEtag] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);

  const prefixQuery = useMemo(
    () => (prefix.trim().length > 0 ? `&prefix=${encodeURIComponent(idFromName(prefix.trim()))}` : ""),
    [prefix]
  );
  const basePath = useCallback(
    (cursorValue: string | null) =>
      `keyspaces/durable/entries?limit=50${prefixQuery}${cursorValue ? `&cursor=${encodeURIComponent(cursorValue)}` : ""}`,
    [prefixQuery]
  );

  const loader = useCallback(async () => {
    const response = await list<DurableEntryMeta>(profileId, basePath(cursor));
    setEntries(response.data);
    setMeta({ nextCursor: response.meta?.next_cursor ?? null, weak: response.meta?.weakly_consistent ?? false });
    const aggregateResponse = await admin<{ data: unknown }>(profileId, "keyspaces/durable");
    const parsedAggregate = durableKeyspaceSchema.safeParse(aggregateResponse.json.data);
    if (!parsedAggregate.success) throw new ApiError("upstream_contract", "The durable keyspace response was not understood.", 502);
    setAggregate(parsedAggregate.data);
    setLoaded(true);
  }, [profileId, basePath, cursor]);

  const { lastUpdated, error, stale, refresh } = usePolling(loader, 20_000);
  const loadedOnce = lastUpdated !== null || error !== null;
  const filtered = prefix.trim().length > 0;
  const blocked = mutationsBlocked(profileId);
  const mutable = Boolean(capability?.enabled) && !blocked;

  const resetToFirstPage = () => { setBackStack([]); setCursor(null); refresh(); };

  const openEntry = async (id: string) => {
    setPageError(null);
    try {
      const response = await admin<unknown>(profileId, `keyspaces/durable/entries/${id}`);
      const parsed = durableEntrySchema.safeParse(response.json);
      if (!parsed.success) throw new ApiError("upstream_contract", "The durable entry response was not understood.", 502);
      setSelected(parsed.data);
      setSelectedEtag(response.etag);
    } catch (reason) {
      setPageError(reason instanceof Error ? reason : new Error(String(reason)));
    }
  };

  if (!capability) {
    return (
      <div>
        <PageHeader title="Durable state" description="The durable keyspace used by atomic job completion." />
        <div className="grid justify-items-start gap-1.5 border-t border-rule-strong px-4 py-8">
          <p className="text-sm font-medium">Durable state is unavailable on this server.</p>
          <p className="max-w-2xl text-sm text-muted-foreground">
            This server either lacks atomic job completion or predates it (server flag --job-completion). There is no
            fallback: durable state changes exist only inside the atomic completion commit or its direct version-checked
            API.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <PageHeader
        title="Durable state"
        description="The durable keyspace used by atomic job completion. Entries never expire and are never evicted; capacity is bounded by the server's state budget. This is not the default cache."
        actions={
          <>
            <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
            <Button size="sm" onClick={() => setPutOpen(true)} disabled={!mutable}>
              <Plus className="size-4" />Put entry
            </Button>
          </>
        }
      />
      {!capability.enabled && (
        <ErrorBanner
          error={new ApiError("unsupported_feature", "This server supports atomic job completion but was started without it (--job-completion). Durable state is read-only here; mutations need an enabled server.", 503)}
          className="mb-4"
        />
      )}
      {capability.enabled && blocked && (
        <ErrorBanner
          error={new ApiError("audit_unavailable", "The audit trail is unhealthy. Durable state mutations are blocked for this connection; reads remain available.", 503)}
          className="mb-4"
        />
      )}
      {aggregate && (
        <div className="mb-4 border-y border-rule-strong">
          <dl className="flex flex-wrap gap-x-8 gap-y-1 py-3 text-sm">
            <div className="flex gap-2"><dt className="text-muted-foreground">Entries</dt><dd className="font-medium tabular-nums">{aggregate.entry_count}</dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">Live bytes</dt><dd className="font-medium tabular-nums">{formatBytes(aggregate.live_bytes)}</dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">Capacity</dt><dd className="font-medium tabular-nums">{formatBytes(Number(aggregate.capacity_bytes))}</dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">Persistence</dt><dd><StateBadge state={aggregate.persistence_healthy ? "healthy" : "unhealthy"} /></dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">Storage</dt><dd className="font-mono text-xs">{aggregate.storage_class}</dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">Eviction</dt><dd>never</dd></div>
            <div className="flex gap-2"><dt className="text-muted-foreground">TTL</dt><dd>none</dd></div>
          </dl>
        </div>
      )}

      <div className="mb-3 flex flex-wrap items-end gap-2">
        <div className="grid gap-1.5">
          <Label htmlFor="durable-prefix">Prefix search (server-side)</Label>
          <Input id="durable-prefix" value={prefix} onChange={(event) => setPrefix(event.target.value)} placeholder="order:" className="w-56" spellCheck={false} />
        </div>
        <Button variant="ghost" size="sm" onClick={resetToFirstPage}>Apply</Button>
      </div>

      {error && !loaded && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {error && stale && loaded && <ErrorBanner error={error} onRetry={refresh} className="mb-4" />}
      {!loaded ? (
        <div className="grid gap-2" aria-busy="true">
          <span className="sr-only">Loading durable state…</span>
          {Array.from({ length: 5 }, (_, index) => <Skeleton key={index} className="h-11 w-full" />)}
        </div>
      ) : entries.length === 0 ? (
        <div className="border-t border-rule-strong pt-2">
          {filtered ? (
            <div className="grid justify-items-start gap-1.5 px-4 py-10">
              <p className="text-sm font-medium">No durable state keys match this prefix.</p>
              <Button variant="outline" size="sm" className="mt-2" onClick={() => { setPrefix(""); resetToFirstPage(); }}>Clear filter</Button>
            </div>
          ) : (
            <div className="grid justify-items-start gap-1.5 px-4 py-10">
              <p className="text-sm font-medium">No durable state yet.</p>
              <p className="text-sm text-muted-foreground">Job completions and direct version-checked puts create entries here; they never expire or evict.</p>
              {mutable && <Button size="sm" className="mt-2" onClick={() => setPutOpen(true)}><Plus className="size-4" />Put entry</Button>}
            </div>
          )}
        </div>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Key</TableHead>
              <TableHead>Entry ID</TableHead>
              <TableHead className="text-right">Version</TableHead>
              <TableHead className="text-right">Size</TableHead>
              <TableHead className="text-right">Last commit</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {entries.map((entry) => (
              <TableRow key={entry.entry_id} className="cursor-pointer" onClick={() => void openEntry(entry.entry_id)}>
                <TableCell>
                  <button type="button" className="text-left font-medium hover:underline" onClick={(event) => { event.stopPropagation(); void openEntry(entry.entry_id); }}>
                    {nameFromId(entry.entry_id) ?? entry.entry_id}
                  </button>
                </TableCell>
                <TableCell><CopyId id={entry.entry_id} /></TableCell>
                <TableCell className="text-right font-mono text-xs tabular-nums">{entry.version}</TableCell>
                <TableCell className="text-right tabular-nums">{formatBytes(entry.value_size)}</TableCell>
                <TableCell className="text-right font-mono text-xs tabular-nums text-muted-foreground">{entry.last_commit_id}</TableCell>
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

      <DurablePutDialog
        open={putOpen}
        onOpenChange={setPutOpen}
        profileId={profileId}
        mutable={mutable}
        onDone={resetToFirstPage}
      />
      <DurableEntryDialog
        entry={selected}
        etag={selectedEtag}
        onClose={() => setSelected(null)}
        profileId={profileId}
        mutable={mutable}
        onDeleted={resetToFirstPage}
        onUpdated={refresh}
      />
    </div>
  );
}

function DurablePutDialog({ open, onOpenChange, profileId, mutable, existing, onDone }: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  profileId: string;
  mutable: boolean;
  existing?: { id: string; version: string } | null;
  onDone: () => void;
}) {
  const [keyText, setKeyText] = useState("");
  const [useRawId, setUseRawId] = useState(false);
  const [mode, setMode] = useState<PreconditionMode>(existing ? "update" : "create");
  const [expectedVersion, setExpectedVersion] = useState(existing?.version ?? "0");
  const [valueMode, setValueMode] = useState<ValueMode>("text");
  const [raw, setRaw] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const draft = encodeDraft(valueMode, raw);
  const entryId = existing
    ? existing.id
    : useRawId
      ? keyText.trim()
      : keyText.trim().length > 0
        ? idFromName(keyText.trim())
        : "";
  const idValid = entryId.length > 0 && isValidB64uId(entryId);

  const submit = async () => {
    if (!idValid || draft.error) return;
    setBusy(true); setError(null);
    try {
      const response = await admin(profileId, `keyspaces/durable/entries/${entryId}`, {
        method: "PUT",
        idempotencyKey: newIdempotencyKey(),
        ...(mode === "create" ? { ifNoneMatch: "*" } : { ifMatch: stateEtag(expectedVersion.trim() || "0") }),
        body: { value: { encoding: "base64", data: draft.base64 } }
      });
      const receipt = durableMutationReceiptSchema.safeParse(response.json);
      toast.success(
        receipt.success
          ? receipt.data.replayed
            ? "Already committed — retained receipt replayed"
            : `Durable state committed at version ${receipt.data.state_version}`
          : "Durable state committed",
        { duration: 5000 }
      );
      onOpenChange(false);
      setRaw(""); setKeyText("");
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
          <DialogTitle>{existing ? "Edit durable state" : "Put durable state"}</DialogTitle>
          <DialogDescription>
            A version-checked durable commit that receives its own operation receipt. Values never expire and never
            evict; empty values are allowed.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 py-2">
          {!existing && (
            <div className="grid gap-1.5">
              <div className="flex items-center justify-between">
                <Label htmlFor="durable-entry-key">Key</Label>
                <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Switch checked={useRawId} onCheckedChange={setUseRawId} aria-label="Use raw b64u ID" /> raw b64u ID
                </label>
              </div>
              <Input id="durable-entry-key" value={keyText} onChange={(event) => setKeyText(event.target.value)} placeholder="order:42" spellCheck={false} aria-invalid={keyText.length > 0 && !idValid ? true : undefined} />
              {entryId.length > 0 && idValid && <p className="font-mono text-xs break-all text-muted-foreground">{entryId}</p>}
              {entryId.length > 0 && !idValid && <p className="text-xs text-destructive">Not a valid opaque entry ID.</p>}
            </div>
          )}
          <div className="grid gap-1.5">
            <Label htmlFor="durable-precondition">Precondition</Label>
            <Select value={mode} onValueChange={(value) => setMode(value as PreconditionMode)}>
              <SelectTrigger id="durable-precondition"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="create">Create only — key must not exist (version 0)</SelectItem>
                <SelectItem value="update">Update exact version</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {mode === "update" && (
            <div className="grid max-w-48 gap-1.5">
              <Label htmlFor="durable-version">Expected version</Label>
              <Input id="durable-version" value={expectedVersion} onChange={(event) => setExpectedVersion(event.target.value)} inputMode="numeric" className="font-mono text-xs" />
            </div>
          )}
          <div className="grid gap-1.5">
            <Label>Encoding</Label>
            <Tabs value={valueMode} onValueChange={(value) => setValueMode(value as ValueMode)}>
              <TabsList aria-label="Value encoding"><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger><TabsTrigger value="base64">Base64</TabsTrigger></TabsList>
            </Tabs>
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="durable-value">Value ({valueMode})</Label>
            <Textarea id="durable-value" value={raw} onChange={(event) => setRaw(event.target.value)} rows={4} spellCheck={false} className="font-mono text-xs" />
          </div>
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <span>{draft.bytes} bytes · no TTL · never evicted</span>
            {draft.error && <span className="text-destructive">{draft.error}</span>}
          </div>
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={() => void submit()} disabled={!mutable || busy || !idValid || Boolean(draft.error)}>
            {busy ? "Committing…" : existing ? "Commit update" : "Commit put"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function DurableEntryDialog({ entry, etag, onClose, profileId, mutable, onDeleted, onUpdated }: {
  entry: DurableEntry | null;
  etag: string | null;
  onClose: () => void;
  profileId: string;
  mutable: boolean;
  onDeleted: () => void;
  onUpdated: () => void;
}) {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [editOpen, setEditOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  if (!entry) return null;
  const version = versionFromEtag(etag) ?? entry.version;
  const decodedName = nameFromId(entry.key) ?? entry.entry_id;

  const remove = async () => {
    setBusy(true); setError(null);
    try {
      const response = await admin(profileId, `keyspaces/durable/entries/${entry.entry_id}`, {
        method: "DELETE",
        idempotencyKey: newIdempotencyKey(),
        ifMatch: stateEtag(version),
        confirm: "durable-state-delete",
        body: {}
      });
      const receipt = durableMutationReceiptSchema.safeParse(response.json);
      toast.success(receipt.success && receipt.data.replayed ? "Already deleted — retained receipt replayed" : `Deleted ${decodedName}`);
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
          <DialogTitle className="break-all">{decodedName}</DialogTitle>
          <DialogDescription className="break-all font-mono text-xs">{entry.entry_id}</DialogDescription>
        </DialogHeader>
        <div className="grid gap-3 py-2">
          <DetailGrid rows={[
            { label: "Version", value: version, mono: true },
            { label: "Last commit", value: entry.last_commit_id, mono: true },
            { label: "TTL", value: "none — never expires" },
            { label: "Eviction", value: "never" }
          ]} />
          <BinaryValue value={{
            encoding: entry.value.encoding,
            data: entry.value.data,
            ...(typeof entry.value.size === "number" ? { size: entry.value.size } : {}),
            ...(typeof entry.value.content_type === "string" ? { content_type: entry.value.content_type } : {})
          }} />
          {error && <ErrorBanner error={error} />}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Close</Button>
          <Button variant="outline" disabled={!mutable} onClick={() => setEditOpen(true)}><Pencil className="size-4" />Edit</Button>
          <Button variant="destructive" disabled={!mutable} onClick={() => setConfirmOpen(true)}><Trash2 className="size-4" />Delete</Button>
        </DialogFooter>
        {editOpen && (
          <DurablePutDialog
            open
            onOpenChange={setEditOpen}
            profileId={profileId}
            mutable={mutable}
            existing={{ id: entry.entry_id, version }}
            onDone={() => { setEditOpen(false); onClose(); onUpdated(); }}
          />
        )}
        <ConfirmDestructive
          open={confirmOpen} onOpenChange={setConfirmOpen} confirmId={entry.entry_id} inFlight={busy}
          title="Delete durable state"
          description="Durably removes this exact state key through the atomic job completion engine. The delete receives its own receipt."
          context={<ConnectionContextLine profileId={profileId} />}
          error={error}
          confirmLabel="Delete durable state"
          onConfirm={() => void remove()}
        />
      </DialogContent>
    </Dialog>
  );
}
