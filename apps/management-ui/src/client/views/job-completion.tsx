import { useCallback, useEffect, useMemo, useState } from "react";
import { Search, ShieldCheck } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { encodeDraft } from "@/components/binary-value";
import { ErrorBanner } from "@/components/error-banner";
import { CopyId, CursorPager, DetailGrid, EmptyState, LastRefreshed, Section, Skeleton } from "@/components/shared";
import { usePolling } from "@/hooks/use-polling";
import { admin, ApiError, list, newIdempotencyKey, type CollectionMeta } from "@/lib/api";
import { idFromName, isValidB64uId } from "@/lib/codec";
import { useConnections } from "@/state/connections";
import {
  advanceSubmission,
  buildCompletionRequest,
  completionPreviewEffects,
  completionReceiptSchema,
  durableOperationReceiptSchema,
  jobCompletionCapability,
  jobCompletionResultSchema,
  receiptStatusWord,
  retryAllowed,
  type CompletionIntent,
  type CompletionReceipt,
  type DurableOperationReceipt,
  type JobCompletionResult,
  type SubmissionPhase
} from "@/lib/job-completion";
import { clearPendingDelivery, getPendingDelivery, useClearPendingOnDisconnect } from "@/lib/job-intent-store";
import { formatTimestamp } from "@/lib/format";

type ValueMode = "text" | "json" | "base64";

const UUID_PATTERN = /^[0-9a-fA-F-]{36}$/;

/**
 * "Complete Queue job" (atomic job completion). The three effects — input ACK,
 * durable state write, optional output publish — commit under one operation
 * id. The exact intent (payload + operation id) is frozen at submit time and
 * reused verbatim on retries; timeouts resolve to "Outcome unknown", never
 * "Failed; try again".
 */
export function JobCompletionView({ profileId, onOpenQueues }: {
  profileId: string;
  onOpenQueues?: () => void;
}) {
  const { capabilities, live, mutationsBlocked } = useConnections();
  const capability = jobCompletionCapability(capabilities.get(profileId));
  const enabled = Boolean(capability?.enabled) && !mutationsBlocked(profileId);
  useClearPendingOnDisconnect(profileId, live.has(profileId));

  const [phase, setPhase] = useState<SubmissionPhase>({ phase: "composing" });
  // The held delivery is read once on mount. Acquiring one is the queue
  // view's explicit action; this view never consumes.
  const [heldDelivery] = useState(() => getPendingDelivery(profileId));
  const [operationId, setOperationId] = useState(() => newIdempotencyKey());

  const [inputQueue, setInputQueue] = useState(heldDelivery?.queue ?? "");
  const [inputIncarnation, setInputIncarnation] = useState(heldDelivery?.queueIncarnation ?? "");
  const [inputMessageId, setInputMessageId] = useState(heldDelivery?.messageId ?? "");
  const [deliveryProof, setDeliveryProof] = useState(heldDelivery?.deliveryProof ?? "");
  const [stateKey, setStateKey] = useState("");
  const [expectedVersion, setExpectedVersion] = useState("0");
  const [stateMode, setStateMode] = useState<ValueMode>("text");
  const [stateRaw, setStateRaw] = useState("");
  const [hasOutput, setHasOutput] = useState(false);
  const [outputQueue, setOutputQueue] = useState("");
  const [outputMode, setOutputMode] = useState<ValueMode>("text");
  const [outputRaw, setOutputRaw] = useState("");
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [lookupBusy, setLookupBusy] = useState(false);
  const [foundReceipt, setFoundReceipt] = useState<CompletionReceipt | null>(null);
  const [errorBanner, setErrorBanner] = useState<Error | null>(null);
  const [outputIncarnation, setOutputIncarnation] = useState<string | null>(null);

  const stateDraft = encodeDraft(stateMode, stateRaw);
  const outputDraft = encodeDraft(outputMode, outputRaw);
  const stateKeyId = stateKey.trim().length > 0 ? idFromName(stateKey.trim()) : "";
  const busy = phase.phase === "submitting";
  const locked = phase.phase === "submitting" || phase.phase === "unknown" || phase.phase === "failed";

  useEffect(() => {
    if (!live.has(profileId)) {
      // Locked or disconnected: drop the held proof and any unresolved intent.
      clearPendingDelivery(profileId);
      setPhase({ phase: "composing" });
    }
  }, [live, profileId]);

  // Resolve the live output queue identity while composing; the server
  // re-checks it inside the commit.
  useEffect(() => {
    if (!hasOutput || outputQueue.trim().length === 0) { setOutputIncarnation(null); return; }
    let cancelled = false;
    admin<{ data?: { revision?: number } }>(profileId, `queues/${idFromName(outputQueue.trim())}`)
      .then((response) => { if (!cancelled) setOutputIncarnation(String(response.json.data?.revision ?? 0)); })
      .catch(() => { if (!cancelled) setOutputIncarnation("0"); });
    return () => { cancelled = true; };
  }, [hasOutput, outputQueue, profileId]);

  const intent: CompletionIntent = useMemo(
    () => ({
      operationId,
      input: { queue: inputQueue.trim(), queueIncarnation: inputIncarnation.trim(), messageId: inputMessageId.trim(), deliveryProof: deliveryProof.trim() },
      state: { key: stateKeyId, expectedVersion: expectedVersion.trim() || "0", valueBase64: stateDraft.base64 },
      outgoing: hasOutput ? { queue: outputQueue.trim(), queueIncarnation: outputIncarnation ?? "0", valueBase64: outputDraft.base64 } : null
    }),
    [operationId, inputQueue, inputIncarnation, inputMessageId, deliveryProof, stateKeyId, expectedVersion, stateDraft.base64, hasOutput, outputQueue, outputIncarnation, outputDraft.base64]
  );

  const composeError: Record<string, string> = useMemo(() => {
    const errors: Record<string, string> = {};
    if (intent.input.queue.length === 0) errors.inputQueue = "The input Queue is required.";
    if (!/^[0-9]+$/.test(intent.input.queueIncarnation) || intent.input.queueIncarnation === "0") errors.inputIncarnation = "The queue incarnation from the delivery is required.";
    if (!/^[0-9]+$/.test(intent.input.messageId) || intent.input.messageId === "0") errors.inputMessageId = "The message id from the delivery is required.";
    if (intent.input.deliveryProof.length === 0) errors.deliveryProof = "A delivery proof is required. Consume for completion from the durable Queue first.";
    if (!isValidB64uId(intent.state.key)) errors.stateKey = "A durable state key is required (plain text or a valid b64u identifier).";
    if (!/^[0-9]+$/.test(intent.state.expectedVersion)) errors.expectedVersion = "Expected version must be a decimal string (0 = create only).";
    if (stateDraft.error) errors.stateValue = stateDraft.error;
    if (hasOutput) {
      if (intent.outgoing && intent.outgoing.queue.length === 0) errors.outputQueue = "An output queue name is required.";
      if (outputDraft.error) errors.outputValue = outputDraft.error;
    }
    return errors;
  }, [intent, stateDraft.error, hasOutput, outputDraft.error]);

  const previewEffects = completionPreviewEffects(intent);
  const unresolved = phase.phase === "unknown";
  const failed = phase.phase === "failed";
  const resolved = phase.phase === "resolved";

  const submit = async (frozen: CompletionIntent, body: Record<string, unknown>) => {
    setPhase({ phase: "submitting", intent: frozen });
    setFoundReceipt(null);
    setErrorBanner(null);
    try {
      const response = await admin<unknown>(profileId, "job-completions", {
        method: "POST",
        idempotencyKey: frozen.operationId,
        body,
        // Abort just after the gateway's own 30s upstream timeout so a lost
        // response resolves to "Outcome unknown" — never an automatic retry.
        signal: typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function" ? AbortSignal.timeout(31_000) : undefined
      });
      const parsed = jobCompletionResultSchema.safeParse(response.json);
      if (!parsed.success) throw new ApiError("upstream_contract", "The completion receipt response was not understood.", 502);
      setPhase({ phase: "resolved", intent: frozen, result: parsed.data });
      toast.success(parsed.data.replayed ? "Already completed — receipt replayed" : "Job completed");
    } catch (reason) {
      setPhase(advanceSubmission({ phase: "submitting", intent: frozen }, { kind: "fail", error: reason }));
    }
  };

  const onSubmit = () => {
    setFieldErrors(composeError);
    if (Object.keys(composeError).length > 0) return;
    // Freeze exactly this intent; the retry path below reuses it verbatim.
    const frozen: CompletionIntent = JSON.parse(JSON.stringify(intent)) as CompletionIntent;
    void submit(frozen, buildCompletionRequest(frozen));
  };

  const retrySameCompletion = () => {
    if (phase.phase !== "unknown" && phase.phase !== "failed") return;
    // Same frozen intent: same Completion ID, same payload. Never the
    // (possibly edited) field state.
    void submit(phase.intent, buildCompletionRequest(phase.intent));
  };

  const checkCompletion = async () => {
    if (phase.phase !== "unknown") return;
    setLookupBusy(true);
    try {
      const response = await admin<unknown>(profileId, `job-completions/${phase.intent.operationId}`);
      const parsed = completionReceiptSchema.safeParse(response.json);
      if (!parsed.success) throw new ApiError("upstream_contract", "The completion receipt response was not understood.", 502);
      setFoundReceipt(parsed.data);
      toast.success("Retained receipt found — this completion already committed");
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 404) {
        toast.info("No retained receipt for this Completion ID yet. It may still be in flight — reconcile, then retry the same completion while the delivery lease allows.");
      } else {
        setErrorBanner(reason instanceof Error ? reason : new Error(String(reason)));
      }
    } finally {
      setLookupBusy(false);
    }
  };

  const startNewIntent = () => {
    // Explicit operator action: fresh operation id and cleared form. Never
    // reached automatically from an unresolved phase.
    clearPendingDelivery(profileId);
    setOperationId(newIdempotencyKey());
    setPhase({ phase: "composing" });
    setFoundReceipt(null);
    setInputQueue(""); setInputIncarnation(""); setInputMessageId(""); setDeliveryProof("");
    setStateKey(""); setExpectedVersion("0"); setStateRaw("");
    setHasOutput(false); setOutputQueue(""); setOutputRaw("");
    setFieldErrors({});
    setErrorBanner(null);
  };

  if (!capability) {
    return (
      <Section title="Complete Queue job">
        <EmptyState
          title="Atomic job completion is unavailable on this server."
          hint="This server does not advertise job completion support. There is deliberately no fallback: an ACK, a durable state write, and an output publish cannot be made atomic through separate calls, so the console offers no such path."
        />
      </Section>
    );
  }

  return (
    <div className="grid grid-cols-1 items-start gap-6 lg:grid-cols-[3fr_2fr] lg:gap-8">
      <Section title="Compose completion">
        <div className="grid gap-3">
          {!capability.enabled && (
            <ErrorBanner
              error={new ApiError("unsupported_feature", "This server supports atomic job completion but was started without it (--job-completion). Composing is disabled; nothing can be committed.", 503)}
            />
          )}
          {capability.enabled && mutationsBlocked(profileId) && (
            <ErrorBanner
              error={new ApiError("audit_unavailable", "The audit trail is unhealthy. Completions cannot be committed for this connection; reads remain available.", 503)}
            />
          )}
          <div className="border px-3 py-2.5">
            <p className="text-xs font-medium uppercase tracking-[0.06em] text-muted-foreground">Completion ID (operation id)</p>
            <div className="mt-1 flex flex-wrap items-center gap-2">
              <span className="break-all font-mono text-xs">{operationId}</span>
              <CopyId id={operationId} label="Copy Completion ID" />
              <Button variant="ghost" size="xs" onClick={() => { setOperationId(newIdempotencyKey()); toast.info("Fresh Completion ID generated for a new intent."); }} disabled={locked || busy}>
                New intent
              </Button>
            </div>
            <p className="mt-1 text-xs text-muted-foreground">Generated once per intent and reused on every retry of the same completion.</p>
          </div>

          {heldDelivery && (
            <div className="border border-success/40 bg-success-surface px-3 py-2.5 text-sm">
              <p className="font-medium">Delivery held from Queue {heldDelivery.queue}</p>
              <p className="mt-0.5 text-xs text-muted-foreground">
                Message {heldDelivery.messageId}, incarnation {heldDelivery.queueIncarnation}, acquired {formatTimestamp(Math.floor(heldDelivery.acquiredAt / 1000))}.
                The proof stays in this browser tab's memory only.
              </p>
            </div>
          )}

          <fieldset disabled={locked || !enabled} className="grid gap-3 disabled:opacity-60">
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div className="grid gap-1.5">
                <Label htmlFor="job-input-queue">Input Queue</Label>
                <Input id="job-input-queue" value={inputQueue} onChange={(event) => setInputQueue(event.target.value)} spellCheck={false} aria-invalid={fieldErrors.inputQueue ? true : undefined} />
                {fieldErrors.inputQueue && <p className="text-xs text-destructive">{fieldErrors.inputQueue}</p>}
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="job-input-message">Message ID</Label>
                <Input id="job-input-message" value={inputMessageId} onChange={(event) => setInputMessageId(event.target.value)} inputMode="numeric" aria-invalid={fieldErrors.inputMessageId ? true : undefined} />
                {fieldErrors.inputMessageId && <p className="text-xs text-destructive">{fieldErrors.inputMessageId}</p>}
              </div>
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor="job-input-incarnation">Queue incarnation</Label>
              <Input id="job-input-incarnation" value={inputIncarnation} onChange={(event) => setInputIncarnation(event.target.value)} inputMode="numeric" className="font-mono text-xs" aria-invalid={fieldErrors.inputIncarnation ? true : undefined} />
              {fieldErrors.inputIncarnation && <p className="text-xs text-destructive">{fieldErrors.inputIncarnation}</p>}
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor="job-input-proof">Delivery proof (Base64, one-use)</Label>
              <Input id="job-input-proof" value={deliveryProof} onChange={(event) => setDeliveryProof(event.target.value)} spellCheck={false} className="font-mono text-xs" aria-invalid={fieldErrors.deliveryProof ? true : undefined} />
              {fieldErrors.deliveryProof && <p className="text-xs text-destructive">{fieldErrors.deliveryProof}</p>}
              <p className="text-xs text-muted-foreground">Held in browser memory only — never stored, logged, or placed in URLs.</p>
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-[2fr_1fr]">
              <div className="grid gap-1.5">
                <Label htmlFor="job-state-key">Durable state key</Label>
                <Input id="job-state-key" value={stateKey} onChange={(event) => setStateKey(event.target.value)} spellCheck={false} placeholder="order:42" aria-invalid={fieldErrors.stateKey ? true : undefined} />
                {stateKeyId && <p className="break-all font-mono text-xs text-muted-foreground">{stateKeyId}</p>}
                {fieldErrors.stateKey && <p className="text-xs text-destructive">{fieldErrors.stateKey}</p>}
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="job-state-version">Expected version</Label>
                <Input id="job-state-version" value={expectedVersion} onChange={(event) => setExpectedVersion(event.target.value)} inputMode="numeric" className="font-mono text-xs" aria-invalid={fieldErrors.expectedVersion ? true : undefined} />
                <p className="text-xs text-muted-foreground">0 = key must not exist; otherwise the exact current version.</p>
                {fieldErrors.expectedVersion && <p className="text-xs text-destructive">{fieldErrors.expectedVersion}</p>}
              </div>
            </div>
            <div className="grid gap-1.5">
              <Label>State value encoding</Label>
              <Tabs value={stateMode} onValueChange={(value) => setStateMode(value as ValueMode)}>
                <TabsList aria-label="State value encoding"><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger><TabsTrigger value="base64">Base64</TabsTrigger></TabsList>
              </Tabs>
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor="job-state-value">State value</Label>
              <Textarea id="job-state-value" value={stateRaw} onChange={(event) => setStateRaw(event.target.value)} rows={4} spellCheck={false} className="font-mono text-xs" />
              <p className="text-xs text-muted-foreground">{stateDraft.bytes} bytes · empty values are allowed{stateDraft.error ? ` · ${stateDraft.error}` : ""}</p>
            </div>

            <div className="flex items-center justify-between border-t border-rule pt-3">
              <Label htmlFor="job-has-output" className="text-sm">Publish an output message</Label>
              <Switch id="job-has-output" checked={hasOutput} onCheckedChange={setHasOutput} disabled={locked || !enabled} />
            </div>
            {hasOutput && (
              <>
                <div className="grid gap-1.5">
                  <Label htmlFor="job-output-queue">Output Queue</Label>
                  <Input id="job-output-queue" value={outputQueue} onChange={(event) => setOutputQueue(event.target.value)} spellCheck={false} aria-invalid={fieldErrors.outputQueue ? true : undefined} />
                  {fieldErrors.outputQueue && <p className="text-xs text-destructive">{fieldErrors.outputQueue}</p>}
                </div>
                <div className="grid gap-1.5">
                  <Label>Output body encoding</Label>
                  <Tabs value={outputMode} onValueChange={(value) => setOutputMode(value as ValueMode)}>
                    <TabsList aria-label="Output body encoding"><TabsTrigger value="text">Text</TabsTrigger><TabsTrigger value="json">JSON</TabsTrigger><TabsTrigger value="base64">Base64</TabsTrigger></TabsList>
                  </Tabs>
                </div>
                <div className="grid gap-1.5">
                  <Label htmlFor="job-output-value">Output body</Label>
                  <Textarea id="job-output-value" value={outputRaw} onChange={(event) => setOutputRaw(event.target.value)} rows={3} spellCheck={false} className="font-mono text-xs" />
                  <p className="text-xs text-muted-foreground">{outputDraft.bytes} bytes · empty bodies are allowed{outputDraft.error ? ` · ${outputDraft.error}` : ""}</p>
                </div>
              </>
            )}
          </fieldset>

          {unresolved || failed ? (
            <div role="alert" className={`border px-3 py-2.5 text-sm ${unresolved ? "border-warning/40 bg-warning-surface" : "border-destructive/40 bg-danger-surface"}`}>
              {unresolved ? (
                <>
                  <p className="font-medium text-warning">Outcome unknown.</p>
                  <p className="mt-0.5 text-muted-foreground">
                    The commit may or may not have taken effect — the response was lost or the server reported the
                    outcome as unknown. Check the receipt by Completion ID, or retry the exact same completion (same ID
                    and payload). Editing is locked until you resolve or discard this intent.
                  </p>
                </>
              ) : (
                <>
                  <p className="font-medium text-destructive">The completion was not committed.</p>
                  <p className="mt-0.5 text-muted-foreground">The server refused this completion without ambiguity. Fix the cause or retry the exact same intent.</p>
                </>
              )}
              {(phase.phase === "unknown" || phase.phase === "failed") && phase.error instanceof Error && (
                <div className="mt-1.5"><ErrorBanner error={phase.error} /></div>
              )}
              <div className="mt-2 flex flex-wrap gap-2">
                {unresolved && (
                  <Button variant="outline" size="sm" onClick={() => void checkCompletion()} disabled={lookupBusy}>
                    <Search className="size-4" />{lookupBusy ? "Checking…" : "Check completion"}
                  </Button>
                )}
                <Button size="sm" onClick={retrySameCompletion} disabled={busy || !retryAllowed(phase)}>
                  {busy ? "Submitting…" : "Retry same completion"}
                </Button>
                <Button variant="ghost" size="sm" onClick={startNewIntent} disabled={busy}>
                  Discard and compose a new intent
                </Button>
              </div>
            </div>
          ) : resolved ? (
            <div className="flex flex-wrap gap-2">
              <Button variant="outline" onClick={startNewIntent}>Compose a new completion</Button>
            </div>
          ) : (
            <>
              {errorBanner && <ErrorBanner error={errorBanner} />}
              <div>
                <Button onClick={onSubmit} disabled={busy || !enabled}>
                  <ShieldCheck className="size-4 mr-1" />{busy ? "Committing…" : "Complete job"}
                </Button>
              </div>
            </>
          )}
        </div>
      </Section>

      <div className="grid gap-6">
        <Section title="Intent preview">
          <p className="mb-2 text-xs text-muted-foreground">This one commit will:</p>
          <ol className="grid list-decimal gap-1.5 pl-5 text-sm">
            {previewEffects.map((effect) => (
              <li key={effect}>{effect}</li>
            ))}
          </ol>
          <p className="mt-3 text-xs text-muted-foreground">
            The input delivery is ACKed by this commit — <span className="font-medium">no separate ACK</span> should
            follow a success. A matched retry of the same Completion ID returns the original receipt and changes
            nothing.
          </p>
        </Section>

        {resolved && phase.phase === "resolved" && (
          <CompletionResultPanel
            profileId={profileId}
            result={phase.result}
            stateKey={phase.intent.state.key}
            inputQueue={phase.intent.input.queue}
            {...(onOpenQueues ? { onOpenQueues } : {})}
          />
        )}
        {foundReceipt && !resolved && (
          <Section title="Retained receipt">
            <DetailGrid rows={[
              { label: "Status", value: <Badge variant="success" className="font-mono text-xs">Already completed</Badge> },
              { label: "Completion ID", value: foundReceipt.operation_id, mono: true },
              { label: "Commit ID", value: foundReceipt.commit_id, mono: true },
              { label: "State version", value: foundReceipt.state_version, mono: true },
              { label: "Output message", value: foundReceipt.output_message_id === "0" ? "none" : foundReceipt.output_message_id, mono: true },
              { label: "Completed", value: formatTimestamp(Number(foundReceipt.completed_at) / 1000) },
              { label: "Retry window ends", value: formatTimestamp(Number(foundReceipt.receipt_expires_at) / 1000) }
            ]} />
          </Section>
        )}
      </div>
    </div>
  );
}

function CompletionResultPanel({ profileId, result, stateKey, inputQueue, onOpenQueues }: {
  profileId: string;
  result: JobCompletionResult;
  stateKey: string;
  inputQueue: string;
  onOpenQueues?: () => void;
}) {
  return (
    <Section title="Completion receipt">
      <div className="grid gap-2">
        <DetailGrid rows={[
          { label: "Status", value: <Badge variant={result.replayed ? "info" : "success"} className="font-mono text-xs">{receiptStatusWord(result)}</Badge> },
          { label: "Completion ID", value: <CopyId id={result.operation_id} />, mono: true },
          { label: "Commit ID", value: result.commit_id, mono: true },
          { label: "State key", value: stateKey, mono: true },
          { label: "State version", value: result.state.version, mono: true },
          { label: "Output", value: result.output ? `message ${result.output.message_id} in Queue ${result.output.queue}` : "none published", mono: result.output !== null },
          { label: "Completed", value: formatTimestamp(Number(result.completed_at) / 1000) },
          { label: "Retry window ends", value: formatTimestamp(Number(result.receipt_expires_at) / 1000) }
        ]} />
        <p className="text-xs text-muted-foreground">
          {result.replayed
            ? "This is the original receipt from the first commit; the repeated submission changed nothing."
            : "The input delivery was ACKed as part of this commit — sending another ACK for it would be wrong."}
          {" "}The receipt stays retrievable by this Completion ID until the retry window ends, even after a restart.
        </p>
        <div className="flex flex-wrap gap-2 text-xs">
          <a
            className="underline decoration-border underline-offset-4 hover:decoration-foreground"
            href={`#/c/${encodeURIComponent(profileId)}/keyspace/durable`}
          >
            Open Durable state
          </a>
          <a
            className="underline decoration-border underline-offset-4 hover:decoration-foreground"
            href={`#/c/${encodeURIComponent(profileId)}/queues`}
            onClick={() => onOpenQueues?.()}
          >
            Open Queues{inputQueue ? ` (${inputQueue})` : ""}
          </a>
        </div>
        <p className="text-xs text-muted-foreground">
          If the output message has already been consumed downstream, that delivery belonged to another worker; the
          receipt above remains the authoritative record of the completion.
        </p>
      </div>
    </Section>
  );
}

/**
 * Bounded retained completion history plus operation-id lookup that needs no
 * delivery proof — usable after restarts. Deliberately labeled bounded, not a
 * permanent audit log.
 */
export function CompletionHistoryView({ profileId }: { profileId: string }) {
  const { capabilities } = useConnections();
  const capability = jobCompletionCapability(capabilities.get(profileId));
  const [receipts, setReceipts] = useState<CompletionReceipt[]>([]);
  const [meta, setMeta] = useState<CollectionMeta | null>(null);
  const [backStack, setBackStack] = useState<string[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [lookupId, setLookupId] = useState("");
  const [lookupResult, setLookupResult] = useState<CompletionReceipt | "missing" | null>(null);
  const [lookupError, setLookupError] = useState<Error | null>(null);
  const [stateOpId, setStateOpId] = useState("");
  const [stateOpResult, setStateOpResult] = useState<DurableOperationReceipt | "missing" | null>(null);

  const loader = useCallback(async () => {
    const response = await list<CompletionReceipt>(profileId, `job-completions?limit=50${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`);
    setReceipts(response.data);
    setMeta(response.meta);
    setLoaded(true);
  }, [profileId, cursor]);
  const { lastUpdated, error: pollError, stale, refresh } = usePolling(loader, 20_000);

  const lookup = async () => {
    const trimmed = lookupId.trim();
    if (!UUID_PATTERN.test(trimmed)) { setLookupError(new ApiError("validation_failed", "Enter the Completion ID (UUID) to look up.", 400)); return; }
    setLookupError(null);
    try {
      const response = await admin<unknown>(profileId, `job-completions/${trimmed}`);
      const parsed = completionReceiptSchema.safeParse(response.json);
      setLookupResult(parsed.success ? parsed.data : "missing");
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 404) { setLookupResult("missing"); return; }
      setLookupError(reason instanceof Error ? reason : new Error(String(reason)));
    }
  };

  const lookupStateOperation = async () => {
    const trimmed = stateOpId.trim();
    if (!UUID_PATTERN.test(trimmed)) { setLookupError(new ApiError("validation_failed", "Enter the durable operation id (UUID).", 400)); return; }
    setLookupError(null);
    try {
      const response = await admin<unknown>(profileId, `durable-operations/${trimmed}`);
      const parsed = durableOperationReceiptSchema.safeParse(response.json);
      setStateOpResult(parsed.success ? parsed.data : "missing");
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 404) { setStateOpResult("missing"); return; }
      setLookupError(reason instanceof Error ? reason : new Error(String(reason)));
    }
  };

  if (!capability) {
    return (
      <Section title="Completion receipts">
        <EmptyState title="Completion receipts are unavailable on this server." hint="Receipt lookup needs a server with atomic job completion." />
      </Section>
    );
  }

  return (
    <div className="grid gap-6">
      <Section title="Look up a completion">
        <div className="grid gap-3">
          <p className="text-xs text-muted-foreground">
            Lookup by Completion ID never needs the old delivery proof and keeps working after a restart. A missing
            receipt means it was not retained — it never proves the operation did not run.
          </p>
          <div className="flex flex-wrap items-end gap-2">
            <div className="grid flex-1 gap-1.5">
              <Label htmlFor="completion-lookup-id">Completion ID</Label>
              <Input
                id="completion-lookup-id"
                value={lookupId}
                onChange={(event) => setLookupId(event.target.value)}
                placeholder="00000000-0000-0000-0000-000000000000"
                className="font-mono text-xs"
                spellCheck={false}
              />
            </div>
            <Button variant="outline" onClick={() => void lookup()} disabled={lookupId.trim().length === 0}>
              <Search className="size-4 mr-1" />Look up
            </Button>
          </div>
          {lookupError && <ErrorBanner error={lookupError} />}
          {lookupResult === "missing" && (
            <div className="border px-3 py-2.5 text-sm">
              <p className="font-medium">No retained receipt for this Completion ID.</p>
              <p className="mt-0.5 text-muted-foreground">
                The receipt may have expired past its retry window, or the completion never committed. A missing receipt
                is not proof of non-execution.
              </p>
            </div>
          )}
          {lookupResult && lookupResult !== "missing" && (
            <DetailGrid rows={[
              { label: "Status", value: <Badge variant="success" className="font-mono text-xs">Retained</Badge> },
              { label: "Completion ID", value: <CopyId id={lookupResult.operation_id} />, mono: true },
              { label: "Commit ID", value: lookupResult.commit_id, mono: true },
              { label: "State version", value: lookupResult.state_version, mono: true },
              { label: "Output message", value: lookupResult.output_message_id === "0" ? "none" : lookupResult.output_message_id, mono: true },
              { label: "Completed", value: formatTimestamp(Number(lookupResult.completed_at) / 1000) },
              { label: "Retry window ends", value: formatTimestamp(Number(lookupResult.receipt_expires_at) / 1000) }
            ]} />
          )}
        </div>
      </Section>

      <Section title="Durable state mutation receipt">
        <div className="grid gap-3">
          <p className="text-xs text-muted-foreground">
            Direct Durable state puts and deletes (from the Durable state browser) carry their own operation ids; look
            them up here.
          </p>
          <div className="flex flex-wrap items-end gap-2">
            <div className="grid flex-1 gap-1.5">
              <Label htmlFor="durable-op-id">Operation ID</Label>
              <Input id="durable-op-id" value={stateOpId} onChange={(event) => setStateOpId(event.target.value)} className="font-mono text-xs" spellCheck={false} />
            </div>
            <Button variant="outline" onClick={() => void lookupStateOperation()} disabled={stateOpId.trim().length === 0}>
              <Search className="size-4 mr-1" />Look up
            </Button>
          </div>
          {stateOpResult === "missing" && <p className="text-sm text-muted-foreground">No retained receipt for this operation id.</p>}
          {stateOpResult && stateOpResult !== "missing" && (
            <DetailGrid rows={[
              { label: "Kind", value: stateOpResult.kind === "state_put" ? "durable state put" : "durable state delete", mono: true },
              { label: "Operation ID", value: <CopyId id={stateOpResult.operation_id} />, mono: true },
              { label: "Commit ID", value: stateOpResult.commit_id, mono: true },
              { label: "State version", value: stateOpResult.state_version, mono: true },
              { label: "Completed", value: formatTimestamp(Number(stateOpResult.completed_at) / 1000) }
            ]} />
          )}
        </div>
      </Section>

      <div className="border-t border-rule-strong pt-3">
        <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
          <h2 className="text-base font-semibold tracking-[-0.015em]">
            Completion history{" "}
            <span className="ml-2 text-xs font-normal text-muted-foreground">
              (bounded — receipts expire after their retry window; not a permanent audit log)
            </span>
          </h2>
          <LastRefreshed lastUpdated={lastUpdated} onRefresh={refresh} stale={stale} />
        </div>
        {(pollError ?? error) && <ErrorBanner error={pollError ?? error} onRetry={refresh} className="mb-4" />}
        {!loaded ? (
          <div className="grid gap-2" aria-busy="true">
            <span className="sr-only">Loading completion history…</span>
            {Array.from({ length: 4 }, (_, index) => <Skeleton key={index} className="h-11 w-full" />)}
          </div>
        ) : receipts.length === 0 ? (
          <EmptyState title="No retained completion receipts." hint="Committed job completions appear here while their receipts remain retained." />
        ) : (
          <>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Completion ID</TableHead>
                  <TableHead className="text-right">Commit</TableHead>
                  <TableHead className="text-right">State version</TableHead>
                  <TableHead className="text-right">Output</TableHead>
                  <TableHead>Completed</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {receipts.map((receipt) => (
                  <TableRow key={receipt.operation_id}>
                    <TableCell><CopyId id={receipt.operation_id} /></TableCell>
                    <TableCell className="text-right font-mono text-xs tabular-nums">{receipt.commit_id}</TableCell>
                    <TableCell className="text-right font-mono text-xs tabular-nums">{receipt.state_version}</TableCell>
                    <TableCell className="text-right font-mono text-xs tabular-nums">{receipt.output_message_id === "0" ? "—" : receipt.output_message_id}</TableCell>
                    <TableCell className="text-xs text-muted-foreground">{formatTimestamp(Number(receipt.completed_at) / 1000)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <div className="mt-3">
              <CursorPager
                nextCursor={meta?.next_cursor ?? null}
                backStack={backStack}
                onBack={() => {
                  const previous = backStack[backStack.length - 1];
                  if (previous === undefined) return;
                  setBackStack((stack) => stack.slice(0, -1));
                  setCursor(previous);
                }}
                onNext={() => {
                  if (!meta?.next_cursor) return;
                  setBackStack((stack) => [...stack, cursor ?? ""]);
                  setCursor(meta.next_cursor);
                }}
              />
            </div>
          </>
        )}
      </div>
    </div>
  );
}
