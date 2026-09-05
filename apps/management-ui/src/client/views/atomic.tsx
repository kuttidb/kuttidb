import { useCallback, useState } from "react";
import { Atom, Braces } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { ErrorBanner } from "@/components/error-banner";
import { DetailGrid, PageHeader, Section } from "@/components/shared";
import { admin, newIdempotencyKey, ApiError } from "@/lib/api";
import { base64FromUtf8, idFromName, isValidB64uId } from "@/lib/codec";
import { validateAtomicInput, mutationOutcome, type AtomicFieldErrors, type MutationOutcome } from "@/lib/atomic-form";
import { useConnections } from "@/state/connections";

type OpSpec = { op: string; needsRoutingKey: boolean; needsValue: boolean; needsBody: boolean };

const OP_SPECS: Record<string, OpSpec> = {
  "put-and-route": { op: "put-and-route", needsRoutingKey: true, needsValue: true, needsBody: false },
  "put-and-enqueue": { op: "put-and-enqueue", needsRoutingKey: false, needsValue: true, needsBody: false },
  "delete-and-route": { op: "delete-and-route", needsRoutingKey: true, needsValue: false, needsBody: true },
  "update-if-present-and-route": { op: "update-if-present-and-route", needsRoutingKey: true, needsValue: true, needsBody: false }
};

export function AtomicView({ profileId }: { profileId: string }) {
  const { capabilities, mutationsBlocked } = useConnections();
  const supported = capabilities.get(profileId)?.operations.atomic_operations ?? [];
  const [operation, setOperation] = useState<string>(supported[0] ?? "put-and-enqueue");
  const [keyText, setKeyText] = useState("");
  const [targetName, setTargetName] = useState("");
  const [routingKey, setRoutingKey] = useState("");
  const [valueText, setValueText] = useState("");
  const [bodyText, setBodyText] = useState("");
  const [ttlSeconds, setTtlSeconds] = useState("");
  const [expertMode, setExpertMode] = useState(false);
  const [expertJson, setExpertJson] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [fieldErrors, setFieldErrors] = useState<AtomicFieldErrors>({});
  const [outcome, setOutcome] = useState<MutationOutcome>("clean");
  const [result, setResult] = useState<{ transaction_id: string; routed_queue_count?: number; durability?: string } | null>(null);

  const spec: OpSpec = OP_SPECS[operation] ?? { op: operation, needsRoutingKey: true, needsValue: true, needsBody: false };
  const keyId = keyText.trim().length > 0 ? idFromName(keyText.trim()) : "";
  const targetId = targetName.trim().length > 0 ? idFromName(targetName.trim()) : "";
  const blocked = mutationsBlocked(profileId);

  const structuredBody = useCallback((): { valid: boolean; body?: Record<string, unknown>; problem?: string } => {
    if (!isValidB64uId(keyId)) return { valid: false, problem: "Key must be plain text or a valid b64u identifier." };
    if (!isValidB64uId(targetId)) return { valid: false, problem: "Target queue must be plain text or a valid b64u identifier." };
    if (spec.needsRoutingKey && routingKey.trim().length === 0) return { valid: false, problem: "This operation requires a routing key." };
    if (spec.needsValue && valueText.length === 0) return { valid: false, problem: "This operation requires a value." };
    if (spec.needsBody && bodyText.length === 0) return { valid: false, problem: "This operation requires a body." };
    const body: Record<string, unknown> = { operation, key_id: keyId, target_id: targetId };
    if (spec.needsRoutingKey) body.routing_key = base64FromUtf8(routingKey.trim());
    if (spec.needsValue) body.value = base64FromUtf8(valueText);
    if (spec.needsBody) body.body = base64FromUtf8(bodyText);
    if (ttlSeconds.trim().length > 0) body.ttl_ms = Math.round(Number(ttlSeconds) * 1000);
    return { valid: true, body };
  }, [operation, keyId, targetId, spec, routingKey, valueText, bodyText, ttlSeconds]);

  const FIELD_IDS = { key: "atomic-key", target: "atomic-target", routingKey: "atomic-routing-key", value: "atomic-value", body: "atomic-body" } as const;

  const submit = async () => {
    setError(null); setResult(null);
    if (!expertMode) {
      const errors = validateAtomicInput({ operation, keyText, targetName, routingKey, valueText, bodyText }, spec, idFromName, isValidB64uId);
      setFieldErrors(errors);
      const firstInvalid = (Object.keys(FIELD_IDS) as (keyof typeof FIELD_IDS)[]).find((field) => errors[field]);
      if (firstInvalid) {
        document.getElementById(FIELD_IDS[firstInvalid])?.focus();
        return;
      }
    }
    setBusy(true);
    try {
      let body: Record<string, unknown>;
      if (expertMode) {
        body = JSON.parse(expertJson) as Record<string, unknown>;
      } else {
        const composed = structuredBody();
        if (!composed.valid || !composed.body) throw new ApiError("validation_failed", composed.problem ?? "Invalid input.", 400);
        body = composed.body;
      }
      const response = await admin<{ data: { transaction_id: string; routed_queue_count?: number; durability?: string } }>(profileId, "atomic-operations", {
        method: "POST", idempotencyKey: newIdempotencyKey(), body
      });
      setOutcome("clean");
      setResult(response.json.data);
      toast.success(`Transaction committed (${response.json.data.durability ?? "durable"})`);
    } catch (reason) {
      const classified = mutationOutcome(reason);
      setOutcome(classified);
      setError(reason instanceof Error ? reason : new Error(String(reason)));
      // For operation_in_doubt the Commit control stays disabled below until
      // the operator acknowledges the unresolved outcome and starts a new
      // intent — retries are never automatic.
    }
    finally { setBusy(false); }
  };

  /** Deliberate reconciliation acknowledgement; never an automatic retry. */
  const acknowledgeInDoubt = () => {
    setOutcome("clean");
    setError(null);
  };

  return (
    <div>
      <PageHeader
        title="Atomic operations"
        description="One native all-or-nothing commit covering a keyspace mutation and a queue/routing delivery."
      />
      {blocked && (
        <ErrorBanner
          error={new ApiError("audit_unavailable", "The audit trail is unhealthy. Mutations — including atomic operations — are blocked for this connection; reads remain available.", 503)}
          className="mb-4"
        />
      )}
      <div className="grid grid-cols-1 items-start gap-6 lg:grid-cols-[3fr_2fr] lg:gap-8">
        <Section
          title="Compose operation"
          actions={
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <Braces className="size-3" /> expert JSON
              <Switch checked={expertMode} onCheckedChange={setExpertMode} aria-label="Expert JSON mode" />
            </label>
          }
        >
          <div className="grid gap-3">
            {!expertMode ? (
              <>
                <div className="grid gap-1.5"><Label htmlFor="atomic-operation">Operation</Label>
                  <Select value={operation} onValueChange={setOperation}>
                    <SelectTrigger id="atomic-operation"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {(supported.length > 0 ? supported : Object.keys(OP_SPECS)).map((op) => <SelectItem key={op} value={op}>{op}</SelectItem>)}
                    </SelectContent>
                  </Select>
                  {supported.length === 0 && (
                    <p className="text-xs text-warning">The server did not advertise supported atomic operations; the list may not match this target.</p>
                  )}
                </div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <div className="grid gap-1.5"><Label htmlFor="atomic-key">Cache key (plain text)</Label>
                    <Input id="atomic-key" value={keyText} onChange={(event) => setKeyText(event.target.value)} spellCheck={false} placeholder="user:42"
                      aria-invalid={fieldErrors.key ? true : undefined} aria-describedby={fieldErrors.key ? "atomic-key-error" : keyId ? "atomic-key-id" : undefined} />
                    {keyId && <p id="atomic-key-id" className="font-mono text-xs break-all text-muted-foreground">{keyId}</p>}
                    {fieldErrors.key && <p id="atomic-key-error" className="text-xs text-destructive">{fieldErrors.key}</p>}
                  </div>
                  <div className="grid gap-1.5"><Label htmlFor="atomic-target">Target queue (name)</Label>
                    <Input id="atomic-target" value={targetName} onChange={(event) => setTargetName(event.target.value)} spellCheck={false} placeholder="jobs"
                      aria-invalid={fieldErrors.target ? true : undefined} aria-describedby={fieldErrors.target ? "atomic-target-error" : targetId ? "atomic-target-id" : undefined} />
                    {targetId && <p id="atomic-target-id" className="font-mono text-xs break-all text-muted-foreground">{targetId}</p>}
                    {fieldErrors.target && <p id="atomic-target-error" className="text-xs text-destructive">{fieldErrors.target}</p>}
                  </div>
                </div>
                {spec.needsRoutingKey && (
                  <div className="grid gap-1.5"><Label htmlFor="atomic-routing-key">Routing key (exact key, Base64-encoded)</Label>
                    <Input id="atomic-routing-key" value={routingKey} onChange={(event) => setRoutingKey(event.target.value)} spellCheck={false} placeholder="resize"
                      aria-invalid={fieldErrors.routingKey ? true : undefined} aria-describedby={fieldErrors.routingKey ? "atomic-routing-key-error" : undefined} />
                    {fieldErrors.routingKey && <p id="atomic-routing-key-error" className="text-xs text-destructive">{fieldErrors.routingKey}</p>}
                  </div>
                )}
                {spec.needsValue && (
                  <div className="grid gap-1.5"><Label htmlFor="atomic-value">Value = message body (text)</Label>
                    <Input id="atomic-value" value={valueText} onChange={(event) => setValueText(event.target.value)} spellCheck={false}
                      aria-invalid={fieldErrors.value ? true : undefined} aria-describedby={fieldErrors.value ? "atomic-value-error" : undefined} />
                    {fieldErrors.value && <p id="atomic-value-error" className="text-xs text-destructive">{fieldErrors.value}</p>}
                  </div>
                )}
                {spec.needsBody && (
                  <div className="grid gap-1.5"><Label htmlFor="atomic-body">Queue message body (text)</Label>
                    <Input id="atomic-body" value={bodyText} onChange={(event) => setBodyText(event.target.value)} spellCheck={false}
                      aria-invalid={fieldErrors.body ? true : undefined} aria-describedby={fieldErrors.body ? "atomic-body-error" : undefined} />
                    {fieldErrors.body && <p id="atomic-body-error" className="text-xs text-destructive">{fieldErrors.body}</p>}
                  </div>
                )}
                <div className="grid max-w-40 gap-1.5"><Label htmlFor="atomic-ttl">TTL seconds (optional)</Label>
                  <Input id="atomic-ttl" inputMode="numeric" value={ttlSeconds} onChange={(event) => setTtlSeconds(event.target.value)} /></div>
              </>
            ) : (
              <div className="grid gap-1.5">
                <Label htmlFor="atomic-expert">Request body (JSON)</Label>
                <Textarea id="atomic-expert" value={expertJson} onChange={(event) => setExpertJson(event.target.value)} rows={10} spellCheck={false}
                  className="font-mono text-xs" placeholder='{"operation":"put-and-enqueue","key_id":"b64u:…","target_id":"b64u:…","value":"…"}' />
              </div>
            )}
            {outcome === "in_doubt" ? (
              <div role="alert" className="border border-warning/40 bg-warning-surface px-3 py-2.5 text-sm">
                <p className="font-medium text-warning">Operation outcome unknown — reconcile before retrying.</p>
                <p className="mt-0.5 text-muted-foreground">
                  The commit may or may not have taken effect. Check whether the key and the queue delivery exist
                  (Keyspace and the target queue), then acknowledge below to compose a fresh intent. Nothing is
                  resubmitted automatically.
                </p>
                {error && <div className="mt-1.5"><ErrorBanner error={error} /></div>}
                <Button variant="outline" size="sm" className="mt-2" onClick={acknowledgeInDoubt}>
                  I have reconciled this outcome — start a new intent
                </Button>
              </div>
            ) : (
              <>
                {error && <ErrorBanner error={error} />}
                <div>
                  <Button onClick={() => void submit()} disabled={busy || blocked}>
                    {busy ? "Committing…" : <><Atom className="size-4" />Commit atomically</>}
                  </Button>
                </div>
              </>
            )}
          </div>
        </Section>
        <div className="grid gap-6">
          <Section title="Transaction preview">
            <DetailGrid rows={[
              { label: "Keyspace action", value: operation.startsWith("delete") ? "delete key" : operation.startsWith("update-if-present") ? "conditional put" : "put value", mono: true },
              { label: "Delivery action", value: operation.endsWith("route") ? "routed publish" : "queue enqueue", mono: true }
            ]} />
            <p className="mt-3 text-xs text-muted-foreground">Both sides commit under one durable transaction ID: after recovery, both exist or neither does. Each commit sends its own idempotency key, so a repeated intent is detected server-side.</p>
          </Section>
          {result && (
            <Section title="Result">
              <DetailGrid rows={[
                { label: "Transaction ID", value: result.transaction_id, mono: true },
                { label: "Routed queues", value: result.routed_queue_count ?? 0 },
                { label: "Durability", value: <Badge variant="outline" className="font-mono text-xs">{result.durability ?? "known"}</Badge> }
              ]} />
            </Section>
          )}
        </div>
      </div>
    </div>
  );
}
