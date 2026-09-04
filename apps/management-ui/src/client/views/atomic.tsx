import { useCallback, useState } from "react";
import { Atom, Braces } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { ErrorBanner } from "@/components/error-banner";
import { DetailGrid, PageHeader } from "@/components/shared";
import { admin, newIdempotencyKey, ApiError } from "@/lib/api";
import { base64FromUtf8, idFromName, isValidB64uId } from "@/lib/codec";
import { useConnections } from "@/state/connections";

type OpSpec = { op: string; needsRoutingKey: boolean; needsValue: boolean; needsBody: boolean };

const OP_SPECS: Record<string, OpSpec> = {
  "put-and-route": { op: "put-and-route", needsRoutingKey: true, needsValue: true, needsBody: false },
  "put-and-enqueue": { op: "put-and-enqueue", needsRoutingKey: false, needsValue: true, needsBody: false },
  "delete-and-route": { op: "delete-and-route", needsRoutingKey: true, needsValue: false, needsBody: true },
  "update-if-present-and-route": { op: "update-if-present-and-route", needsRoutingKey: true, needsValue: true, needsBody: false }
};

export function AtomicView({ profileId }: { profileId: string }) {
  const { capabilities } = useConnections();
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
  const [result, setResult] = useState<{ transaction_id: string; routed_queue_count?: number; durability?: string } | null>(null);

  const spec: OpSpec = OP_SPECS[operation] ?? { op: operation, needsRoutingKey: true, needsValue: true, needsBody: false };
  const keyId = keyText.trim().length > 0 ? idFromName(keyText.trim()) : "";
  const targetId = targetName.trim().length > 0 ? idFromName(targetName.trim()) : "";

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

  const submit = async () => {
    setBusy(true); setError(null); setResult(null);
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
      setResult(response.json.data);
      toast.success(`Transaction committed (${response.json.data.durability ?? "durable"})`);
    } catch (reason) { setError(reason instanceof Error ? reason : new Error(String(reason))); }
    finally { setBusy(false); }
  };

  return (
    <div>
      <PageHeader
        title="Atomic operations"
        description="One native all-or-nothing commit covering a keyspace mutation and a queue/routing delivery."
      />
      <div className="grid gap-4 lg:grid-cols-2 items-start">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center justify-between">Compose operation
              <label className="flex items-center gap-2 text-xs font-normal text-muted-foreground">
                <Braces className="size-3" /> expert JSON <Switch checked={expertMode} onCheckedChange={setExpertMode} className="scale-90" />
              </label>
            </CardTitle>
          </CardHeader>
          <CardContent className="grid gap-3">
            {!expertMode ? (
              <>
                <div className="grid gap-2"><Label>Operation</Label>
                  <Select value={operation} onValueChange={setOperation}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {(supported.length > 0 ? supported : Object.keys(OP_SPECS)).map((op) => <SelectItem key={op} value={op}>{op}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <div className="grid gap-2 sm:grid-cols-2">
                  <div className="grid gap-2"><Label>Cache key (plain text)</Label>
                    <Input value={keyText} onChange={(event) => setKeyText(event.target.value)} spellCheck={false} placeholder="user:42" />
                    {keyId && <p className="text-[11px] font-mono text-muted-foreground">{keyId}</p>}
                  </div>
                  <div className="grid gap-2"><Label>Target queue (name)</Label>
                    <Input value={targetName} onChange={(event) => setTargetName(event.target.value)} spellCheck={false} placeholder="jobs" />
                    {targetId && <p className="text-[11px] font-mono text-muted-foreground">{targetId}</p>}
                  </div>
                </div>
                {spec.needsRoutingKey && (
                  <div className="grid gap-2"><Label>Routing key (Base64 of exact key)</Label>
                    <Input value={routingKey} onChange={(event) => setRoutingKey(event.target.value)} spellCheck={false} placeholder="resize" /></div>
                )}
                {spec.needsValue && (
                  <div className="grid gap-2"><Label>Value = message body (text)</Label>
                    <Input value={valueText} onChange={(event) => setValueText(event.target.value)} spellCheck={false} /></div>
                )}
                {spec.needsBody && (
                  <div className="grid gap-2"><Label>Queue message body (text)</Label>
                    <Input value={bodyText} onChange={(event) => setBodyText(event.target.value)} spellCheck={false} /></div>
                )}
                <div className="grid gap-2"><Label>TTL seconds (optional)</Label>
                  <Input inputMode="numeric" value={ttlSeconds} onChange={(event) => setTtlSeconds(event.target.value)} className="w-40" /></div>
              </>
            ) : (
              <Textarea value={expertJson} onChange={(event) => setExpertJson(event.target.value)} rows={10} spellCheck={false}
                className="font-mono text-xs" placeholder='{"operation":"put-and-enqueue","key_id":"b64u:…","target_id":"b64u:…","value":"…"}' />
            )}
            {error && <ErrorBanner error={error} />}
            <Button onClick={() => void submit()} disabled={busy}>{busy ? "Committing…" : <><Atom className="size-4 mr-1" />Commit atomically</>}</Button>
          </CardContent>
        </Card>
        <div className="grid gap-4">
          <Card>
            <CardHeader className="pb-2"><CardTitle className="text-sm">Transaction preview</CardTitle></CardHeader>
            <CardContent>
              <DetailGrid rows={[
                { label: "Keyspace action", value: operation.startsWith("delete") ? "delete key" : operation.startsWith("update-if-present") ? "conditional put" : "put value", mono: true },
                { label: "Delivery action", value: operation.endsWith("route") ? "routed publish" : "queue enqueue", mono: true },
                { label: "Idempotency", value: "fresh UUID per intent", mono: true }
              ]} />
              <p className="text-xs text-muted-foreground mt-3">Both sides commit under one durable transaction ID: after recovery, both exist or neither does.</p>
            </CardContent>
          </Card>
          {result && (
            <Card>
              <CardHeader className="pb-2"><CardTitle className="text-sm">Result</CardTitle></CardHeader>
              <CardContent>
                <DetailGrid rows={[
                  { label: "Transaction ID", value: result.transaction_id, mono: true },
                  { label: "Routed queues", value: result.routed_queue_count ?? 0 },
                  { label: "Durability", value: <Badge variant="outline" className="font-mono text-[11px]">{result.durability ?? "known"}</Badge> }
                ]} />
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}
