import { useState } from "react";
import { ArrowRight, Lock, Trash2, Unplug } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { ErrorBanner } from "@/components/error-banner";
import { EmptyState, Section, StandaloneFrame, StatusDot } from "@/components/shared";
import { useConnections } from "@/state/connections";
import { formatRelative } from "@/lib/format";

export function ConnectionsView({ onOpenConnection }: { onOpenConnection: (profileId: string) => void }) {
  const { profiles, live, connect, reconnect, disconnect, removeProfile, lockAll } = useConnections();
  const [form, setForm] = useState({ label: "", endpoint: "http://127.0.0.1:7380", token: "", rememberProfile: true });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [reconnectFor, setReconnectFor] = useState<string | null>(null);
  const [reconnectToken, setReconnectToken] = useState("");
  const [reconnectError, setReconnectError] = useState<Error | null>(null);
  const [removeFor, setRemoveFor] = useState<string | null>(null);

  const submitConnect = async () => {
    setSubmitting(true); setError(null);
    try {
      const profileId = crypto.randomUUID();
      await connect({ profileId, label: form.label.trim() || form.endpoint, endpoint: form.endpoint.trim(), token: form.token, rememberProfile: form.rememberProfile });
      toast.success("Connected");
      setForm((current) => ({ ...current, label: "", token: "" }));
      onOpenConnection(profileId);
    } catch (reason) {
      setError(reason instanceof Error ? reason : new Error(String(reason)));
    } finally { setSubmitting(false); }
  };

  const submitReconnect = async () => {
    if (!reconnectFor) return;
    setSubmitting(true); setReconnectError(null);
    try {
      await reconnect(reconnectFor, reconnectToken);
      toast.success("Reconnected");
      setReconnectFor(null); setReconnectToken("");
      onOpenConnection(reconnectFor);
    } catch (reason) {
      setReconnectError(reason instanceof Error ? reason : new Error(String(reason)));
    } finally { setSubmitting(false); }
  };

  return (
    <StandaloneFrame wide>
      <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
        <div className="min-w-0">
          {/* The StandaloneFrame header already shows the mascot — at most one
              per visible screen. */}
          <h1 className="text-[26px] leading-8 font-semibold tracking-[-0.035em]">Connections</h1>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">Connect this console to a KuttiDB gateway and manage the profiles saved on this device.</p>
        </div>
        <Button variant="outline" size="sm" onClick={() => void lockAll()}><Lock className="size-4" /> Lock all tokens</Button>
      </div>

      <div className="mt-8 grid grid-cols-1 items-start gap-8 min-[900px]:grid-cols-[minmax(0,400px)_minmax(0,1fr)]">
        <Section title="Add KuttiDB" className="max-w-[400px]">
          <div className="grid gap-4">
            <p className="text-xs text-muted-foreground">The gateway validates the token against <span className="font-mono">/capabilities</span> before storing it.</p>
            <div className="grid gap-1.5">
              <Label htmlFor="conn-label">Connection name</Label>
              <Input id="conn-label" value={form.label} onChange={(event) => setForm({ ...form, label: event.target.value })} placeholder="Local development" />
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor="conn-endpoint">Management endpoint</Label>
              <Input id="conn-endpoint" value={form.endpoint} onChange={(event) => setForm({ ...form, endpoint: event.target.value })} spellCheck={false} />
              <p className="text-xs text-muted-foreground">Origin only, e.g. <span className="font-mono">http://127.0.0.1:7380</span></p>
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor="conn-token">Administrator token</Label>
              <Input id="conn-token" type="password" value={form.token} onChange={(event) => setForm({ ...form, token: event.target.value })} autoComplete="off" spellCheck={false} />
            </div>
            <label className="flex items-center gap-2 text-sm text-muted-foreground">
              <Switch checked={form.rememberProfile} onCheckedChange={(checked) => setForm({ ...form, rememberProfile: checked })} className="scale-90" />
              Remember this profile (never the token)
            </label>
            <p className="text-xs text-muted-foreground">Profiles are saved on this device. Tokens stay in gateway memory for this browser session and are never saved with a profile.</p>
            {error && <ErrorBanner error={error} />}
            <Button onClick={() => void submitConnect()} disabled={submitting || form.token.length === 0 || form.endpoint.trim().length === 0}>
              {submitting ? "Checking capabilities…" : "Connect to KuttiDB"}
            </Button>
          </div>
        </Section>

        <div className="min-[900px]:border-l min-[900px]:border-rule-strong min-[900px]:pl-8">
          {profiles.length === 0 ? (
            <EmptyState title="No saved profiles yet." hint="Add a KuttiDB endpoint to get started." className="px-0 py-6" />
          ) : (
            <ul className="border-t border-rule-strong">
              {profiles.map((profile) => {
                const connection = live.get(profile.id);
                return (
                  <li key={profile.id} className="border-b border-rule-strong py-4">
                    <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                          <span className="text-sm font-medium">{profile.label}</span>
                          <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                            <StatusDot tone={connection ? "success" : "neutral"} />
                            {connection ? "Connected" : "Needs token"}
                          </span>
                        </div>
                        <p className="mt-0.5 font-mono text-xs break-all">{profile.endpoint}</p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          {connection
                            ? `v${connection.capabilities.server_version} · contract ${connection.capabilities.management_api_contract} · connected ${formatRelative(new Date(connection.connectedAt).getTime())}`
                            : profile.lastConnectedAt ? `Last connected ${formatRelative(new Date(profile.lastConnectedAt).getTime())}` : "Never connected"}
                        </p>
                      </div>
                      <div className="flex flex-wrap items-center gap-2">
                        {connection
                          ? <Button size="sm" onClick={() => onOpenConnection(profile.id)}>Open <ArrowRight className="size-3.5" /></Button>
                          : <Button size="sm" variant="outline" onClick={() => { setReconnectFor(profile.id); setReconnectToken(""); }}>Reconnect…</Button>}
                        {connection && <Button size="sm" variant="ghost" onClick={() => void disconnect(profile.id)}><Unplug className="size-3.5" />Disconnect</Button>}
                        <Button size="sm" variant="ghost" className="text-destructive" onClick={() => setRemoveFor(profile.id)}><Trash2 className="size-3.5" />Remove</Button>
                      </div>
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </div>

      <Dialog open={reconnectFor !== null} onOpenChange={(open) => { if (!open) { setReconnectFor(null); setReconnectError(null); } }}>
        <DialogContent className="sm:max-w-[480px]">
          <DialogHeader><DialogTitle>Reconnect</DialogTitle>
            <DialogDescription>Enter this profile's administrator token. The endpoint stays unchanged.</DialogDescription></DialogHeader>
          <div className="grid gap-3 py-2">
            <div className="grid gap-1.5">
              <Label htmlFor="reconnect-token">Administrator token</Label>
              <Input id="reconnect-token" type="password" value={reconnectToken} onChange={(event) => setReconnectToken(event.target.value)} autoComplete="off" spellCheck={false} placeholder="admin token" />
            </div>
            {reconnectError && <ErrorBanner error={reconnectError} />}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setReconnectFor(null)}>Cancel</Button>
            <Button onClick={() => void submitReconnect()} disabled={submitting || reconnectToken.length === 0}>{submitting ? "Checking…" : "Reconnect"}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={removeFor !== null} onOpenChange={(open) => { if (!open) setRemoveFor(null); }}>
        <DialogContent className="sm:max-w-[480px]">
          <DialogHeader><DialogTitle>Remove profile</DialogTitle>
            <DialogDescription>Disconnects first and clears this device's stored profile metadata. Database data is untouched.</DialogDescription></DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setRemoveFor(null)}>Cancel</Button>
            <Button variant="destructive" onClick={() => { if (removeFor) void removeProfile(removeFor); setRemoveFor(null); }}>Remove profile</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </StandaloneFrame>
  );
}
