import { useState } from "react";
import { ArrowRight, Lock, Plus, Trash2, Unplug } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { ErrorBanner } from "@/components/error-banner";
import { PageHeader } from "@/components/shared";
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
    <div>
      <PageHeader
        title="Connections"
        description="Profiles stay on this device. Tokens are held only in the console gateway's process memory for this browser session — never saved with a profile."
        actions={
          <Button variant="outline" size="sm" onClick={() => void lockAll()}><Lock className="size-4 mr-1" />Lock all</Button>
        }
      >
        <img src="/kuttidb-mark.png" alt="KuttiDB" className="h-20 w-20 mt-4 drop-shadow-sm" draggable={false} />
      </PageHeader>
      <div className="grid gap-4 lg:grid-cols-[minmax(320px,2fr)_minmax(360px,3fr)] items-start">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">Add KuttiDB</CardTitle>
            <CardDescription>The gateway validates the token against /capabilities before storing it.</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-3">
            <div className="grid gap-1.5"><Label htmlFor="conn-label">Label</Label>
              <Input id="conn-label" value={form.label} onChange={(event) => setForm({ ...form, label: event.target.value })} placeholder="Local development" /></div>
            <div className="grid gap-1.5"><Label htmlFor="conn-endpoint">Management API origin</Label>
              <Input id="conn-endpoint" value={form.endpoint} onChange={(event) => setForm({ ...form, endpoint: event.target.value })} spellCheck={false} />
              <p className="text-xs text-muted-foreground">Origin only, e.g. <span className="font-mono">http://127.0.0.1:7380</span></p></div>
            <div className="grid gap-1.5"><Label htmlFor="conn-token">Administrator token</Label>
              <Input id="conn-token" type="password" value={form.token} onChange={(event) => setForm({ ...form, token: event.target.value })} autoComplete="off" spellCheck={false} /></div>
            <label className="flex items-center gap-2 text-sm text-muted-foreground">
              <Switch checked={form.rememberProfile} onCheckedChange={(checked) => setForm({ ...form, rememberProfile: checked })} className="scale-90" />
              Remember this profile (never the token)
            </label>
            {error && <ErrorBanner error={error} />}
            <Button onClick={() => void submitConnect()} disabled={submitting || form.token.length === 0 || form.endpoint.trim().length === 0}>
              {submitting ? "Checking capabilities…" : "Connect to KuttiDB"}
            </Button>
          </CardContent>
        </Card>

        <div className="grid gap-3">
          {profiles.length === 0 && (
            <div className="rounded-xl border border-dashed p-8 text-center text-muted-foreground grid justify-items-center gap-3">
              <img src="/kuttidb-mark.png" alt="" aria-hidden className="h-14 w-14 opacity-90" draggable={false} />
              <span>No saved profiles yet. Add a KuttiDB endpoint to get started.</span>
            </div>
          )}
          {profiles.map((profile) => {
            const connection = live.get(profile.id);
            return (
              <Card key={profile.id}>
                <CardHeader className="pb-1">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <span className={`size-2 rounded-full ${connection ? "bg-chart-3" : "bg-warning"}`} />
                    {profile.label}
                    <Badge variant="outline" className="ml-1 text-[10px] font-normal">{connection ? "Connected" : "Needs token"}</Badge>
                  </CardTitle>
                  <CardDescription className="font-mono text-xs">{profile.endpoint}</CardDescription>
                </CardHeader>
                <CardContent>
                  <p className="text-xs text-muted-foreground mb-3">
                    {connection
                      ? `v${connection.capabilities.server_version} · contract ${connection.capabilities.management_api_contract} · connected ${formatRelative(new Date(connection.connectedAt).getTime())}`
                      : profile.lastConnectedAt ? `Last connected ${formatRelative(new Date(profile.lastConnectedAt).getTime())}` : "Never connected"}
                  </p>
                  <div className="flex flex-wrap gap-2">
                    {connection
                      ? <Button size="sm" onClick={() => onOpenConnection(profile.id)}>Open <ArrowRight className="size-3.5 ml-1" /></Button>
                      : <Button size="sm" variant="outline" onClick={() => { setReconnectFor(profile.id); setReconnectToken(""); }}>Reconnect…</Button>}
                    {connection && <Button size="sm" variant="ghost" onClick={() => void disconnect(profile.id)}><Unplug className="size-3.5 mr-1" />Disconnect</Button>}
                    <Button size="sm" variant="ghost" className="text-destructive" onClick={() => setRemoveFor(profile.id)}><Trash2 className="size-3.5 mr-1" />Remove</Button>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      </div>

      <Dialog open={reconnectFor !== null} onOpenChange={(open) => { if (!open) { setReconnectFor(null); setReconnectError(null); } }}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader><DialogTitle>Reconnect</DialogTitle>
            <DialogDescription>Enter this profile's administrator token. The endpoint stays unchanged.</DialogDescription></DialogHeader>
          <div className="grid gap-3 py-2">
            <Input type="password" value={reconnectToken} onChange={(event) => setReconnectToken(event.target.value)} autoComplete="off" spellCheck={false} placeholder="admin token" />
            {reconnectError && <ErrorBanner error={reconnectError} />}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setReconnectFor(null)}>Cancel</Button>
            <Button onClick={() => void submitReconnect()} disabled={submitting || reconnectToken.length === 0}>{submitting ? "Checking…" : "Reconnect"}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={removeFor !== null} onOpenChange={(open) => { if (!open) setRemoveFor(null); }}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader><DialogTitle>Remove profile</DialogTitle>
            <DialogDescription>Disconnects first and clears this device's stored profile metadata. Database data is untouched.</DialogDescription></DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setRemoveFor(null)}>Cancel</Button>
            <Button variant="destructive" onClick={() => { if (removeFor) void removeProfile(removeFor); setRemoveFor(null); }}>Remove profile</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
