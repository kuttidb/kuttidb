import { useEffect, useState } from "react";
import {
  Database, LayoutDashboard, ListOrdered, Waypoints, Users, Share2, Atom, Wrench, Plug,
  ChevronDown, Lock, Moon, Sun
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ConnectionsView } from "@/views/connections";
import { OverviewView } from "@/views/overview";
import { KeyspaceView } from "@/views/keyspace";
import { QueuesView, QueueDetailView } from "@/views/queues";
import { StreamsView, StreamDetailView } from "@/views/streams";
import { ConsumerGroupsView, GroupDetailView } from "@/views/groups";
import { RoutingView, RouterDetailView } from "@/views/routing";
import { AtomicView } from "@/views/atomic";
import { MaintenanceView } from "@/views/maintenance";
import { useConnections } from "@/state/connections";
import { useRoute } from "@/hooks/use-route";
import { cn } from "@/lib/utils";

type NavItem = { key: string; label: string; icon: React.ReactNode; requiresEngine?: string; base: string[] };

const NAV_ITEMS: NavItem[] = [
  { key: "overview", label: "Overview", icon: <LayoutDashboard className="size-4" />, base: ["overview"] },
  { key: "keyspace", label: "Keyspace", icon: <Database className="size-4" />, requiresEngine: "keyspaces", base: ["keyspace"] },
  { key: "queues", label: "Queues", icon: <ListOrdered className="size-4" />, requiresEngine: "queues", base: ["queues"] },
  { key: "streams", label: "Streams", icon: <Waypoints className="size-4" />, requiresEngine: "streams", base: ["streams"] },
  { key: "groups", label: "Consumer Groups", icon: <Users className="size-4" />, requiresEngine: "consumer-groups", base: ["groups"] },
  { key: "routing", label: "Routing", icon: <Share2 className="size-4" />, requiresEngine: "routing", base: ["routing"] },
  { key: "atomic", label: "Atomic Ops", icon: <Atom className="size-4" />, requiresEngine: "keyspaces", base: ["operations", "atomic"] },
  { key: "maintenance", label: "Maintenance", icon: <Wrench className="size-4" />, base: ["operations", "maintenance"] }
];

export function AppShell() {
  const { profiles, live, capabilities, lockAll } = useConnections();
  const { segments, navigate } = useRoute();
  const [dark, setDark] = useState(() => window.matchMedia("(prefers-color-scheme: dark)").matches);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
  }, [dark]);

  const activeProfileId = segments[0] === "c" ? segments[1] : undefined;
  const activeProfile = profiles.find((profile) => profile.id === activeProfileId);
  const activeLive = activeProfileId ? live.get(activeProfileId) : undefined;
  const caps = activeProfileId ? capabilities.get(activeProfileId) : undefined;
  const view = segments[2] ?? "overview";

  const go = (path: string[]) => navigate(["c", activeProfileId ?? "", ...path].filter((segment) => segment.length > 0));

  if (segments.length === 0 || segments[0] === "connections") {
    return <ConnectionsView onOpenConnection={(profileId) => navigate(["c", profileId, "overview"])} />;
  }

  if (!activeProfile) {
    return <div className="p-8 text-muted-foreground text-sm">Unknown profile. <button className="underline" onClick={() => navigate(["connections"])}>Manage connections</button></div>;
  }

  if (!activeLive) {
    return (
      <div className="p-8 max-w-md">
        <h1 className="text-lg font-semibold mb-1">{activeProfile.label} is locked</h1>
        <p className="text-sm text-muted-foreground mb-4">The gateway session is missing or expired. Reconnect to restore this route — the profile and navigation are preserved.</p>
        <Button onClick={() => navigate(["connections"])}><Plug className="size-4 mr-1" />Manage connections</Button>
      </div>
    );
  }

  const content = (() => {
    switch (view) {
      case "overview": return <OverviewView profileId={activeProfile.id} onNavigate={go} />;
      case "keyspace": return <KeyspaceView profileId={activeProfile.id} />;
      case "queues":
        return segments[3]
          ? <QueueDetailView profileId={activeProfile.id} queueId={decodeURIComponent(segments[3])} onBack={() => go(["queues"])} />
          : <QueuesView profileId={activeProfile.id} onOpenQueue={(queueId) => go(["queues", queueId])} />;
      case "streams":
        return segments[3]
          ? <StreamDetailView profileId={activeProfile.id} streamId={decodeURIComponent(segments[3])} onBack={() => go(["streams"])} />
          : <StreamsView profileId={activeProfile.id} onOpenStream={(streamId) => go(["streams", streamId])} />;
      case "groups":
        return segments[3] && segments[4]
          ? <GroupDetailView profileId={activeProfile.id} streamId={decodeURIComponent(segments[3])} groupId={decodeURIComponent(segments[4])} onBack={() => go(["groups"])} />
          : <ConsumerGroupsView profileId={activeProfile.id} onOpenGroup={(streamId, groupId) => go(["groups", streamId, groupId])} />;
      case "routing":
        return segments[3]
          ? <RouterDetailView profileId={activeProfile.id} routerId={decodeURIComponent(segments[3])} onBack={() => go(["routing"])} />
          : <RoutingView profileId={activeProfile.id} onOpenRouter={(routerId) => go(["routing", routerId])} />;
      case "operations":
        return segments[3] === "maintenance" ? <MaintenanceView profileId={activeProfile.id} /> : <AtomicView profileId={activeProfile.id} />;
      default: return <OverviewView profileId={activeProfile.id} onNavigate={go} />;
    }
  })();

  return (
    <div className="flex min-h-screen">
      <aside className="w-60 shrink-0 bg-sidebar text-sidebar-foreground flex flex-col fixed inset-y-0 left-0">
        <div className="px-5 py-4 flex items-center gap-3">
          <img src="/kuttidb-mark.png" alt="KuttiDB" className="h-12 w-12 rounded-xl bg-card p-1" draggable={false} />
          <div className="min-w-0">
            <p className="font-semibold tracking-tight text-sm leading-tight">KuttiDB Console</p>
            <p className="text-xs text-sidebar-foreground/60 truncate">contract {caps?.management_api_contract ?? "1.0"}</p>
          </div>
        </div>
        <div className="px-3 pb-3">
          <ConnectionSwitcher />
        </div>
        <nav className="px-3 grid gap-0.5 flex-1 content-start">
          {NAV_ITEMS.map((item) => {
            const engineEnabled = !item.requiresEngine || (caps?.enabled_engines ?? []).includes(item.requiresEngine);
            if (!engineEnabled) return null;
            const active = view === item.base[0] || (item.base.length > 1 && segments[2] === item.base[0] && segments[3] === item.base[1]);
            return (
              <button key={item.key} onClick={() => go(item.base)}
                className={cn(
                  "flex items-center gap-2.5 rounded-md px-3 py-2 text-sm transition-colors text-left",
                  active ? "bg-sidebar-primary text-sidebar-primary-foreground font-medium" : "text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                )}>
                {item.icon}
                {item.label}
              </button>
            );
          })}
        </nav>
        <div className="px-3 py-3 border-t border-sidebar-border">
          <Button variant="ghost" size="sm" className="w-full justify-start text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground" onClick={() => void lockAll()}>
            <Lock className="size-4 mr-2" />Lock all tokens
          </Button>
        </div>
      </aside>

      <div className="flex-1 ml-60">
        <header className="h-14 border-b bg-card/60 backdrop-blur sticky top-0 z-10 flex items-center px-6 gap-3">
          <span className="text-sm text-muted-foreground">{activeProfile.label}</span>
          <Badge variant="outline" className="font-mono text-[10px]">{activeProfile.endpoint}</Badge>
          <span className={`ml-2 size-2 rounded-full ${caps?.audit?.healthy === false ? "bg-destructive" : "bg-chart-3"}`} title={caps?.audit?.healthy === false ? "Audit unhealthy — mutations blocked" : "Healthy"} />
          <div className="ml-auto flex items-center gap-1">
            <Tooltip><TooltipTrigger asChild>
              <Button variant="ghost" size="icon" className="size-8" onClick={() => setDark((value) => !value)}>
                {dark ? <Sun className="size-4" /> : <Moon className="size-4" />}
              </Button>
            </TooltipTrigger><TooltipContent>Toggle theme</TooltipContent></Tooltip>
            <Button variant="ghost" size="sm" onClick={() => navigate(["connections"])}><Plug className="size-4 mr-1" />Connections</Button>
          </div>
        </header>
        <main className="p-6 max-w-[1400px]">{content}</main>
      </div>
    </div>
  );
}

function ConnectionSwitcher() {
  const { profiles, live, capabilities } = useConnections();
  const { segments, navigate } = useRoute();
  const activeProfileId = segments[0] === "c" ? segments[1] : undefined;
  const activeProfile = profiles.find((profile) => profile.id === activeProfileId);
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="w-full justify-between bg-sidebar-accent/60 border-sidebar-border text-sidebar-foreground hover:bg-sidebar-accent">
          <span className="truncate">{activeProfile ? activeProfile.label : "Choose connection"}</span>
          <ChevronDown className="size-3.5 opacity-70" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent side="right" align="start" className="w-64">
        <DropdownMenuLabel>Connections</DropdownMenuLabel>
        {profiles.map((profile) => {
          const isLive = live.has(profile.id);
          const version = live.get(profile.id)?.capabilities.server_version;
          return (
            <DropdownMenuItem key={profile.id} onClick={() => navigate(isLive ? ["c", profile.id, "overview"] : ["connections"])}>
              <span className={`size-2 rounded-full mr-2 ${isLive ? "bg-chart-3" : "bg-warning"}`} />
              <span className="truncate">{profile.label}</span>
              {version && <Badge variant="outline" className="ml-auto text-[10px] font-mono">v{version}</Badge>}
            </DropdownMenuItem>
          );
        })}
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={() => navigate(["connections"])}><Plug className="size-4 mr-2" />Manage connections…</DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

export function useCurrentCapabilities(profileId: string | undefined) {
  const { capabilities } = useConnections();
  return profileId ? capabilities.get(profileId) : undefined;
}
