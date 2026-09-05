import { useState } from "react";
import {
  Database, LayoutDashboard, ListOrdered, Waypoints, Users, Share2, Atom, Wrench,
  ChevronDown, Lock, Menu, Plug, X
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { BrandLockup, StatusDot, ThemeMenu } from "@/components/shared";
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
import { isNavActive, routeSegments } from "@/lib/nav";
import { cn } from "@/lib/utils";

type NavItem = { key: string; label: string; icon: React.ReactNode; requiresEngine?: string; base: string[] };
type NavGroup = { label: string; items: NavItem[] };

const NAV_GROUPS: NavGroup[] = [
  { label: "Server", items: [
    { key: "overview", label: "Overview", icon: <LayoutDashboard className="size-4" />, base: ["overview"] }
  ]},
  { label: "Data", items: [
    { key: "keyspace", label: "Keyspace", icon: <Database className="size-4" />, requiresEngine: "keyspaces", base: ["keyspace"] },
    { key: "queues", label: "Queues", icon: <ListOrdered className="size-4" />, requiresEngine: "queues", base: ["queues"] },
    { key: "streams", label: "Streams", icon: <Waypoints className="size-4" />, requiresEngine: "streams", base: ["streams"] }
  ]},
  { label: "Coordination", items: [
    { key: "groups", label: "Consumer groups", icon: <Users className="size-4" />, requiresEngine: "consumer-groups", base: ["groups"] },
    { key: "routing", label: "Routing", icon: <Share2 className="size-4" />, requiresEngine: "routing", base: ["routing"] }
  ]},
  { label: "Operations", items: [
    { key: "atomic", label: "Atomic operations", icon: <Atom className="size-4" />, requiresEngine: "keyspaces", base: ["operations", "atomic"] },
    { key: "maintenance", label: "Maintenance", icon: <Wrench className="size-4" />, base: ["operations", "maintenance"] }
  ]}
];

/**
 * In-memory only: the locked screen remembers which deep route the operator
 * wanted so reconnect can return there. Never persisted to storage.
 */
let pendingRouteAfterReconnect: string[] | null = null;

export function AppShell() {
  const { profiles, live, capabilities, lockAll } = useConnections();
  const { segments, navigate } = useRoute();
  const [menuOpen, setMenuOpen] = useState(false);

  const activeProfileId = segments[0] === "c" ? segments[1] : undefined;
  const activeProfile = profiles.find((profile) => profile.id === activeProfileId);
  const activeLive = activeProfileId ? live.get(activeProfileId) : undefined;
  const caps = activeProfileId ? capabilities.get(activeProfileId) : undefined;
  const view = segments[2] ?? "overview";

  /** Build an in-app href with the same per-segment encoding the router uses. */
  const hrefFor = (path: string[]) => `#/${routeSegments(activeProfileId ?? "", path).map(encodeURIComponent).join("/")}`;

  /** Navigation callback for views: path is relative to the connection prefix. */
  const go = (path: string[]) => navigate(routeSegments(activeProfileId ?? "", path));

  if (segments.length === 0 || segments[0] === "connections") {
    return (
      <ConnectionsView
        onOpenConnection={(profileId) => {
          const pending = pendingRouteAfterReconnect;
          pendingRouteAfterReconnect = null;
          if (pending && pending[0] === "c" && pending[1] === profileId) navigate(pending);
          else navigate(["c", profileId, "overview"]);
        }}
      />
    );
  }

  if (!activeProfile) {
    return (
      <div className="min-h-dvh bg-background">
        <header className="border-b border-rule-strong">
          <div className="mx-auto flex w-full max-w-[1120px] items-center justify-between gap-4 px-4 py-4 md:px-8">
            <BrandLockup markSize={32} />
            <ThemeMenu />
          </div>
        </header>
        <div className="mx-auto w-full max-w-[720px] px-4 py-16 md:px-8">
          <h1 className="text-[26px] font-semibold tracking-[-0.035em]">Unknown connection</h1>
          <p className="mt-2 max-w-xl text-sm text-muted-foreground">
            This route points at a profile this browser does not have — it may have been removed,
            or the link came from another device. Manage connections to add or reopen a server.
          </p>
          <p className="mt-2 font-mono text-xs text-muted-foreground">Requested route: #/{segments.map(encodeURIComponent).join("/")}</p>
          <Button className="mt-6" onClick={() => navigate(["connections"])}>
            <Plug className="size-4" />Manage connections
          </Button>
        </div>
      </div>
    );
  }

  if (!activeLive) {
    return (
      <div className="min-h-dvh bg-background">
        <header className="border-b border-rule-strong">
          <div className="mx-auto flex w-full max-w-[1120px] items-center justify-between gap-4 px-4 py-4 md:px-8">
            <BrandLockup markSize={32} />
            <ThemeMenu />
          </div>
        </header>
        <div className="mx-auto w-full max-w-[720px] px-4 py-16 md:px-8">
          <p className="text-xs font-medium uppercase tracking-[0.06em] text-muted-foreground">Session locked</p>
          <h1 className="mt-2 text-[26px] font-semibold tracking-[-0.035em]">{activeProfile.label} is locked</h1>
          <p className="mt-2 max-w-xl text-sm text-muted-foreground">
            The gateway session for this profile is missing or expired — tokens live only in gateway
            memory for this browser session. Reconnect to restore it; the profile and the route below
            are preserved.
          </p>
          <p className="mt-2 font-mono text-xs break-all text-muted-foreground">{activeProfile.endpoint}</p>
          <p className="mt-1 font-mono text-xs break-all text-muted-foreground">Route: #/{segments.map(encodeURIComponent).join("/")}</p>
          <div className="mt-6 flex flex-wrap gap-2">
            <Button
              onClick={() => {
                pendingRouteAfterReconnect = segments;
                navigate(["connections"]);
              }}
            >
              <Plug className="size-4" />Reconnect
            </Button>
            <Button variant="outline" onClick={() => navigate(["connections"])}>Manage connections</Button>
          </div>
        </div>
      </div>
    );
  }

  const auditUnhealthy = caps?.audit?.required && caps.audit.healthy === false;

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

  const navContent = (variant: "sidebar" | "drawer") => (
    <>
      <div className="flex items-center justify-between px-4 pt-4 pb-2">
        <BrandLockup markSize={28} />
        {variant === "drawer" && (
          <button
            type="button"
            className="rounded-[4px] p-1.5 text-muted-foreground hover:bg-muted hover:text-foreground"
            aria-label="Close menu"
            onClick={() => setMenuOpen(false)}
          >
            <X className="size-4" />
          </button>
        )}
      </div>
      <div className="px-3 pb-3">
        <ConnectionSwitcher />
      </div>
      <nav aria-label="Console" className="grid flex-1 content-start gap-4 px-3 pb-4">
        {NAV_GROUPS.map((group) => {
          const items = group.items.filter((item) => !item.requiresEngine || (caps?.enabled_engines ?? []).includes(item.requiresEngine));
          if (items.length === 0) return null;
          return (
            <div key={group.label} className="grid gap-0.5">
              <p className="px-3 pb-1 text-[11px] leading-4 font-medium uppercase tracking-[0.06em] text-muted-foreground">{group.label}</p>
              {items.map((item) => {
                // Full-base matching keeps the two Operations entries exclusive
                // while list items stay active on their detail routes.
                const active = isNavActive(item.base, segments);
                return (
                  <a
                    key={item.key}
                    href={hrefFor(item.base)}
                    aria-current={active ? "page" : undefined}
                    onClick={() => setMenuOpen(false)}
                    className={cn(
                      "relative flex min-h-9 items-center gap-2.5 px-3 py-1.5 text-left text-sm transition-colors",
                      active
                        ? "bg-sidebar-primary font-semibold text-sidebar-primary-foreground"
                        : "text-sidebar-foreground/85 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                    )}
                  >
                    {active && <span aria-hidden className="absolute inset-y-1 left-0 w-[3px] bg-brand-orange" />}
                    {item.icon}
                    {item.label}
                  </a>
                );
              })}
            </div>
          );
        })}
      </nav>
      <div className="grid gap-0.5 border-t border-sidebar-border px-3 py-3">
        <a
          href="#/connections"
          onClick={() => setMenuOpen(false)}
          className="flex min-h-9 items-center gap-2.5 px-3 py-1.5 text-sm text-sidebar-foreground/85 transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
        >
          <Plug className="size-4" />Connections
        </a>
        <div className="flex items-center justify-between gap-2 px-3">
          <span className="text-sm text-sidebar-foreground/85">Theme</span>
          <ThemeMenu />
        </div>
        <Button
          variant="ghost"
          className="w-full justify-start text-sidebar-foreground/85 hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
          onClick={() => void lockAll()}
        >
          <Lock className="size-4" />Lock all tokens
        </Button>
      </div>
    </>
  );

  return (
    <Dialog open={menuOpen} onOpenChange={setMenuOpen}>
      <div className="min-h-dvh bg-background">
      {/* Skip link: focuses <main> without touching the hash-router URL. */}
      <a
        href="#main-content"
        onClick={(event) => {
          event.preventDefault();
          const main = document.getElementById("main-content");
          if (main) { main.focus(); main.scrollIntoView(); }
        }}
        className="sr-only focus:not-sr-only focus:fixed focus:top-2 focus:left-2 focus:z-50 focus:bg-primary focus:px-3 focus:py-2 focus:text-sm focus:text-primary-foreground"
      >
        Skip to content
      </a>

      {/* Paper sidebar, 216 px including its right border (≥1200 px). */}
      <aside className="fixed inset-y-0 left-0 z-30 hidden w-[216px] flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground min-[1200px]:flex">
        {navContent("sidebar")}
      </aside>

      <div className="min-[1200px]:pl-[216px]">
        <header className="sticky top-0 z-20 border-b border-border bg-card">
          <div className="flex min-h-14 flex-wrap items-center gap-x-3 gap-y-1 px-4 md:px-6 lg:px-8">
            {/* Real trigger so Radix restores focus here on Close, Escape,
                and navigation dismissal of the drawer. */}
            <DialogTrigger asChild>
              <Button
                variant="ghost"
                size="sm"
                className="min-[1200px]:hidden"
                aria-label="Open menu"
                aria-haspopup="dialog"
              >
                <Menu className="size-4" />Menu
              </Button>
            </DialogTrigger>
            <span className="truncate text-sm font-medium">{activeProfile.label}</span>
            <Badge variant="outline" className="max-w-full font-mono text-xs normal-case">
              <span className="truncate">{activeProfile.endpoint}</span>
            </Badge>
            <span className="ml-auto inline-flex items-center gap-1.5 text-xs text-muted-foreground">
              <StatusDot tone={auditUnhealthy ? "destructive" : "success"} />
              {auditUnhealthy ? "Connected · audit unhealthy" : "Connected"}
            </span>
          </div>
        </header>
        <main id="main-content" tabIndex={-1} className="max-w-[1440px] px-4 py-6 outline-none md:px-6 lg:px-8 lg:pt-8">
          {content}
        </main>
      </div>

      {/* Menu drawer below 1200 px; Radix returns focus to the Menu trigger
          on Close, Escape, and navigation dismissal. */}
      <DialogContent
        showCloseButton={false}
        aria-describedby={undefined}
        className="inset-y-0 left-0 h-dvh max-h-dvh w-72 max-w-[85vw] translate-x-0 translate-y-0 gap-0 overflow-y-auto rounded-none border-r bg-sidebar text-sidebar-foreground p-0 data-[state=closed]:slide-out-to-left data-[state=open]:slide-in-from-left"
      >
        <DialogTitle className="sr-only">Console menu</DialogTitle>
        {navContent("drawer")}
      </DialogContent>
      </div>
    </Dialog>
  );
}

function ConnectionSwitcher() {
  const { profiles, live } = useConnections();
  const { segments, navigate } = useRoute();
  const activeProfileId = segments[0] === "c" ? segments[1] : undefined;
  const activeProfile = profiles.find((profile) => profile.id === activeProfileId);
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className="w-full justify-between bg-sidebar-primary/60 border-sidebar-border text-sidebar-foreground hover:bg-sidebar-accent"
          aria-label={`Connection: ${activeProfile ? activeProfile.label : "none"}`}
        >
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
              <StatusDot tone={isLive ? "success" : "neutral"} />
              <span className="truncate">{profile.label}</span>
              {version && <Badge variant="outline" className="ml-auto font-mono text-xs">v{version}</Badge>}
            </DropdownMenuItem>
          );
        })}
        {profiles.length === 0 && (
          <DropdownMenuItem disabled>No saved profiles</DropdownMenuItem>
        )}
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={() => navigate(["connections"])}><Plug className="size-4" />Manage connections…</DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

export function useCurrentCapabilities(profileId: string | undefined) {
  const { capabilities } = useConnections();
  return profileId ? capabilities.get(profileId) : undefined;
}
