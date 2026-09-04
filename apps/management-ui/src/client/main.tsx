import { createRoot } from "react-dom/client";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Toaster } from "@/components/ui/sonner";
import { ConnectionsProvider } from "@/state/connections";
import { AppShell } from "@/app-shell";
import "@/styles.css";

createRoot(document.getElementById("root")!).render(
  <ConnectionsProvider>
    <TooltipProvider delayDuration={200}>
      <AppShell />
      <Toaster position="bottom-right" />
    </TooltipProvider>
  </ConnectionsProvider>
);
