import { useEffect, useState, type ReactNode } from "react";
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter,
  AlertDialogHeader, AlertDialogTitle
} from "@/components/ui/alert-dialog";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

/**
 * Shared destructive-confirmation flow: the operator must type the exact
 * canonical target ID. `ifMatch` is captured fresh by the caller right before
 * opening, per the shared mutation coordinator.
 */
export function ConfirmDestructive({
  open, onOpenChange, title, description, confirmId, affected, onConfirm, inFlight = false
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: ReactNode;
  confirmId: string;
  affected?: ReactNode;
  onConfirm: () => void;
  inFlight?: boolean;
}) {
  const [typed, setTyped] = useState("");
  useEffect(() => { if (open) setTyped(""); }, [open]);
  const matches = typed === confirmId;
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription asChild>
            <div className="space-y-2">
              <div>{description}</div>
              {affected && <div className="text-foreground font-medium">{affected}</div>}
              <div className="text-xs">
                Type <code className="font-mono bg-muted rounded px-1 py-0.5 select-all">{confirmId}</code> to confirm.
              </div>
              <Input
                value={typed}
                onChange={(event) => setTyped(event.target.value)}
                placeholder={confirmId}
                spellCheck={false}
                autoComplete="off"
                className="font-mono text-xs"
                disabled={inFlight}
              />
            </div>
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={inFlight}>Cancel</AlertDialogCancel>
          <AlertDialogAction
            disabled={!matches || inFlight}
            className={cn("bg-destructive text-destructive-foreground hover:bg-destructive/90")}
            onClick={(event) => { event.preventDefault(); onConfirm(); }}
          >
            {inFlight ? "Working…" : "Confirm"}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
