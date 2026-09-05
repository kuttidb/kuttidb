import { useEffect, useState, type ReactNode } from "react";
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter,
  AlertDialogHeader, AlertDialogTitle
} from "@/components/ui/alert-dialog";
import { ErrorBanner } from "@/components/error-banner";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

/**
 * Shared destructive-confirmation flow: the operator types the exact canonical
 * target ID. `ifMatch` is captured fresh by the caller right before opening.
 * Cancel holds initial focus; the final button uses a specific verb label.
 * `context` names the connection and resource inside the modal; `error` keeps
 * a rejected or uncertain outcome's explanation visible until it is resolved.
 */
export function ConfirmDestructive({
  open, onOpenChange, title, description, confirmId, affected, context, error, confirmLabel = "Confirm", onConfirm, inFlight = false
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: ReactNode;
  confirmId: string;
  affected?: ReactNode;
  /** Connection identity shown so the target server is unambiguous. */
  context?: ReactNode;
  /** Failure or unknown-outcome feedback rendered inside the active dialog. */
  error?: unknown;
  /** Specific final action label, e.g. `Purge messages` — never bare `Confirm`. */
  confirmLabel?: string;
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
              {affected && <div className="font-medium text-foreground">{affected}</div>}
              {context && <div className="text-xs text-muted-foreground">{context}</div>}
              {error ? <ErrorBanner error={error} /> : null}
              <div className="text-xs">
                Type <code className="rounded-[2px] bg-muted px-1 py-0.5 font-mono select-all">{confirmId}</code> to confirm.
              </div>
              <Input
                value={typed}
                onChange={(event) => setTyped(event.target.value)}
                placeholder={confirmId}
                aria-label={`Confirmation ID for ${title}`}
                spellCheck={false}
                autoComplete="off"
                className="font-mono text-xs"
                disabled={inFlight}
              />
            </div>
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel autoFocus disabled={inFlight}>Cancel</AlertDialogCancel>
          <AlertDialogAction
            disabled={!matches || inFlight}
            className={cn("bg-destructive text-destructive-foreground hover:bg-destructive/90")}
            onClick={(event) => { event.preventDefault(); onConfirm(); }}
          >
            {inFlight ? "Working…" : confirmLabel}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
