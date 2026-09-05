import { useState } from "react";
import { Braces, Binary, FileText, Copy, Check } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { base64FromUtf8, bytesFromBase64, jsonPreviewFromBase64, looseUtf8FromBytes, toHex, utf8FromBase64 } from "@/lib/codec";
import { cn } from "@/lib/utils";

export type BinaryField = { encoding: string; data: string; size?: number; content_type?: string } | null | undefined;

/**
 * Canonical Base64 is always shown; UTF-8 and JSON are opt-in interpretations.
 * Invalid UTF-8 falls back to hex. Canonical content is what copy exports.
 */
export function BinaryValue({ value, compact = false, className }: { value: BinaryField; compact?: boolean; className?: string }) {
  const [copied, setCopied] = useState(false);
  if (!value || typeof value.data !== "string") return <span className="text-muted-foreground">—</span>;
  const bytes = (() => { try { return bytesFromBase64(value.data); } catch { return null; } })();
  const utf8 = utf8FromBase64(value.data);
  const json = jsonPreviewFromBase64(value.data);
  const size = value.size ?? bytes?.byteLength ?? 0;
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(value.data);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1200);
    } catch { /* clipboard unavailable */ }
  };
  return (
    <div className={cn("min-w-0", className)}>
      <Tabs defaultValue="utf8">
        <div className="flex flex-wrap items-center gap-2">
          <TabsList className="h-7 border-b-0">
            <TabsTrigger value="utf8" className="h-7 px-2 text-xs after:hidden"><FileText className="mr-1 size-3" />UTF-8</TabsTrigger>
            {json !== null && <TabsTrigger value="json" className="h-7 px-2 text-xs after:hidden"><Braces className="mr-1 size-3" />JSON</TabsTrigger>}
            <TabsTrigger value="b64" className="h-7 px-2 text-xs after:hidden"><Binary className="mr-1 size-3" />Base64</TabsTrigger>
            {(utf8 === null || !compact) && <TabsTrigger value="hex" className="h-7 px-2 text-xs after:hidden">Hex</TabsTrigger>}
          </TabsList>
          <Badge variant="neutral" className="font-mono text-xs">{size} B</Badge>
          <Button variant="ghost" size="icon" className="ml-auto size-7" onClick={() => void copy()} aria-label="Copy canonical Base64" title="Copy canonical Base64">
            {copied ? <Check className="size-3.5 text-success" aria-label="Copied" /> : <Copy className="size-3.5" />}
          </Button>
        </div>
        <TabsContent value="utf8" className="mt-2">
          {utf8 !== null
            ? <pre className={cn("rounded-none bg-muted p-2 font-mono text-xs whitespace-pre-wrap break-all", compact && "truncate")}>{utf8}</pre>
            : <p className="text-xs text-muted-foreground">Not valid UTF-8 — see Base64 or Hex.</p>}
        </TabsContent>
        {json !== null && (
          <TabsContent value="json" className="mt-2">
            <pre className="max-h-48 overflow-auto rounded-none bg-muted p-2 font-mono text-xs whitespace-pre-wrap break-all">{JSON.stringify(json, null, 2)}</pre>
          </TabsContent>
        )}
        <TabsContent value="b64" className="mt-2">
          <pre className={cn("rounded-none bg-muted p-2 font-mono text-xs whitespace-pre-wrap break-all", compact && "truncate")}>{value.data}</pre>
        </TabsContent>
        {bytes !== null && (utf8 === null || !compact) && (
          <TabsContent value="hex" className="mt-2">
            <pre className="max-h-32 overflow-auto rounded-none bg-muted p-2 font-mono text-xs whitespace-pre-wrap break-all">{toHex(bytes)}</pre>
          </TabsContent>
        )}
      </Tabs>
    </div>
  );
}

/** Composer input that produces canonical Base64 from Text, JSON, or raw Base64. */
export type EncodedDraft = { base64: string; bytes: number; error: string | null };

export function encodeDraft(mode: "text" | "json" | "base64", raw: string): EncodedDraft {
  if (raw.length === 0) return { base64: "", bytes: 0, error: null };
  try {
    if (mode === "base64") {
      const bytes = bytesFromBase64(raw.trim());
      return { base64: raw.trim(), bytes: bytes.byteLength, error: null };
    }
    if (mode === "json") {
      const parsed = JSON.parse(raw) as unknown;
      const text = JSON.stringify(parsed);
      return { base64: base64FromUtf8(text), bytes: new TextEncoder().encode(text).byteLength, error: null };
    }
    const bytes = new TextEncoder().encode(raw);
    return { base64: base64FromUtf8(raw), bytes: bytes.byteLength, error: null };
  } catch (reason) {
    return { base64: "", bytes: 0, error: reason instanceof Error ? reason.message : String(reason) };
  }
}

export function decodedPreview(value: BinaryField, maxLength = 120): string {
  if (!value) return "—";
  const utf8 = utf8FromBase64(value.data);
  if (utf8 !== null) return utf8.length > maxLength ? `${utf8.slice(0, maxLength)}…` : utf8;
  const bytes = (() => { try { return bytesFromBase64(value.data); } catch { return new Uint8Array(); } })();
  return `${bytes.byteLength} bytes · ${looseUtf8FromBytes(bytes.slice(0, 24)).replace(/[^\x20-\x7e]/g, "·")}`;
}
