#!/usr/bin/env node
import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { buildApp, productionStaticRoot } from "./app.js";
import { readConfig } from "./config.js";

export type CliArgs = {
  host?: string;
  port?: string;
  open: boolean;
  help: boolean;
  version: boolean;
  allowLoopbackHttp: boolean;
  allowPrivateTargets: boolean;
  targetAllowlist?: string;
};

const helpText = `
KuttiDB Management Console

Runs the self-hosted web console for KuttiDB's Management API. This does not
start KuttiDB itself -- start KuttiDB separately with --admin-bind and
--admin-token-file, then open the console and connect using that token.

Usage:
  npx @kuttidb/management-ui [options]

Options:
  -p, --port <port>          Port to listen on (default: 8080)
  -H, --host <host>          Host to bind to (default: 127.0.0.1)
      --no-open              Do not open a browser automatically
      --allow-private-targets  Allow connecting to private (RFC1918) targets, not just loopback
      --strict-https         Require HTTPS even for loopback targets
      --target-allowlist <hosts>  Comma-separated hostnames this console may connect to
  -v, --version              Print the version and exit
  -h, --help                 Show this help and exit
`;

function setIfDefined(target: CliArgs, key: "host" | "port" | "targetAllowlist", value: string | undefined): void {
  if (value !== undefined) target[key] = value;
}

export function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = { open: true, help: false, version: false, allowLoopbackHttp: true, allowPrivateTargets: false };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--host" || arg === "-H") setIfDefined(args, "host", argv[++index]);
    else if (arg?.startsWith("--host=")) args.host = arg.slice("--host=".length);
    else if (arg === "--port" || arg === "-p") setIfDefined(args, "port", argv[++index]);
    else if (arg?.startsWith("--port=")) args.port = arg.slice("--port=".length);
    else if (arg === "--no-open") args.open = false;
    else if (arg === "--allow-private-targets") args.allowPrivateTargets = true;
    else if (arg === "--strict-https") args.allowLoopbackHttp = false;
    else if (arg === "--target-allowlist") setIfDefined(args, "targetAllowlist", argv[++index]);
    else if (arg?.startsWith("--target-allowlist=")) args.targetAllowlist = arg.slice("--target-allowlist=".length);
    else if (arg === "--help" || arg === "-h") args.help = true;
    else if (arg === "--version" || arg === "-v") args.version = true;
  }
  return args;
}

export function readVersion(): string {
  try {
    const packageJsonPath = resolve(dirname(fileURLToPath(import.meta.url)), "../../package.json");
    const pkg = JSON.parse(readFileSync(packageJsonPath, "utf8")) as { version?: string };
    return pkg.version ?? "unknown";
  } catch {
    return "unknown";
  }
}

export function openBrowser(url: string): void {
  const platform = process.platform;
  const command = platform === "darwin" ? "open" : platform === "win32" ? "cmd" : "xdg-open";
  const commandArgs = platform === "win32" ? ["/c", "start", "", url] : [url];
  try {
    spawn(command, commandArgs, { stdio: "ignore", detached: true }).unref();
  } catch {
    // Best effort only; the console URL is already printed for the user to open by hand.
  }
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(helpText);
    return;
  }
  if (args.version) {
    console.log(readVersion());
    return;
  }

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    HOST: args.host ?? process.env.HOST ?? "127.0.0.1",
    PORT: args.port ?? process.env.PORT ?? "8080",
    NODE_ENV: process.env.NODE_ENV === "production" ? "production" : "development",
    ALLOW_LOOPBACK_HTTP: String(args.allowLoopbackHttp || process.env.ALLOW_LOOPBACK_HTTP === "true"),
    ...(args.allowPrivateTargets ? { ALLOW_PRIVATE_TARGETS: "true" } : {}),
    ...(args.targetAllowlist ? { TARGET_ALLOWLIST: args.targetAllowlist } : {})
  };

  const config = readConfig(env);
  const app = await buildApp(config, productionStaticRoot());

  const shutdown = async () => {
    await app.close();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  await app.listen({ host: config.host, port: config.port });

  const displayHost = config.host === "0.0.0.0" ? "localhost" : config.host;
  const url = `http://${displayHost}:${config.port}`;
  console.log(`\n  KuttiDB Management Console running at ${url}\n`);
  if (args.open) openBrowser(url);
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
