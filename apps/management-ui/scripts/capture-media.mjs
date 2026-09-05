/**
 * Curated console media for the landing page and README.
 * Captures stills plus frame sequences for two GIFs (navigation and theme),
 * then encodes the GIFs with ffmpeg (palettegen/paletteuse).
 *
 * Usage: node apps/management-ui/scripts/capture-media.mjs [--base http://localhost:5173]
 * Output: landing/media/
 */
import { chromium } from "playwright-core";
import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync, readdirSync } from "node:fs";
import { resolve } from "node:path";

const args = process.argv.slice(2);
const base = args.includes("--base") ? args[args.indexOf("--base") + 1] : "http://localhost:5173";
const endpoint = "http://127.0.0.1:7380";
const token = process.env.KUTTIDB_QA_TOKEN ?? "qa-local-token-9f2c7b1e4d6a8f3b5c7d9e1a2b4c6d8e";
const repo = resolve(import.meta.dirname, "../../..");
const outDir = resolve(repo, "landing/media");
const workDir = resolve(repo, "landing/media/.frames");
mkdirSync(outDir, { recursive: true });
mkdirSync(workDir, { recursive: true });

const stills = [];
async function still(name) {
  const path = resolve(outDir, `${name}.png`);
  await page.screenshot({ path });
  stills.push(path);
  console.log("still", name);
}

// GIF frame capture: one frame every `intervalMs` while the step function runs.
async function recordGif(name, seconds, intervalMs, step) {
  const dir = resolve(workDir, name);
  rmSync(dir, { recursive: true, force: true });
  mkdirSync(dir, { recursive: true });
  let index = 0;
  const stopped = { value: false };
  await page.setViewportSize({ width: 880, height: 550 });
  const capture = async () => {
    while (!stopped.value) {
      const frame = `${dir}/f-${String(index).padStart(4, "0")}.png`;
      await page.screenshot({ path: frame });
      index += 1;
      await page.waitForTimeout(intervalMs);
    }
  };
  const capturer = capture();
  await step();
  await page.waitForTimeout(seconds * 1000);
  stopped.value = true;
  await capturer;
  await page.setViewportSize({ width: 1440, height: 900 });
  await encodeGif(name, dir);
  console.log("gif", name, `${index} frames`);
}

function encodeGif(name, dir) {
  const frames = readdirSync(dir).filter((f) => f.endsWith(".png")).sort();
  if (frames.length === 0) throw new Error(`no frames for ${name}`);
  const fps = 5;
  const palette = resolve(dir, "palette.png");
  const raw = resolve(dir, "raw.gif");
  execFileSync("ffmpeg", ["-y", "-framerate", String(fps), "-pattern_type", "glob", "-i", resolve(dir, "f-*.png"), "-vf", "palettegen=max_colors=64", "-frames:v", "1", "-update", "1", palette], { stdio: ["ignore", "ignore", "pipe"] });
  execFileSync("ffmpeg", ["-y", "-framerate", String(fps), "-pattern_type", "glob", "-i", resolve(dir, "f-*.png"), "-i", palette, "-lavfi", "paletteuse=dither=bayer:bayer_scale=5", "-loop", "0", raw], { stdio: ["ignore", "ignore", "pipe"] });
  // Downscale to 1080 px wide to keep the asset small; even width required.
  execFileSync("ffmpeg", ["-y", "-i", raw, "-vf", "scale=760:-2", "-loop", "0", resolve(outDir, `${name}.gif`)], { stdio: ["ignore", "ignore", "pipe"] });
  rmSync(dir, { recursive: true, force: true });
}

const browser = await chromium.launch({
  headless: true,
  executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
});
const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 });
const page = await context.newPage();

// Connect the fixture profile (token stays masked; never rendered in media).
await page.goto(base, { waitUntil: "networkidle" });
await page.getByLabel("Connection name").fill("Local development");
await page.getByLabel("Management endpoint").fill(endpoint);
await page.getByLabel("Administrator token").fill(token);
await page.getByRole("button", { name: /connect/i }).click();
await page.waitForTimeout(1800);

const profileRoute = await page.evaluate(() => {
  const segments = window.location.hash.replace(/^#\/?/, "").split("/").filter(Boolean);
  return `#/c/${segments[1]}`;
});
const gotoView = async (path) => {
  await page.evaluate((h) => { window.location.hash = h; }, `${profileRoute}/${path}`);
  await page.waitForTimeout(900);
};

// Stills (light).
await still("console-overview");
await gotoView("queues");
await still("console-queues");
await page.getByRole("link", { name: "reports" }).first().click();
await page.waitForTimeout(900);
await page.getByRole("tab", { name: "Messages" }).click();
await page.waitForTimeout(800);
await still("console-queue-messages");
await gotoView("keyspace");
await page.waitForTimeout(700);
await page.getByLabel("Prefix search (server-side)").fill("report:");
await page.getByRole("button", { name: "Apply" }).click();
await page.waitForTimeout(800);
await page.getByRole("cell", { name: "report:42" }).first().click();
await page.waitForTimeout(700);
await still("console-keyspace-inspector");
await page.keyboard.press("Escape");
await page.getByLabel("Prefix search (server-side)").fill("");
await page.getByRole("button", { name: "Apply" }).click();
await page.waitForTimeout(500);
await gotoView("operations/maintenance");
await page.waitForTimeout(700);
await still("console-maintenance");

// GIF 1: navigation — overview to queues to queue messages.
await gotoView("overview");
await page.waitForTimeout(600);
await recordGif("console-tour", 0.4, 130, async () => {
  await page.evaluate((h) => { window.location.hash = h; }, `${profileRoute}/queues`);
  await page.waitForTimeout(1400);
  await page.getByRole("link", { name: "reports" }).first().click();
  await page.waitForTimeout(1200);
  await page.getByRole("tab", { name: "Messages" }).click();
});

// Dark still.
await still("console-overview-dark");
await page.evaluate(() => localStorage.setItem("kuttidb.console.theme.v1", "light"));
await page.reload({ waitUntil: "networkidle" });

await browser.close();
rmSync(workDir, { recursive: true, force: true });
console.log("media captured:", stills.length, "stills + 2 gifs");
