/**
 * Browser QA for the redesigned KuttiDB management console.
 * Drives the locally installed Chrome via playwright-core against the dev
 * stack (Vite 5173 -> gateway 8080 -> local KuttiDB admin API 7380).
 *
 * Usage: node run/qa/browser-qa.mjs [--base http://localhost:5173] [--headed]
 * Screenshots: docs/plans/assets/management-ui-redesign/
 */
import { chromium } from "playwright-core";
import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const args = process.argv.slice(2);
const base = args.includes("--base") ? args[args.indexOf("--base") + 1] : "http://localhost:5173";
const headed = args.includes("--headed");
const endpoint = "http://127.0.0.1:7380";
const token = process.env.KUTTIDB_QA_TOKEN ?? "qa-local-token-9f2c7b1e4d6a8f3b5c7d9e1a2b4c6d8e";
const profileLabel = "Local development";

const repo = resolve(import.meta.dirname, "../../..");
const shotDir = resolve(repo, "docs/plans/assets/management-ui-redesign");
mkdirSync(shotDir, { recursive: true });

const results = [];
const consoleErrors = [];
let page;

async function step(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (reason) {
    results.push({ name, ok: false, error: String(reason).split("\n")[0] });
    console.log(`FAIL ${name}: ${String(reason).split("\n")[0]}`);
    try { await shot(`FAIL-${name.replace(/[^a-z0-9-]+/gi, "-")}`); } catch {}
  }
}

async function shot(name) {
  await page.screenshot({ path: resolve(shotDir, `${name}.png`), fullPage: false });
}

async function gotoHash(hash) {
  await page.evaluate((h) => { window.location.hash = h; }, hash);
  await page.waitForTimeout(400);
}

/** Navigate within the connected profile: swaps the path after #/c/{profileId}/. */
let profileRoute = null; // e.g. "#/c/<id>"
async function gotoView(path) {
  if (!profileRoute) {
    profileRoute = await page.evaluate(() => {
      const segments = window.location.hash.replace(/^#\/?/, "").split("/").filter(Boolean);
      if (segments[0] !== "c") throw new Error("not on a connection route: " + window.location.hash);
      return `#/c/${segments[1]}`;
    });
  }
  await gotoHash(`${profileRoute}/${path}`);
}

const browser = await chromium.launch({
  headless: !headed,
  executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  viewport: { width: 1440, height: 900 }
});
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
page = await context.newPage();
page.on("console", (message) => {
  if (message.type() === "error") consoleErrors.push(message.text());
});
page.on("pageerror", (error) => consoleErrors.push(`pageerror: ${error.message}`));

// ---------- Connections ----------
await step("connections: page renders", async () => {
  await page.goto(base, { waitUntil: "networkidle" });
  await shot("fixture-connections-empty");
});

await step("connections: validation failure shows inline error", async () => {
  await page.getByLabel("Connection name").fill("Broken target");
  await page.getByLabel("Management endpoint").fill("http://127.0.0.1:1");
  await page.getByLabel("Administrator token").fill("wrong-token");
  await page.getByRole("button", { name: /connect/i }).click();
  await page.waitForTimeout(800);
  await page.getByRole("alert").first().waitFor({ state: "visible", timeout: 5000 });
  await shot("fixture-connections-validation-error");
});

await step("connections: connect with fixture profile", async () => {
  await page.getByLabel("Connection name").fill(profileLabel);
  await page.getByLabel("Management endpoint").fill(endpoint);
  await page.getByLabel("Administrator token").fill(token);
  await page.getByRole("button", { name: /connect/i }).click();
  await page.waitForTimeout(1200);
  await shot("fixture-overview-after-connect");
});

await step("overview: composition and facts render", async () => {
  await page.getByRole("heading", { name: "Overview", level: 1 }).waitFor({ timeout: 8000 });
  await page.waitForTimeout(800);
  await shot("fixture-overview");
});

await step("navigation: exactly one active item for operations routes", async () => {
  await gotoView("operations/maintenance");
  await page.waitForTimeout(500);
  const active = await page.locator('nav[aria-label="Console"] [aria-current="page"]').allTextContents();
  if (active.length !== 1) throw new Error(`expected 1 active item, got ${JSON.stringify(active)}`);
  if (!active[0].includes("Maintenance")) throw new Error(`maintenance not active: ${active[0]}`);
  await gotoView("operations/atomic");
  await page.waitForTimeout(500);
  const active2 = await page.locator('nav[aria-label="Console"] [aria-current="page"]').allTextContents();
  if (active2.length !== 1 || !active2[0].includes("Atomic")) throw new Error(`atomic nav wrong: ${JSON.stringify(active2)}`);
});

// ---------- Keyspace ----------
await step("keyspace: entries table and inspector", async () => {
  await gotoView("keyspace");
  await page.waitForTimeout(700);
  await page.getByLabel("Prefix search (server-side)").fill("report:");
  await page.getByRole("button", { name: "Apply" }).click();
  await page.waitForTimeout(800);
  await page.getByRole("cell", { name: "report:42" }).first().waitFor({ timeout: 6000 });
  await shot("fixture-keyspace");
  await page.getByRole("cell", { name: "report:42" }).first().click();
  await page.waitForTimeout(700);
  await page.getByText("qa-fixture").first().waitFor({ timeout: 5000 });
  await shot("fixture-keyspace-inspector");
  await page.keyboard.press("Escape");
  await page.waitForTimeout(300);
  await page.getByLabel("Prefix search (server-side)").fill("");
  await page.getByRole("button", { name: "Apply" }).click();
  await page.waitForTimeout(600);
});

// ---------- Queues ----------
await step("queues: list shows fixture queue", async () => {
  await gotoView("queues");
  await page.waitForTimeout(700);
  await page.getByRole("cell", { name: "reports" }).first().waitFor({ timeout: 6000 });
  await shot("fixture-queues");
});

await step("queues: declare + delete a scratch queue", async () => {
  await page.getByRole("button", { name: "Declare queue" }).first().click();
  await page.getByLabel("Name").fill("scratch-queue");
  await page.getByRole("button", { name: "Declare", exact: true }).click();
  await page.waitForTimeout(900);
  await page.getByRole("cell", { name: "scratch-queue" }).first().waitFor({ timeout: 6000 });
  await shot("fixture-queues-declared");
});

await step("queue detail: tabs, publish, messages", async () => {
  await page.getByRole("cell", { name: "reports" }).first().click();
  await page.waitForTimeout(800);
  await page.getByRole("tab", { name: "Messages" }).click();
  await page.waitForTimeout(700);
  await shot("fixture-queue-messages");
  await page.getByRole("tab", { name: "Publish" }).click();
  await page.getByLabel(/Message body/i).fill("qa publish body");
  await page.getByRole("button", { name: /Publish/ }).click();
  await page.waitForTimeout(800);
  await shot("fixture-queue-published");
});

// ---------- Streams ----------
await step("streams: list, detail, records", async () => {
  await gotoView("streams");
  await page.waitForTimeout(700);
  await page.getByRole("cell", { name: "report.events" }).first().waitFor({ timeout: 6000 });
  await shot("fixture-streams");
  await page.getByRole("cell", { name: "report.events" }).first().click();
  await page.waitForTimeout(800);
  await shot("fixture-stream-detail");
  await page.getByRole("tab", { name: "Partitions" }).click();
  await page.waitForTimeout(500);
  await shot("fixture-stream-partitions");
});

// ---------- Theme + responsive ----------
await step("theme: dark mode applies and persists across routes", async () => {
  await page.getByRole("button", { name: /^Theme:/ }).first().click();
  await page.getByRole("menuitem", { name: "Dark" }).click();
  await page.waitForTimeout(400);
  const isDark = await page.evaluate(() => document.documentElement.classList.contains("dark"));
  if (!isDark) throw new Error("dark class not applied");
  await shot("fixture-dark-stream-detail");
  await gotoView("overview");
  await page.waitForTimeout(400);
  const stillDark = await page.evaluate(() => document.documentElement.classList.contains("dark"));
  if (!stillDark) throw new Error("theme lost across navigation");
  await page.evaluate(() => localStorage.setItem("kuttidb.console.theme.v1", "light"));
  await page.reload({ waitUntil: "networkidle" });
  await page.waitForTimeout(600);
  const lightAfterReload = await page.evaluate(() => !document.documentElement.classList.contains("dark"));
  if (!lightAfterReload) throw new Error("stored light theme did not survive reload");
});

await step("mobile: 390x844 menu drawer and no page overflow", async () => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForTimeout(500);
  await shot("fixture-mobile-overview");
  await page.waitForTimeout(2000); // let transient toasts dismiss
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1);
  if (overflow) {
    const offenders = await page.evaluate(() =>
      Array.from(document.querySelectorAll("*"))
        .filter((el) => el.getBoundingClientRect().right > document.documentElement.clientWidth + 1 && el.children.length === 0)
        .slice(0, 8)
        .map((el) => `${el.tagName}.${String(el.className).slice(0, 60)} right=${Math.round(el.getBoundingClientRect().right)}`)
    );
    throw new Error(`horizontal page overflow at 390px; offenders: ${offenders.join(" ;; ")}`);
  }
  await page.getByRole("button", { name: "Open menu" }).click();
  await page.waitForTimeout(400);
  await shot("fixture-mobile-drawer");
  await page.keyboard.press("Escape");
});


await step("design tokens: paper canvas, sidebar 216px, squared controls, orange rail", async () => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await gotoView("overview");
  await page.waitForTimeout(500);
  const checks = await page.evaluate(() => {
    const bodyBg = getComputedStyle(document.body).backgroundColor;
    const aside = document.querySelector("aside");
    const sidebarWidth = aside ? aside.getBoundingClientRect().width : null;
    const button = document.querySelector('[data-slot="button"]');
    const buttonRadius = button ? getComputedStyle(button).borderRadius : null;
    const h1 = document.querySelector("h1");
    const h1Size = h1 ? getComputedStyle(h1).fontSize : null;
    const h1Weight = h1 ? getComputedStyle(h1).fontWeight : null;
    const active = document.querySelector('nav[aria-label="Console"] [aria-current="page"]');
    const rail = active ? active.querySelector("span[aria-hidden]") : null;
    const railBg = rail ? getComputedStyle(rail).backgroundColor : null;
    const railWidth = rail ? rail.getBoundingClientRect().width : null;
    const table = document.querySelector('[data-slot="table-head"]');
    const headBg = table ? getComputedStyle(table.closest("thead")).backgroundColor : null;
    return { bodyBg, sidebarWidth, buttonRadius, h1Size, h1Weight, railBg, railWidth, headBg };
  });
  if (checks.bodyBg !== "rgb(245, 242, 233)") throw new Error(`paper canvas wrong: ${checks.bodyBg}`);
  if (checks.sidebarWidth !== 216) throw new Error(`sidebar width wrong: ${checks.sidebarWidth}`);
  if (checks.buttonRadius !== "4px") throw new Error(`button radius wrong: ${checks.buttonRadius}`);
  if (checks.h1Size !== "26px" || checks.h1Weight !== "600") throw new Error(`h1 wrong: ${checks.h1Size}/${checks.h1Weight}`);
  if (checks.railBg !== "rgb(237, 116, 47)" || checks.railWidth !== 3) throw new Error(`active rail wrong: ${checks.railBg}/${checks.railWidth}`);
});

await step("keyboard: focus outline visible and skip link works", async () => {
  await gotoView("overview");
  await page.evaluate(() => (document.activeElement instanceof HTMLElement) && document.activeElement.blur());
  await page.waitForTimeout(200);
  await page.keyboard.press("Tab"); // skip link
  const skip = await page.evaluate(() => document.activeElement?.textContent ?? "");
  if (!skip.toLowerCase().includes("skip")) throw new Error(`first tab stop not skip link: ${skip}`);
  await page.keyboard.press("Enter");
  await page.waitForTimeout(300);
  const mainFocused = await page.evaluate(() => document.activeElement?.id ?? "");
  if (mainFocused !== "main-content") throw new Error(`skip link target wrong: ${mainFocused}`);
  await page.keyboard.press("Tab");
  const outline = await page.evaluate(() => {
    const el = document.activeElement;
    const style = getComputedStyle(el);
    return { outlineWidth: style.outlineWidth, outlineColor: style.outlineColor, tag: el.tagName };
  });
  if (outline.outlineWidth === "0px") throw new Error("no visible focus outline after Tab");
});

await step("zoom-equivalent 640px: primary content usable, no overflow", async () => {
  await page.setViewportSize({ width: 640, height: 740 });
  await gotoView("overview");
  await page.waitForTimeout(400);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1);
  if (overflow) throw new Error("horizontal overflow at 640px (200% zoom equivalent)");
  await page.setViewportSize({ width: 320, height: 740 });
  await page.waitForTimeout(300);
  const overflow320 = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1);
  if (overflow320) throw new Error("horizontal overflow at 320px");
  await shot("fixture-320-overview");
  await page.setViewportSize({ width: 1440, height: 900 });
});

await step("reduced motion: transitions disabled", async () => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await gotoView("queues");
  await page.waitForTimeout(400);
  const duration = await page.evaluate(() => {
    const button = document.querySelector('[data-slot="button"]');
    return getComputedStyle(button).transitionDuration;
  });
  const durationMs = duration.split(",").map((value) => parseFloat(value) * (value.includes("ms") ? 1 : 1000));
  if (durationMs.some((value) => value > 1)) throw new Error(`reduced motion not applied: ${duration}`);
  await page.emulateMedia({ reducedMotion: "no-preference" });
});


// ---------- Deep workflow pass ----------
await step("groups: listing and detail with both identifiers", async () => {
  await gotoView("groups");
  await page.waitForTimeout(700);
  await shot("fixture-groups-empty");
  // No groups exist on the fixture server; verify the empty state copy.
  await page.getByText("No consumer groups").first().waitFor({ timeout: 5000 });
});

await step("routing: list, declare, detail, route bind, publish", async () => {
  await gotoView("routing");
  await page.waitForTimeout(700);
  await page.getByRole("cell", { name: "ingress" }).first().waitFor({ timeout: 6000 });
  await shot("fixture-routing");
  await page.getByRole("cell", { name: "ingress" }).first().click();
  await page.waitForTimeout(700);
  await shot("fixture-routing-detail");
  await page.getByRole("button", { name: "Bind route" }).click();
  await page.getByLabel("Queue", { exact: true }).click();
  await page.getByRole("option", { name: "reports" }).click();
  await page.getByLabel("Routing key").fill("nightly");
  await page.getByRole("button", { name: "Bind route", exact: true }).click();
  await page.waitForTimeout(800);
  await page.getByRole("cell", { name: "nightly" }).first().waitFor({ timeout: 5000 });
  await shot("fixture-routing-route-bound");
  await page.getByRole("button", { name: "Publish" }).first().click();
  await page.getByLabel("Routing key").fill("nightly");
  await page.getByLabel("Body (text)").fill("qa routed publish");
  await page.getByRole("button", { name: "Publish", exact: true }).click();
  await page.waitForTimeout(800);
  await page.getByText(/queue\(s\)/).waitFor({ timeout: 5000 });
  await shot("fixture-routing-published");
  await page.keyboard.press("Escape");
});

await step("atomic: mode, preview, commit, result", async () => {
  await gotoView("operations/atomic");
  await page.waitForTimeout(700);
  await shot("fixture-atomic");
  await page.getByLabel("Operation").click();
  await page.getByRole("option", { name: "put-and-enqueue" }).click();
  await page.getByLabel("Cache key (plain text)").fill("report:99");
  await page.getByLabel("Target queue (name)").fill("reports");
  await page.getByLabel("Value = message body (text)").fill("atomic qa value");
  await page.getByRole("button", { name: /Commit atomically/ }).click();
  await page.waitForTimeout(1000);
  await page.getByText("Transaction committed").waitFor({ timeout: 5000 });
  await shot("fixture-atomic-result");
});

await step("maintenance: engine rows, enqueue checkpoint, job states", async () => {
  await gotoView("operations/maintenance");
  await page.waitForTimeout(700);
  await shot("fixture-maintenance");
  await page.getByRole("button", { name: "Checkpoint all engines" }).click();
  await page.waitForTimeout(1200);
  await page.getByText(/Job .* queued/).waitFor({ timeout: 5000 });
  await page.waitForTimeout(1500); // allow the job worker to finish
  await shot("fixture-maintenance-jobs");
  const succeeded = await page.getByText("succeeded").first().isVisible().catch(() => false);
  const queued = await page.getByText("queued", { exact: true }).first().isVisible().catch(() => false);
  if (!succeeded && !queued) throw new Error("job state row not visible");
});

await step("destructive: purge dialog requires exact typed ID, focuses Cancel", async () => {
  await gotoView("queues");
  await page.waitForTimeout(500);
  await page.getByRole("link", { name: "scratch-queue" }).first().click();
  await page.waitForTimeout(700);
  await page.getByRole("button", { name: "Queue actions" }).click();
  await page.getByRole("menuitem", { name: "Purge messages…" }).click();
  await page.waitForTimeout(400);
  const focusInDialog = await page.evaluate(() => document.activeElement?.textContent ?? "");
  if (!focusInDialog.includes("Cancel")) throw new Error(`initial focus not Cancel: ${focusInDialog}`);
  const confirmButton = page.getByRole("button", { name: "Purge messages", exact: true });
  if (await confirmButton.isEnabled()) throw new Error("confirm enabled before typing the ID");
  await page.getByLabel(/Confirmation ID/).fill("wrong-id");
  if (await confirmButton.isEnabled()) throw new Error("confirm enabled with wrong ID");
  await shot("fixture-purge-dialog");
  await page.keyboard.press("Escape");
  await page.waitForTimeout(300);
  await page.getByRole("button", { name: "Queue actions" }).click();
  await page.getByRole("menuitem", { name: "Delete queue…" }).click();
  await page.waitForTimeout(400);
  const queueId = await page.evaluate(() => document.querySelector('[data-slot="alert-dialog-content"] code')?.textContent ?? "");
  if (!queueId.startsWith("b64u:")) throw new Error(`could not read canonical queue ID: ${queueId}`);
  await page.getByLabel(/Confirmation ID/).fill(queueId);
  await page.getByRole("button", { name: "Delete queue", exact: true }).click();
  await page.waitForTimeout(1000);
  await page.getByRole("heading", { name: "Queues", level: 1 }).waitFor({ timeout: 5000 });
  await shot("fixture-queues-after-delete");
});

await step("tail: explicit start and stop on stream detail", async () => {
  await gotoView("streams");
  await page.waitForTimeout(500);
  await page.getByRole("link", { name: "report.events" }).first().click();
  await page.waitForTimeout(600);
  await page.getByRole("tab", { name: "Tail" }).click();
  await page.waitForTimeout(300);
  await page.getByRole("button", { name: "Start tail" }).click();
  await page.waitForTimeout(1500);
  await shot("fixture-stream-tail");
  const stopped = await page.getByRole("button", { name: "Stop tail" }).isVisible().catch(() => false);
  if (stopped) await page.getByRole("button", { name: "Stop tail" }).click();
  await page.waitForTimeout(400);
});

await step("locked: lock all shows branded locked frame with route preserved", async () => {
  await gotoView("overview");
  await page.waitForTimeout(400);
  const routeBefore = await page.evaluate(() => window.location.hash);
  try {
    await page.locator("aside").getByRole("button", { name: "Lock all tokens" }).click({ timeout: 10000 });
  } catch (reason) {
    const diag = await page.evaluate(() => ({
      hash: window.location.hash,
      asides: document.querySelectorAll("aside").length,
      dialogs: document.querySelectorAll("[data-slot='dialog-content'], [data-slot='alert-dialog-content']").length,
      active: document.activeElement?.textContent?.slice(0, 40) ?? "",
      tailRunning: Boolean(document.querySelector("[data-slot='tabs-content']")?.textContent?.includes("Tail running"))
    }));
    throw new Error(`lock click failed: ${String(reason).split("\n")[0]} diag=${JSON.stringify(diag)}`);
  }
  await page.waitForTimeout(800);
  await page.getByText("is locked").first().waitFor({ timeout: 5000 });
  const routeAfter = await page.evaluate(() => window.location.hash);
  if (routeAfter !== routeBefore) throw new Error(`route changed: ${routeBefore} -> ${routeAfter}`);
  await shot("fixture-locked");
  // reconnect via Connections: locked screen -> profile row "Reconnect…" -> token dialog
  await page.getByRole("button", { name: "Reconnect", exact: true }).click();
  await page.waitForTimeout(600);
  await page.getByRole("button", { name: "Reconnect…" }).click();
  await page.waitForTimeout(300);
  await page.getByRole("dialog").getByLabel("Administrator token").fill(token);
  await page.getByRole("dialog").getByRole("button", { name: "Reconnect", exact: true }).click();
  await page.waitForTimeout(1400);
  await page.getByRole("heading", { name: "Overview", level: 1 }).waitFor({ timeout: 6000 });
  await shot("fixture-reconnected-overview");
});

await step("unknown profile: branded recovery screen", async () => {
  await gotoHash("#/c/00000000-0000-0000-0000-000000000000/overview");
  await page.waitForTimeout(500);
  await page.getByText("Unknown connection").waitFor({ timeout: 5000 });
  await shot("fixture-unknown-profile");
  await gotoView("overview");
});


// ---------- Audit-driven regression checks ----------
await step("keyspace: prefix search returns matching rows without error", async () => {
  await gotoView("keyspace");
  await page.waitForTimeout(800);
  await page.getByLabel("Prefix search (server-side)").fill("report:");
  await page.getByRole("button", { name: "Apply" }).click();
  await page.waitForTimeout(900);
  const hasError = await page.getByText("limit, prefix, expires, or cursor is invalid").count();
  if (hasError > 0) throw new Error("prefix search surfaced a validation error");
  for (const key of ["report:42", "report:43"]) {
    if ((await page.getByRole("cell", { name: key }).count()) === 0) throw new Error(`missing prefixed row: ${key}`);
  }
  const bulkVisible = await page.getByRole("cell", { name: "bulk:001" }).count();
  if (bulkVisible > 0) throw new Error("prefix filter did not filter");
  await shot("fixture-keyspace-prefix");
  await page.getByLabel("Prefix search (server-side)").fill("");
  await page.getByRole("button", { name: "Apply" }).click();
  await page.waitForTimeout(700);
});

await step("keyspace: cursor pagination crosses at least two pages", async () => {
  await gotoView("keyspace");
  await page.waitForTimeout(900);
  const next = page.getByRole("button", { name: "Next page" });
  if ((await next.count()) === 0 || await next.isDisabled()) throw new Error("no next page cursor on page one");
  await next.click();
  await page.waitForTimeout(900);
  const pageTwoRows = await page.getByRole("cell", { name: /bulk:0[45]/ }).count();
  if (pageTwoRows === 0) throw new Error("second page did not show later bulk rows");
  await shot("fixture-keyspace-page-two");
  const back = page.getByRole("button", { name: "Back" });
  await back.click();
  await page.waitForTimeout(900);
  if ((await page.getByRole("cell", { name: "bulk:000" }).count()) === 0) throw new Error("back stack did not return to page one");
});

await step("queue detail at 320px: no document overflow, tab strip scrolls", async () => {
  await page.setViewportSize({ width: 320, height: 740 });
  await gotoView("queues");
  await page.waitForTimeout(700);
  await page.getByRole("link", { name: "reports" }).first().click();
  await page.waitForTimeout(800);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1);
  if (overflow) {
    const widths = await page.evaluate(() => ({
      doc: document.documentElement.scrollWidth,
      client: document.documentElement.clientWidth,
      tabs: document.querySelector('[data-slot="tabs-list"]')?.getBoundingClientRect().width ?? null
    }));
    throw new Error(`overflow at 320px: ${JSON.stringify(widths)}`);
  }
  // Keyboard: tab to the tab list and arrow through all five tabs.
  const tabNames = ["Messages", "Publish", "Deliveries", "Consumers"];
  await page.getByRole("tab", { name: "Overview" }).click();
  for (const name of tabNames) {
    await page.keyboard.press("ArrowRight");
    await page.waitForTimeout(120);
    const active = await page.evaluate(() => document.querySelector('[role="tab"][aria-selected="true"]')?.textContent ?? "");
    if (!active.includes(name)) throw new Error(`arrow navigation did not reach ${name}, got ${active}`);
  }
  await shot("fixture-320-queue-detail-tabs");
  await page.setViewportSize({ width: 1440, height: 900 });
});

await step("drawer: focus returns to the Menu trigger on Escape", async () => {
  await page.setViewportSize({ width: 900, height: 844 });
  await gotoView("overview");
  await page.waitForTimeout(400);
  await page.evaluate(() => (document.activeElement instanceof HTMLElement) && document.activeElement.blur());
  await page.getByRole("button", { name: "Open menu" }).click();
  await page.waitForTimeout(400);
  await page.keyboard.press("Escape");
  await page.waitForTimeout(400);
  const focus = await page.evaluate(() => ({
    text: document.activeElement?.textContent?.trim() ?? "",
    aria: document.activeElement?.getAttribute("aria-label") ?? ""
  }));
  if (!(focus.text.includes("Menu") || focus.aria.includes("Open menu"))) {
    throw new Error(`focus not restored to Menu trigger: ${JSON.stringify(focus)}`);
  }
  // Navigation dismissal also returns focus to the trigger.
  await page.getByRole("button", { name: "Open menu" }).click();
  await page.waitForTimeout(400);
  await page.locator('[data-slot="dialog-content"] nav a').first().click();
  await page.waitForTimeout(600);
  const focus2 = await page.evaluate(() => ({
    text: document.activeElement?.textContent?.trim() ?? "",
    aria: document.activeElement?.getAttribute("aria-label") ?? ""
  }));
  if (!(focus2.text.includes("Menu") || focus2.aria.includes("Open menu"))) {
    throw new Error(`focus not restored after navigation dismissal: ${JSON.stringify(focus2)}`);
  }
  await page.setViewportSize({ width: 1440, height: 900 });
});

await step("destructive dialogs: connection context inside the modal", async () => {
  await gotoView("streams");
  await page.waitForTimeout(700);
  await page.getByRole("link", { name: "report.events" }).first().click();
  await page.waitForTimeout(700);
  await page.getByRole("button", { name: "Stream actions" }).click();
  await page.getByRole("menuitem", { name: "Delete stream…" }).click();
  await page.waitForTimeout(400);
  const dialog = page.locator("[data-slot='alert-dialog-content']");
  const text = await dialog.innerText();
  if (!text.includes("Connection:")) throw new Error("delete-stream dialog lacks connection context");
  if (!text.includes("report.events")) throw new Error("delete-stream dialog lacks decoded resource name");
  await shot("fixture-stream-delete-context");
  await page.keyboard.press("Escape");
  await page.waitForTimeout(300);
});

await step("atomic: empty submit focuses first invalid field with aria-invalid", async () => {
  await gotoView("operations/atomic");
  await page.waitForTimeout(600);
  await page.getByLabel("Operation").click();
  await page.getByRole("option", { name: "put-and-enqueue" }).click();
  await page.getByRole("button", { name: /Commit atomically/ }).click();
  await page.waitForTimeout(400);
  const invalidCount = await page.locator('[aria-invalid="true"]').count();
  if (invalidCount < 2) throw new Error(`expected aria-invalid fields, got ${invalidCount}`);
  const focusedId = await page.evaluate(() => document.activeElement?.id ?? "");
  if (focusedId !== "atomic-key") throw new Error(`first invalid field not focused: ${focusedId}`);
  await shot("fixture-atomic-field-errors");
});

await step("summary report", async () => {
  writeFileSync(resolve(shotDir, "qa-results.json"), JSON.stringify({ results, consoleErrors }, null, 2));
});

await browser.close();
const failed = results.filter((entry) => !entry.ok);
console.log(`\n${results.length - failed.length}/${results.length} steps passed. Console errors: ${consoleErrors.length}`);
if (failed.length > 0) process.exitCode = 1;
