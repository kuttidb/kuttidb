import { chromium } from "playwright-core";

const base = "http://localhost:5173";
const endpoint = "http://127.0.0.1:7380";
const token = process.env.KUTTIDB_QA_TOKEN ?? "qa-local-token-9f2c7b1e4d6a8f3b5c7d9e1a2b4c6d8e";
const shotDir = process.argv[2] ?? "/tmp/before-shots";

import { mkdirSync } from "node:fs";
mkdirSync(shotDir, { recursive: true });

const browser = await chromium.launch({ headless: true, executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" });
const page = await (await browser.newContext({ viewport: { width: 1440, height: 900 } })).newPage();
await page.goto(base, { waitUntil: "networkidle" });
await page.screenshot({ path: `${shotDir}/before-connections.png` });

await page.getByLabel("Management API origin").fill(endpoint);
await page.getByLabel("Administrator token").fill(token);
await page.getByRole("button", { name: /connect/i }).click();
await page.waitForTimeout(1500);
await page.screenshot({ path: `${shotDir}/before-overview.png` });

const route = await page.evaluate(() => {
  const s = window.location.hash.replace(/^#\/?/, "").split("/").filter(Boolean);
  return `#/c/${s[1]}`;
});
await page.evaluate((h) => { window.location.hash = h; }, `${route}/queues`);
await page.waitForTimeout(900);
await page.screenshot({ path: `${shotDir}/before-queues.png` });

await page.getByRole("cell", { name: "reports" }).first().click();
await page.waitForTimeout(900);
await page.screenshot({ path: `${shotDir}/before-queue-detail.png` });

console.log("before shots done:", shotDir);
await browser.close();
