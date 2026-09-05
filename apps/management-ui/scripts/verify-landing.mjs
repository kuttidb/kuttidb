/** Verify the landing page console section: assets load, layout holds. */
import { chromium } from "playwright-core";
import { mkdirSync } from "node:fs";
import { resolve } from "node:path";

const outDir = resolve(import.meta.dirname, "../../../docs/plans/assets/management-ui-redesign");
mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch({ headless: true, executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" });
const page = await (await browser.newContext({ viewport: { width: 1440, height: 900 } })).newPage();
const failures = [];
page.on("response", (response) => { if (response.status() >= 400) failures.push(`${response.status()} ${response.url()}`); });

await page.goto("http://localhost:4173/", { waitUntil: "networkidle" });
await page.locator("#console").scrollIntoViewIfNeeded();
await page.waitForTimeout(600);
const check = await page.evaluate(() => {
  const section = document.querySelector("#console");
  if (!section) return { ok: false, reason: "section missing" };
  const images = Array.from(section.querySelectorAll("img"));
  const broken = images.filter((img) => !img.complete || img.naturalWidth === 0).map((img) => img.getAttribute("src"));
  return {
    ok: broken.length === 0,
    broken,
    images: images.length,
    gif: images.some((img) => img.src.endsWith(".gif") && img.complete && img.naturalWidth > 0),
    eyebrow: section.querySelector(".eyebrow")?.textContent ?? "",
    docLinks: Array.from(section.querySelectorAll("a")).map((a) => a.href).filter((href) => href.includes("github.com")).length
  };
});
console.log(JSON.stringify(check, null, 1));
await page.screenshot({ path: resolve(outDir, "landing-console-section.png") });
const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1);
console.log("desktop overflow:", overflow);

await page.setViewportSize({ width: 390, height: 844 });
await page.waitForTimeout(500);
const overflowMobile = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1);
await page.locator("#console").scrollIntoViewIfNeeded();
await page.waitForTimeout(400);
await page.screenshot({ path: resolve(outDir, "landing-console-mobile.png") });
console.log("mobile overflow:", overflowMobile);

console.log("http failures:", failures.length ? failures : "none");
await browser.close();
if (!check.ok || overflow || overflowMobile || failures.length) process.exitCode = 1;
