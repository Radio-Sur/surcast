// Invoked by the isolated Rust lifecycle fixture, not global-setup.ts.
// Run: bun run build, then the ignored panel_stop_start_without_refresh test.
import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";
import { chromium, expect } from "@playwright/test";

const base = process.env.SURCAST_BROWSER_BASE;
const auth = process.env.SURCAST_BROWSER_AUTH;
const station = process.env.SURCAST_BROWSER_STATION;
const icecast = process.env.SURCAST_BROWSER_ICECAST;
assert(base && auth && station && icecast, "Run via the Rust lifecycle fixture");

const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  await page.addInitScript((token) => {
    localStorage.setItem("access_token", token);
    localStorage.setItem("refresh_token", "isolated-test");
    localStorage.setItem("i18nextLng", "en");
  }, auth.replace(/^Bearer /, ""));
  // Keep confirmation visibly pending even on a very fast local Icecast.
  await page.route("**/stream/status?verify_output=true", async (route) => {
    await setTimeout(800);
    await route.continue();
  });
  const commands = [];
  page.on("request", (request) => {
    if (request.method() === "POST" && request.url().includes("/stream/")) commands.push(request.url().split("/").at(-1));
  });
  await page.goto(new URL(`/stations/${station}`, base).href);
  await expect(page.getByRole("button", { name: "Start", exact: true })).toBeVisible();
  const documentStartedAt = await page.evaluate(() => performance.timeOrigin);
  let navigations = 0;
  // BrowserRouter initializes history with replaceState (a same-document
  // navigation). Count real document requests, not those history updates.
  page.on("request", (request) => {
    if (request.isNavigationRequest() && request.frame() === page.mainFrame()) navigations++;
  });

  for (let cycle = 0; cycle < 2; cycle++) {
    await page.getByRole("button", { name: "Start", exact: true }).click();
    await expect(page.getByRole("button", { name: /Starting/ })).toBeDisabled();
    await expect(page.getByRole("progressbar")).toBeVisible();
    await expect(page.getByText("Station started", { exact: true })).not.toBeVisible();
    await expect(page.getByText("Station started", { exact: true })).toBeVisible({ timeout: 35000 });
    await expect(page.getByRole("progressbar")).not.toBeVisible();
    const response = await fetch(icecast, { signal: AbortSignal.timeout(10000) });
    assert.equal(response.status, 200);
    assert.match(response.headers.get("content-type"), /audio\/mpeg/);
    const reader = response.body.getReader();
    const chunk = await reader.read();
    assert(chunk.value?.length > 0, "confirmed Start must serve actual audio");
    await reader.cancel();
    await page.getByRole("button", { name: "Close", exact: true }).click();

    await page.getByRole("button", { name: "Stop", exact: true }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByRole("button", { name: "Stop", exact: true }).click();
    await expect(dialog.getByRole("progressbar")).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Cancel" })).toBeDisabled();
    await expect(page.getByText("Station stopped", { exact: true })).not.toBeVisible();
    await page.keyboard.press("Escape");
    await expect(dialog).toBeVisible();
    await expect(page.getByText("Station stopped", { exact: true })).toBeVisible({ timeout: 35000 });
    await expect(dialog).not.toBeVisible();
    assert.equal((await fetch(icecast, { signal: AbortSignal.timeout(5000) })).status, 404);
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await setTimeout(11000); // Exceed the source timeout before the next Start.
  }
  assert.deepEqual(commands, ["play", "stop", "play", "stop"]);
  assert.equal(navigations, 0, "Stop → Start must not require a page reload");
  assert.equal(await page.evaluate(() => performance.timeOrigin), documentStartedAt, "the original document must survive both cycles");
  console.log("Browser: two Start → Stop cycles, >10s stopped, real audio, delayed notifications, no reload: OK");
} finally {
  await browser.close();
}
