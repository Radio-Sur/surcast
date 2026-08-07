import { test, expect } from "@playwright/test";
import { apiDelete, findByName } from "./helpers";

test.describe("Queue Management", () => {
  test("shows queue tab for a station", async ({ page }) => {
    const stationName = `QueueStation ${Date.now()}`;

    await page.goto("/stations");
    await expect(page.getByRole("heading", { name: "Stations" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: "Add Station" }).click();
    await page.getByRole("textbox", { name: /station name/i }).fill(stationName);
    await page.getByRole("textbox", { name: /mount point/i }).fill("queue");
    await page.getByRole("button", { name: "Create Station" }).click();
    await expect(page.getByRole("cell", { name: stationName }).first()).toBeVisible({ timeout: 10000 });

    await page.getByRole("cell", { name: stationName }).first().click();
    await expect(page.getByRole("tab", { name: /queue/i })).toBeVisible({ timeout: 5000 });

    await page.getByRole("tab", { name: /queue/i }).click();
    await expect(page.getByText(stationName)).toBeVisible({ timeout: 5000 });

    const station = await findByName(page, "/api/stations", stationName);
    if (station) {
      await apiDelete(page, `/api/stations/${station.id}`);
    }
  });
});
