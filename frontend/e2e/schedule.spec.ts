import { test, expect } from "@playwright/test";
import { apiDelete, findByName } from "./helpers";

test.describe("Schedule Events", () => {
  test("creates and deletes a schedule event", async ({ page }) => {
    const stationName = `SchedStation ${Date.now()}`;

    await page.goto("/stations");
    await expect(page.getByRole("heading", { name: "Stations" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: "Add Station" }).click();
    await page.getByRole("textbox", { name: /station name/i }).fill(stationName);
    await page.getByRole("textbox", { name: /mount point/i }).fill("sched");
    await page.getByRole("button", { name: "Create Station" }).click();
    await expect(page.getByRole("cell", { name: stationName }).first()).toBeVisible({ timeout: 10000 });

    await page.getByRole("cell", { name: stationName }).first().click();
    await expect(page.getByRole("tab", { name: "Schedule" })).toBeVisible({ timeout: 5000 });

    const station = await findByName(page, "/api/stations", stationName);
    if (station) {
      await apiDelete(page, `/api/stations/${station.id}`);
    }
  });
});
