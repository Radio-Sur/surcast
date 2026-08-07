import { test, expect } from "@playwright/test";
import { apiDelete, findByName } from "./helpers";

test.describe("Stations", () => {
  test("creates a station and views its detail tabs", async ({ page }) => {
    const name = `Station ${Date.now()}`;
    await page.goto("/stations");
    await expect(page.getByRole("heading", { name: "Stations" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: "Add Station" }).click();
    await page.getByRole("textbox", { name: /station name/i }).fill(name);
    await page.getByRole("textbox", { name: /description/i }).fill("Created by Playwright");
    await page.getByRole("textbox", { name: /mount point/i }).fill("e2e");
    await page.getByRole("button", { name: "Create Station" }).click();
    await expect(page.getByRole("cell", { name }).first()).toBeVisible({ timeout: 10000 });

    await page.getByRole("cell", { name }).first().click();
    await expect(page.getByRole("tab", { name: /library/i })).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole("tab", { name: /queue/i })).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole("tab", { name: /schedule/i })).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole("tab", { name: /settings/i })).toBeVisible({ timeout: 5000 });

    const station = await findByName(page, "/api/stations", name);
    if (station) {
      await apiDelete(page, `/api/stations/${station.id}`);
    }
  });

  test("edits a station name", async ({ page }) => {
    const name = `Station ${Date.now()}`;
    await page.goto("/stations");
    await expect(page.getByRole("heading", { name: "Stations" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: "Add Station" }).click();
    await page.getByRole("textbox", { name: /station name/i }).fill(name);
    await page.getByRole("textbox", { name: /mount point/i }).fill("e2e");
    await page.getByRole("button", { name: "Create Station" }).click();
    await expect(page.getByRole("cell", { name }).first()).toBeVisible({ timeout: 10000 });

    await page.getByRole("cell", { name }).first().click();
    await page.getByRole("tab", { name: /settings/i }).click();
    await expect(page.getByRole("heading", { name: "Station Settings" })).toBeVisible({ timeout: 5000 });

    const station = await findByName(page, "/api/stations", name);
    if (station) {
      await apiDelete(page, `/api/stations/${station.id}`);
    }
  });

  test("deletes a station", async ({ page }) => {
    const name = `DeleteMe ${Date.now()}`;
    await page.goto("/stations");
    await expect(page.getByRole("heading", { name: "Stations" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: "Add Station" }).click();
    await page.getByRole("textbox", { name: /station name/i }).fill(name);
    await page.getByRole("textbox", { name: /mount point/i }).fill("del");
    await page.getByRole("button", { name: "Create Station" }).click();
    await expect(page.getByRole("cell", { name }).first()).toBeVisible({ timeout: 10000 });

    const row = page.getByRole("row").filter({ hasText: name });
    await row.getByRole("button", { name: /delete/i }).click();
    await expect(page.getByRole("dialog")).toBeVisible({ timeout: 5000 });
    await page.getByRole("button", { name: /confirm/i }).click();
    await expect(page.getByRole("cell", { name })).not.toBeVisible();
  });
});
