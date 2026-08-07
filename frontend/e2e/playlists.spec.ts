import { test, expect } from "@playwright/test";
import { apiDelete, findByName } from "./helpers";

test.describe("Playlists", () => {
  test("creates a new playlist", async ({ page }) => {
    const name = `Playlist ${Date.now()}`;
    await page.goto("/playlists");
    await expect(page.getByRole("heading", { name: "Playlists" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: "New Playlist" }).click();
    await page.getByRole("textbox", { name: /name/i }).fill(name);
    await page.getByRole("textbox", { name: /description/i }).fill("Created by Playwright");
    await page.getByRole("button", { name: /create/i }).click();
    await expect(page.getByRole("cell", { name }).first()).toBeVisible({ timeout: 10000 });

    const playlist = await findByName(page, "/api/playlists", name);
    if (playlist) {
      await apiDelete(page, `/api/playlists/${playlist.id}`);
    }
  });

  test("deletes a playlist", async ({ page }) => {
    const name = `DeleteMe ${Date.now()}`;
    await page.goto("/playlists");
    await expect(page.getByRole("heading", { name: "Playlists" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: "New Playlist" }).click();
    await page.getByRole("textbox", { name: /name/i }).fill(name);
    await page.getByRole("textbox", { name: /description/i }).fill("Cleanup test");
    await page.getByRole("button", { name: /create/i }).click();
    await expect(page.getByRole("cell", { name }).first()).toBeVisible({ timeout: 10000 });

    const row = page.getByRole("row").filter({ hasText: name });
    await row.getByRole("button", { name: /delete/i }).click();
    await expect(page.getByRole("dialog")).toBeVisible({ timeout: 5000 });
    await page.getByRole("button", { name: /confirm/i }).click();
    await expect(page.getByRole("cell", { name })).not.toBeVisible();
  });
});
