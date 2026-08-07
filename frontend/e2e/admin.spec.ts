import { test, expect } from "@playwright/test";
import { apiDelete, findByName } from "./helpers";

test.describe("Admin - Users", () => {
  test("shows user list", async ({ page }) => {
    await page.goto("/users");
    await expect(page.getByRole("heading", { name: "Users" })).toBeVisible({ timeout: 10000 });
  });

  test("dashboard shows welcome and stations section", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByText(/welcome/i)).toBeVisible({ timeout: 10000 });
    await expect(page.getByRole("heading", { name: /stations/i })).toBeVisible({ timeout: 10000 });
  });
});

test.describe("Admin - API Keys", () => {
  test("creates and deletes an API key", async ({ page }) => {
    const keyName = `APIKey ${Date.now()}`;

    await page.goto("/api-keys");
    await expect(page.getByRole("heading", { name: "API Keys" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: /new|add|create/i }).click();
    await expect(page.getByRole("dialog")).toBeVisible({ timeout: 5000 });
    await page.getByRole("textbox", { name: /name/i }).fill(keyName);
    await page.getByRole("button", { name: /create/i }).click();

    const key = await findByName(page, "/api/api-keys", keyName);
    if (key) {
      await apiDelete(page, `/api/api-keys/${key.id}`);
    }
  });
});
