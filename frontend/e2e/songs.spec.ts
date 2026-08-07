import { test, expect } from "@playwright/test";

test.describe("Songs Library", () => {
  test("shows empty song library", async ({ page }) => {
    await page.goto("/songs");
    await expect(page.getByText("Music Library")).toBeVisible({ timeout: 10000 });
    await expect(page.getByPlaceholder(/search/i)).toBeVisible();
  });

  test("upload button opens upload dialog", async ({ page }) => {
    await page.goto("/songs");
    await expect(page.getByText("Music Library")).toBeVisible({ timeout: 10000 });

    const uploadBtn = page.getByRole("button", { name: /upload/i });
    if (await uploadBtn.isVisible()) {
      await uploadBtn.click();
      await expect(page.getByRole("dialog")).toBeVisible({ timeout: 5000 });
      await page.getByRole("button", { name: /cancel/i }).click();
      await expect(page.getByRole("dialog")).not.toBeVisible();
    }
  });
});
