import { test, expect } from "@playwright/test";

test.describe("Login flow", () => {
  test("shows login page", async ({ page }) => {
    await page.goto("/login");
    await expect(page.getByText("Sign in to your account")).toBeVisible({ timeout: 10000 });
  });

  test("shows error with invalid credentials", async ({ page }) => {
    await page.goto("/login");
    await expect(page.getByText("Sign in to your account")).toBeVisible({ timeout: 10000 });

    await page.getByPlaceholder("admin").fill("wrong");
    await page.getByPlaceholder("••••••••").fill("wrong");
    await page.getByRole("button", { name: "Sign in" }).click();

    await expect(page.getByText("Invalid username or password")).toBeVisible({ timeout: 10000 });
  });
});

test.describe("Authenticated dashboard", () => {
  test("logs in and shows dashboard", async ({ page }) => {
    await page.goto("/login");
    await expect(page.getByText("Sign in to your account")).toBeVisible({ timeout: 10000 });

    await page.getByPlaceholder("admin").fill("admin");
    await page.getByPlaceholder("••••••••").fill("password123");
    await page.getByRole("button", { name: "Sign in" }).click();

    await expect(page.getByText(/welcome/i)).toBeVisible({ timeout: 15000 });
  });

  test("navigates to sidebar pages", async ({ page }) => {
    await page.goto("/login");
    await expect(page.getByText("Sign in to your account")).toBeVisible({ timeout: 10000 });
    await page.getByPlaceholder("admin").fill("admin");
    await page.getByPlaceholder("••••••••").fill("password123");
    await page.getByRole("button", { name: "Sign in" }).click();
    await expect(page.getByText(/welcome/i)).toBeVisible({ timeout: 15000 });

    await page.getByRole("button", { name: "Stations" }).click();
    await expect(page.getByRole("heading", { name: "Stations" })).toBeVisible({ timeout: 5000 });

    await page.getByRole("button", { name: "Music" }).click();
    await expect(page.getByText("Music Library")).toBeVisible({ timeout: 5000 });
  });
});
