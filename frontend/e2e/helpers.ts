import { Page } from "@playwright/test";

const BASE = "http://localhost:6767";

async function authHeaders(page: Page): Promise<Record<string, string>> {
  const token = await page.evaluate(() => localStorage.getItem("access_token"));
  return {
    "Content-Type": "application/json",
    Authorization: `Bearer ${token}`,
  };
}

export async function apiGet<T = any>(page: Page, path: string): Promise<T> {
  const res = await page.request.get(`${BASE}${path}`, { headers: await authHeaders(page) });
  return res.json();
}

export async function apiPost<T = any>(page: Page, path: string, data: unknown): Promise<T> {
  const res = await page.request.post(`${BASE}${path}`, { headers: await authHeaders(page), data });
  return res.json();
}

export async function apiDelete(page: Page, path: string): Promise<boolean> {
  const res = await page.request.delete(`${BASE}${path}`, { headers: await authHeaders(page) });
  return res.ok();
}

export async function findByName(page: Page, path: string, name: string): Promise<any> {
  const items = await apiGet<any[]>(page, path);
  return items.find((i: any) => i.name === name) || null;
}
