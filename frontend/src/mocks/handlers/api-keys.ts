import { HttpResponse, http } from "msw";
import { fakeApiKey } from "@/mocks/data";

const apiKeys = new Map<string, ReturnType<typeof fakeApiKey>>();

export function seedApiKeys() {
  apiKeys.clear();
  const key = fakeApiKey({ id: "1", name: "Production Key" });
  apiKeys.set(key.id, key);
}

export const apiKeysHandlers = [
  http.get("/api/api-keys", () => {
    return HttpResponse.json(Array.from(apiKeys.values()));
  }),

  http.post("/api/api-keys", async ({ request }) => {
    const body = (await request.json()) as { name: string; expires_at?: string | null };
    const key = fakeApiKey({ name: body.name, expires_at: body.expires_at ?? null });
    apiKeys.set(key.id, key);
    return HttpResponse.json({ ...key, key: `sk_${key.id}_secret` }, { status: 201 });
  }),

  http.put("/api/api-keys/:id", async ({ params, request }) => {
    const body = (await request.json()) as { name?: string; is_active?: boolean };
    const existing = apiKeys.get(params.id as string);
    if (!existing) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const updated = { ...existing, ...body };
    apiKeys.set(params.id as string, updated);
    return HttpResponse.json(updated);
  }),

  http.delete("/api/api-keys/:id", ({ params }) => {
    apiKeys.delete(params.id as string);
    return HttpResponse.json({ success: true });
  }),
];
