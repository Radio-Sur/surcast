import { HttpResponse, http } from "msw";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { api } from "@/lib/api";
import { server } from "@/mocks/server";

beforeEach(() => {
  localStorage.clear();
  server.resetHandlers();
});

afterEach(() => {
  localStorage.clear();
});

describe("api request interceptor", () => {
  it("adds Bearer token when access_token exists in localStorage", async () => {
    localStorage.setItem("access_token", "my-token");
    let capturedAuth: string | null = null;
    server.use(
      http.get("/api/test-auth", ({ request }) => {
        capturedAuth = request.headers.get("Authorization");
        return HttpResponse.json({ ok: true });
      }),
    );
    await api.get("/test-auth");
    expect(capturedAuth).toBe("Bearer my-token");
  });

  it("does not add Authorization header when no token", async () => {
    let capturedAuth: string | null = "unset";
    server.use(
      http.get("/api/test-noauth", ({ request }) => {
        capturedAuth = request.headers.get("Authorization");
        return HttpResponse.json({ ok: true });
      }),
    );
    await api.get("/test-noauth");
    expect(capturedAuth).toBeNull();
  });
});

describe("api response interceptor", () => {
  it("rejects non-401 errors without refresh attempt", async () => {
    let refreshCalled = false;
    server.use(
      http.get("/api/test-500", () => HttpResponse.json(null, { status: 500 })),
      http.post("/api/auth/refresh", () => {
        refreshCalled = true;
        return HttpResponse.json(null, { status: 200 });
      }),
    );
    await expect(api.get("/test-500")).rejects.toThrow();
    expect(refreshCalled).toBe(false);
  });

  it("retries the original request after successful token refresh on 401", async () => {
    let attempts = 0;
    let refreshCalled = false;
    server.use(
      http.get("/api/test-retry", () => {
        attempts++;
        return HttpResponse.json(null, { status: 401 });
      }),
      http.post("/api/auth/refresh", () => {
        refreshCalled = true;
        return HttpResponse.json({
          access_token: "new-access",
          refresh_token: "new-refresh",
          user: { id: "1", username: "admin", name: "Admin", role: "admin", created_at: "2026-01-01T00:00:00Z" },
        });
      }),
    );
    localStorage.setItem("access_token", "old-token");
    localStorage.setItem("refresh_token", "old-refresh");

    await expect(api.get("/test-retry")).rejects.toThrow();
    expect(refreshCalled).toBe(true);
    expect(attempts).toBe(2);
    expect(localStorage.getItem("access_token")).toBe("new-access");
    expect(localStorage.getItem("refresh_token")).toBe("new-refresh");
  });

  it("redirects to /login when token refresh fails on 401", async () => {
    server.use(
      http.get("/api/test-redirect", () => HttpResponse.json(null, { status: 401 })),
      http.post("/api/auth/refresh", () => HttpResponse.json(null, { status: 500 })),
    );
    localStorage.setItem("access_token", "old-token");
    localStorage.setItem("refresh_token", "old-refresh");

    await expect(api.get("/test-redirect")).rejects.toThrow();
    expect(localStorage.getItem("access_token")).toBeNull();
    expect(localStorage.getItem("refresh_token")).toBeNull();
  });

  it("rejects 401 when no refresh_token is available", async () => {
    server.use(http.get("/api/test-no-refresh", () => HttpResponse.json(null, { status: 401 })));
    localStorage.setItem("access_token", "old-token");

    await expect(api.get("/test-no-refresh")).rejects.toThrow();
  });
});
