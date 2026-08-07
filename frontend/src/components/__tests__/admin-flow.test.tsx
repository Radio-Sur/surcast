import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { ApiKeysPage } from "@/pages/api-keys";
import { UsersPage } from "@/pages/users";
import { render, screen, setupAuth, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
});

function setupAuthenticated() {
  setupAuth();
}

describe("Users Page", () => {
  it("renders user list", async () => {
    setupAuthenticated();
    server.use(
      http.get("/api/users", () =>
        HttpResponse.json([
          {
            id: "1",
            username: "admin",
            name: "Admin User",
            role: "admin" as const,
            created_at: "2026-01-01T00:00:00Z",
          },
          { id: "2", username: "jdoe", name: "John Doe", role: "manager" as const, created_at: "2026-01-01T00:00:00Z" },
        ]),
      ),
    );
    render(<UsersPage />, { route: "/users" });
    await waitFor(
      () => {
        expect(screen.getByText("jdoe")).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
  });
});

describe("API Keys Page", () => {
  it("renders API keys list", async () => {
    setupAuthenticated();
    server.use(
      http.get("/api/api-keys", () =>
        HttpResponse.json([
          {
            id: "1",
            name: "Production Key",
            key_prefix: "sk_prod",
            is_active: true,
            last_used_at: null,
            expires_at: null,
            created_at: "2026-01-01T00:00:00Z",
          },
          {
            id: "2",
            name: "Dev Key",
            key_prefix: "sk_dev",
            is_active: false,
            last_used_at: null,
            expires_at: null,
            created_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
    );
    render(<ApiKeysPage />, { route: "/api-keys" });
    await waitFor(
      () => {
        expect(screen.getByText("Production Key")).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
  });
});
