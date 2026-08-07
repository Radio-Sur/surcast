import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { ApiKeysPage } from "@/pages/api-keys";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("API Keys Page", () => {
  it("shows empty state when no keys exist", async () => {
    server.use(http.get("/api/api-keys", () => HttpResponse.json([])));
    render(<ApiKeysPage />, { route: "/api-keys" });
    await waitFor(() => expect(screen.getByText(/No API keys/i)).toBeInTheDocument(), { timeout: 5000 });
  });

  it("renders key list when keys are available", async () => {
    server.use(
      http.get("/api/api-keys", () =>
        HttpResponse.json([
          {
            id: "k1",
            name: "Production Key",
            key_prefix: "prod_abc",
            is_active: true,
            last_used_at: null,
            expires_at: null,
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
          },
          {
            id: "k2",
            name: "Dev Key",
            key_prefix: "dev_xyz",
            is_active: false,
            last_used_at: "2026-06-01T00:00:00Z",
            expires_at: "2027-01-01T00:00:00Z",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
    );
    render(<ApiKeysPage />, { route: "/api-keys" });
    await waitFor(() => expect(screen.getByText("Production Key")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText("Dev Key")).toBeInTheDocument();
    expect(screen.getByText("prod_abc...")).toBeInTheDocument();
  });

  it("opens create dialog and creates a key", async () => {
    server.use(http.get("/api/api-keys", () => HttpResponse.json([])));
    server.use(
      http.post("/api/api-keys", () =>
        HttpResponse.json({
          id: "k3",
          name: "New Key",
          key: "sk_new_secret_key_123",
          key_prefix: "sk_new",
          is_active: true,
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        }),
      ),
    );
    render(<ApiKeysPage />, { route: "/api-keys" });
    await waitFor(() => expect(screen.getByText(/No API keys/i)).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /new/i }));
    const nameInput = screen.getByLabelText(/key name/i);
    await user.type(nameInput, "New Key");
    await user.click(screen.getByRole("button", { name: /create/i }));
    await waitFor(() => expect(screen.getByText("sk_new_secret_key_123")).toBeInTheDocument(), { timeout: 5000 });
  });

  it("opens delete dialog and deletes a key", async () => {
    let deleteCalled = false;
    server.use(
      http.get("/api/api-keys", () =>
        HttpResponse.json([
          {
            id: "k1",
            name: "Production Key",
            key_prefix: "prod_abc",
            is_active: true,
            last_used_at: null,
            expires_at: null,
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
      http.delete("/api/api-keys/k1", () => {
        deleteCalled = true;
        return HttpResponse.json({ success: true });
      }),
    );
    render(<ApiKeysPage />, { route: "/api-keys" });
    await waitFor(() => expect(screen.getByText("Production Key")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const deleteBtn = screen.getAllByRole("button").find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeTruthy();
    await user.click(deleteBtn!);
    await screen.findByRole("dialog");
    await user.click(screen.getByRole("button", { name: /delete/i }));
    await waitFor(() => expect(deleteCalled).toBe(true), { timeout: 3000 });
  });
});
