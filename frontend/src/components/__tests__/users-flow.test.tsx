import { fireEvent, within } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { UsersPage } from "@/pages/users";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("Users Page", () => {
  it("keeps demo actions visible during menu dismissal and updates them for the next account", async () => {
    server.use(
      http.get("/api/users", () =>
        HttpResponse.json([
          { id: "1", username: "admin", role: "admin", created_at: "2026-01-01T00:00:00Z" },
          { id: "2", username: "demo", role: "demo", created_at: "2026-01-01T00:00:00Z" },
        ]),
      ),
    );
    render(<UsersPage />, { route: "/users" });
    const user = userEvent.setup();
    const demoRow = (await screen.findByText("demo")).closest("tr")!;
    await user.click(within(demoRow).getByRole("button"));
    const menu = screen.getByRole("menu");
    expect(within(menu).getByText("Configure sandbox")).toBeInTheDocument();
    const backdrop = document.querySelector(".MuiBackdrop-root")!;
    fireEvent.click(backdrop);
    // Check synchronously, while the closing animation still renders the menu.
    expect(within(menu).getByText("Configure sandbox")).toBeInTheDocument();
    await waitFor(() => expect(menu).not.toBeInTheDocument());
    const adminRow = screen.getByRole("cell", { name: "admin" }).closest("tr")!;
    await user.click(within(adminRow).getByRole("button"));
    expect(within(screen.getByRole("menu")).queryByText("Configure sandbox")).not.toBeInTheDocument();
  });

  it("shows empty state when no users exist", async () => {
    server.use(http.get("/api/users", () => HttpResponse.json([])));
    render(<UsersPage />, { route: "/users" });
    await waitFor(() => expect(screen.getByText(/no users/i)).toBeInTheDocument(), { timeout: 5000 });
  });

  it("renders user list", async () => {
    server.use(
      http.get("/api/users", () =>
        HttpResponse.json([
          { id: "1", username: "admin", role: "admin", created_at: "2026-01-01T00:00:00Z" },
          { id: "2", username: "jdoe", role: "demo", created_at: "2026-02-01T00:00:00Z" },
        ]),
      ),
    );
    render(<UsersPage />, { route: "/users" });
    await waitFor(() => expect(screen.getByText("jdoe")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.queryByText(/^name$/i)).not.toBeInTheDocument();
  });

  it("opens edit dialog, changes role, and saves", async () => {
    const testUsers = [
      { id: "1", username: "admin", role: "admin", created_at: "2026-01-01T00:00:00Z" },
      { id: "2", username: "jdoe", role: "demo", created_at: "2026-02-01T00:00:00Z" },
    ];
    let updatedUser: any = null;
    server.use(
      http.get("/api/users", () => HttpResponse.json(testUsers)),
      http.put("/api/users/:id", async ({ params, request }) => {
        if (params.id === "2") {
          updatedUser = await request.json();
          return HttpResponse.json({ ...testUsers[1], ...updatedUser });
        }
        return HttpResponse.json({ error: "Not found" }, { status: 404 });
      }),
    );
    render(<UsersPage />, { route: "/users" });
    await waitFor(() => expect(screen.getByText("jdoe")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const rows = screen.getAllByRole("row");
    const jdoeRow = rows.find((r) => r.textContent?.includes("jdoe"));
    expect(jdoeRow).toBeTruthy();
    const menuBtn = jdoeRow!.querySelector('[data-testid="MoreVertIcon"]')?.closest("button");
    expect(menuBtn).toBeTruthy();
    await user.click(menuBtn!);
    await user.click(screen.getByText(/edit/i));
    await screen.findByRole("dialog");
    const roleSelect = screen.getByLabelText(/role/i);
    await user.selectOptions(roleSelect, "admin");
    await user.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(updatedUser?.role).toBe("admin"), { timeout: 3000 });
  });

  it("opens delete dialog and deletes user", async () => {
    const testUsers = [
      { id: "1", username: "admin", role: "admin", created_at: "2026-01-01T00:00:00Z" },
      { id: "2", username: "jdoe", role: "demo", created_at: "2026-02-01T00:00:00Z" },
    ];
    let deleteCalled = false;
    server.use(
      http.get("/api/users", () => HttpResponse.json(testUsers)),
      http.delete("/api/users/:id", ({ params }) => {
        if (params.id === "2") {
          deleteCalled = true;
          return HttpResponse.json({ success: true });
        }
        return HttpResponse.json({ error: "Not found" }, { status: 404 });
      }),
    );
    render(<UsersPage />, { route: "/users" });
    await waitFor(() => expect(screen.getByText("jdoe")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const rows = screen.getAllByRole("row");
    const jdoeRow = rows.find((r) => r.textContent?.includes("jdoe"));
    expect(jdoeRow).toBeTruthy();
    const menuBtn = jdoeRow!.querySelector('[data-testid="MoreVertIcon"]')?.closest("button");
    expect(menuBtn).toBeTruthy();
    await user.click(menuBtn!);
    await user.click(screen.getByText(/delete/i));
    await screen.findByRole("dialog");
    await user.click(screen.getByRole("button", { name: /delete/i }));
    await waitFor(() => expect(deleteCalled).toBe(true), { timeout: 3000 });
  });
});
