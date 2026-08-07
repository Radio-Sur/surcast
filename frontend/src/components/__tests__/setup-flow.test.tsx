import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { SetupPage } from "@/pages/setup";
import { render, screen, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  localStorage.clear();
  server.resetHandlers();
});

describe("Setup Page", () => {
  it("shows setup form when setup is incomplete", async () => {
    server.use(http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: false })));
    render(<SetupPage />, { route: "/setup" });
    await waitFor(() => expect(screen.getByText("Create the initial admin account")).toBeInTheDocument(), {
      timeout: 5000,
    });
    expect(screen.getByPlaceholderText("admin")).toBeInTheDocument();
  });

  it("creates admin account and shows success", async () => {
    server.use(
      http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: false })),
      http.post("/api/setup/init", () => HttpResponse.json({ id: "1", username: "admin", role: "admin" })),
    );
    render(<SetupPage />, { route: "/setup" });
    await waitFor(() => expect(screen.getByText("Create the initial admin account")).toBeInTheDocument(), {
      timeout: 5000,
    });
    const user = userEvent.setup();
    await user.type(screen.getByPlaceholderText("admin"), "admin");
    await user.type(screen.getByPlaceholderText("••••••••"), "password123");
    await user.click(screen.getByRole("button", { name: /create admin account/i }));
    await waitFor(() => expect(screen.getByText(/Account created/i)).toBeInTheDocument(), { timeout: 5000 });
  });
});
