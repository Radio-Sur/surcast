import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { DashboardPage } from "@/pages/dashboard";
import { LoginPage } from "@/pages/login";
import { SetupPage } from "@/pages/setup";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
});

async function waitForAuth() {
  return waitFor(
    () => {
      expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
    },
    { timeout: 3000 },
  );
}

describe("Login Page", () => {
  it("shows login form when setup is complete", async () => {
    server.use(http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: true })));
    render(<LoginPage />, { route: "/login" });
    await waitForAuth();
    expect(screen.getByText("Sign in to your account")).toBeInTheDocument();
  });

  it("shows error on failed login", async () => {
    server.use(http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: true })));
    render(<LoginPage />, { route: "/login" });
    await waitForAuth();

    const user = userEvent.setup();
    await user.type(screen.getByPlaceholderText("admin"), "fail");
    await user.type(screen.getByPlaceholderText("••••••••"), "wrong");
    await user.click(screen.getByText("Sign in"));

    await waitFor(() => {
      expect(screen.getByText("Invalid credentials")).toBeInTheDocument();
    });
  });
});

describe("Setup Page", () => {
  it("shows setup form when setup is incomplete", async () => {
    server.use(http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: false })));
    render(<SetupPage />, { route: "/setup" });
    await waitForAuth();
    expect(screen.getByText("Create the initial admin account")).toBeInTheDocument();
  });
});

describe("Dashboard Page", () => {
  it("shows welcome and station list for authenticated user", async () => {
    setupAuth();
    server.use(
      http.get("/api/stations", () =>
        HttpResponse.json([
          {
            id: "1",
            name: "My Station",
            description: "Test",
            slug: "my-station",
            stream_url: null,
            current_song_index: 0,
            prebuffer_bytes: 0,
            played_limit: 100,
            default_fade_ms: 2000,
            created_by: "1",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
    );
    render(<DashboardPage />, { route: "/" });
    await waitFor(
      () => {
        expect(screen.getByText(/welcome back/i)).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
    await waitFor(
      () => {
        expect(screen.getByText("My Station")).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
    expect(screen.getByText("admin")).toBeInTheDocument();
  });
});
