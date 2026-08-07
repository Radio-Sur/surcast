import { HttpResponse, http } from "msw";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { AppLayout } from "@/components/layout/app-layout";
import { server } from "@/mocks/server";
import { render, screen, setupAuth, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("AppLayout", () => {
  it("shows loading spinner while auth is loading", async () => {
    server.use(http.get("/api/auth/me", () => new Promise(() => {})));
    render(<AppLayout />, { route: "/" });
    expect(screen.getByRole("progressbar")).toBeInTheDocument();
  });

  it("renders sidebar and header when authenticated", async () => {
    server.use(
      http.get("/api/auth/me", () =>
        HttpResponse.json({
          id: "1",
          username: "admin",
          name: "Admin",
          role: "admin",
          created_at: "2026-01-01T00:00:00Z",
        }),
      ),
    );
    render(
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<div>Dashboard Content</div>} />
        </Route>
      </Routes>,
      { route: "/" },
    );
    await waitFor(() => expect(screen.getByText("Dashboard")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText("Dashboard Content")).toBeInTheDocument();
  });

  it("renders sidebar when authenticated and meQuery succeeds", async () => {
    server.use(
      http.get("/api/auth/me", () =>
        HttpResponse.json({
          id: "1",
          username: "admin",
          name: "Admin",
          role: "admin",
          created_at: "2026-01-01T00:00:00Z",
        }),
      ),
    );
    render(
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<div>Dashboard Content</div>} />
        </Route>
        <Route path="/login" element={<div>Login Page</div>} />
      </Routes>,
      { route: "/" },
    );
    await waitFor(() => expect(screen.getByText("Dashboard")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText("Dashboard Content")).toBeInTheDocument();
  });

  it("redirects to login when unauthenticated", async () => {
    localStorage.removeItem("access_token");
    server.use(http.get("/api/auth/me", () => HttpResponse.json(null, { status: 401 })));
    render(
      <Routes>
        <Route path="/" element={<AppLayout />} />
        <Route path="/login" element={<div>Login Page</div>} />
      </Routes>,
      { route: "/" },
    );
    await waitFor(() => expect(screen.getByText("Login Page")).toBeInTheDocument());
  });
});
