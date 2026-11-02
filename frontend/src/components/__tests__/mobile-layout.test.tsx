import { HttpResponse, http } from "msw";
import { Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AppLayout } from "@/components/layout/app-layout";
import { fakeSong, fakeStation } from "@/mocks/data";
import { server } from "@/mocks/server";
import { SongsPage } from "@/pages/songs";
import { StationsListPage } from "@/pages/stations/list";
import { UsersPage } from "@/pages/users";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  setupAuth();
  vi.spyOn(window, "matchMedia").mockImplementation((query) => ({
    matches: query.includes("max-width"),
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  }));
});

afterEach(() => vi.restoreAllMocks());

describe("Mobile layout", () => {
  it("opens navigation from the hamburger and closes it after navigating", async () => {
    render(
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<div>Home content</div>} />
          <Route path="/stations" element={<div>Stations content</div>} />
        </Route>
      </Routes>,
    );
    const user = userEvent.setup();
    await screen.findByText("Home content");
    expect(screen.queryByText("Stations")).not.toBeVisible();
    await user.click(screen.getByRole("button", { name: "Open menu" }));
    expect(screen.getByText("Stations")).toBeVisible();
    await user.click(screen.getByText("Stations"));
    expect(await screen.findByText("Stations content")).toBeVisible();
    await waitFor(() => expect(screen.queryByText("Stations")).not.toBeVisible());
  });

  it("renders compact user rows rather than a wide table and preserves account actions", async () => {
    server.use(
      http.get("/api/users", () =>
        HttpResponse.json([
          { id: "1", username: "admin", role: "admin", created_at: "2026-01-01T00:00:00Z" },
          { id: "2", username: "demo", role: "demo", created_at: "2026-01-01T00:00:00Z" },
        ]),
      ),
    );
    render(<UsersPage />, { route: "/users" });
    expect(await screen.findByText("demo")).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    const user = userEvent.setup();
    await user.click(screen.getAllByRole("button", { name: "Edit" })[1]);
    expect(await screen.findByText("Configure sandbox")).toBeVisible();
  });

  it("groups song actions in a menu and opens the edit dialog", async () => {
    server.use(http.get("/api/songs", () => HttpResponse.json([fakeSong({ title: "Mobile song" })])));
    render(<SongsPage />, { route: "/songs" });
    expect(await screen.findByText("Mobile song")).toBeVisible();
    expect(screen.queryByRole("button", { name: "Edit" })).not.toBeInTheDocument();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Actions" }));
    expect(screen.getByRole("menuitem", { name: "Delete" })).toBeVisible();
    await user.click(screen.getByRole("menuitem", { name: "Edit" }));
    expect(await screen.findByRole("dialog")).toBeVisible();
    expect(screen.getByDisplayValue("Mobile song")).toBeVisible();
  });

  it("does not navigate when opening station actions or selecting edit", async () => {
    server.use(http.get("/api/stations", () => HttpResponse.json([fakeStation({ name: "Mobile station" })])));
    render(
      <Routes>
        <Route path="/stations" element={<StationsListPage />} />
        <Route path="/stations/:id" element={<div>Station detail</div>} />
      </Routes>,
      { route: "/stations" },
    );
    expect(await screen.findByText("Mobile station")).toBeVisible();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Actions" }));
    await user.click(screen.getByRole("menuitem", { name: "Edit" }));
    expect(await screen.findByRole("dialog")).toBeVisible();
    expect(screen.queryByText("Station detail")).not.toBeInTheDocument();
  });
});
