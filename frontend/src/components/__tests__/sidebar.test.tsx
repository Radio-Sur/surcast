import { beforeEach, describe, expect, it } from "vitest";
import { Sidebar } from "@/components/layout/sidebar";
import { server } from "@/mocks/server";
import { render, screen, setupAuth } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("Sidebar", () => {
  it("shows the Icecast entry for demo accounts", async () => {
    server.use(
      http.get("/api/auth/me", () =>
        HttpResponse.json({ id: "demo", username: "demo", role: "demo", created_at: "2026-01-01T00:00:00Z" }),
      ),
    );
    render(<Sidebar />, { route: "/" });
    expect(await screen.findByText("DEMO")).toBeInTheDocument();
    expect(screen.getByText("Icecast")).toBeInTheDocument();
  });

  it("renders nav items", async () => {
    render(<Sidebar />, { route: "/" });
    expect(await screen.findByText("Dashboard")).toBeInTheDocument();
    expect(screen.getByText("Stations")).toBeInTheDocument();
    expect(screen.getByText("Music")).toBeInTheDocument();
    expect(screen.getByText("Playlists")).toBeInTheDocument();
    expect(screen.getByText("API Keys")).toBeInTheDocument();
    expect(screen.getByText("Users")).toBeInTheDocument();
  });

  it("renders admin section for admin users", async () => {
    render(<Sidebar />, { route: "/" });
    expect(await screen.findByText("Icecast")).toBeInTheDocument();
  });
});

import { HttpResponse, http } from "msw";
