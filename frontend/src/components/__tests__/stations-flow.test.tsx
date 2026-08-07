import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { DashboardPage } from "@/pages/dashboard";
import { render, screen, setupAuth, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
});

function setupAuthenticated(stations?: any[]) {
  setupAuth();
  server.use(http.get("/api/stations", () => HttpResponse.json(stations || [])));
}

describe("Stations Dashboard", () => {
  it("shows get started when no stations exist", async () => {
    setupAuthenticated([]);
    render(<DashboardPage />, { route: "/" });
    await waitFor(
      () => {
        expect(screen.getByRole("heading", { name: /get started/i })).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
  });

  it("lists stations when data is available", async () => {
    setupAuthenticated([
      {
        id: "1",
        name: "Pop Radio",
        description: "Top 40 hits",
        slug: "pop-radio",
        stream_url: null,
        current_song_index: 0,
        prebuffer_bytes: 0,
        played_limit: 100,
        default_fade_ms: 2000,
        created_by: "1",
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
      },
      {
        id: "2",
        name: "Rock FM",
        description: "Rock classics",
        slug: "rock-fm",
        stream_url: null,
        current_song_index: 0,
        prebuffer_bytes: 0,
        played_limit: 100,
        default_fade_ms: 2000,
        created_by: "1",
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
      },
    ]);
    render(<DashboardPage />, { route: "/" });
    await waitFor(
      () => {
        expect(screen.getByText("Pop Radio")).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
    expect(screen.getByText("Rock FM")).toBeInTheDocument();
  });
});
