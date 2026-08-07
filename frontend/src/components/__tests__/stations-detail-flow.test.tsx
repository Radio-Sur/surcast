import { HttpResponse, http } from "msw";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { StationDetailPage } from "@/pages/stations/detail";
import { render, screen, setupAuth, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

const mockStation = {
  id: "station-1",
  name: "Test Station",
  description: "A test station",
  slug: "test-station",
  stream_url: "main",
  current_song_index: 0,
  prebuffer_bytes: 16384,
  played_limit: 100,
  default_fade_ms: 3000,
  created_by: "1",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

describe("Station Detail Page", () => {
  it("shows not found when station does not exist", async () => {
    server.use(
      http.get("/api/stations/station-1", () => HttpResponse.json(null, { status: 404 })),
      http.get("/api/stations/station-1/songs", () => HttpResponse.json([])),
      http.get("/api/stations/station-1/queue", () => HttpResponse.json([])),
      http.get("/api/songs", () => HttpResponse.json([])),
      http.get("/api/playlists", () => HttpResponse.json([])),
    );
    render(
      <Routes>
        <Route path="/stations/:id" element={<StationDetailPage />} />
      </Routes>,
      { route: "/stations/station-1" },
    );
    await waitFor(() => expect(screen.getByText(/Request failed/)).toBeInTheDocument(), { timeout: 5000 });
  });

  it("renders station name and tabs", async () => {
    server.use(
      http.get("/api/stations/station-1", () => HttpResponse.json(mockStation)),
      http.get("/api/stations/station-1/songs", () => HttpResponse.json([])),
      http.get("/api/stations/station-1/queue", () => HttpResponse.json([])),
      http.get("/api/songs", () => HttpResponse.json([])),
      http.get("/api/playlists", () => HttpResponse.json([])),
    );
    render(
      <Routes>
        <Route path="/stations/:id" element={<StationDetailPage />} />
      </Routes>,
      { route: "/stations/station-1" },
    );
    await waitFor(() => expect(screen.getByText("Test Station")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByRole("tab", { name: /settings/i })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: /schedule/i })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: /library/i })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: /queue/i })).toBeInTheDocument();
  });

  it("shows start restart and edit buttons", async () => {
    server.use(
      http.get("/api/stations/station-1", () => HttpResponse.json(mockStation)),
      http.get("/api/stations/station-1/songs", () => HttpResponse.json([])),
      http.get("/api/stations/station-1/queue", () => HttpResponse.json([])),
      http.get("/api/songs", () => HttpResponse.json([])),
      http.get("/api/playlists", () => HttpResponse.json([])),
    );
    render(
      <Routes>
        <Route path="/stations/:id" element={<StationDetailPage />} />
      </Routes>,
      { route: "/stations/station-1" },
    );
    await waitFor(() => expect(screen.getByText("Test Station")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByRole("button", { name: "Start" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /restart/i })).toBeInTheDocument();
  });
});
