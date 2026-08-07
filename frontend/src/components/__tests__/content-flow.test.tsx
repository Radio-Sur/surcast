import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { PlaylistsListPage } from "@/pages/playlists/list";
import { SongsPage } from "@/pages/songs";
import { render, screen, setupAuth, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
});

function setupAuthenticated() {
  setupAuth();
}

describe("Songs Page", () => {
  it("renders song list when songs are available", async () => {
    setupAuthenticated();
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "1",
            title: "Bohemian Rhapsody",
            artist: "Queen",
            album: "A Night at the Opera",
            duration: 180,
            file_size: 5000000,
            mime_type: "audio/mpeg",
            has_cover: false,
            uploaded_by: "1",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
            station_ids: [],
          },
          {
            id: "2",
            title: "Imagine",
            artist: "John Lennon",
            album: "Imagine",
            duration: 200,
            file_size: 4000000,
            mime_type: "audio/mpeg",
            has_cover: false,
            uploaded_by: "1",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
            station_ids: [],
          },
        ]),
      ),
    );
    render(<SongsPage />, { route: "/songs" });
    await waitFor(
      () => {
        expect(screen.getByText("Bohemian Rhapsody")).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
    const imagineElements = screen.getAllByText("Imagine");
    expect(imagineElements.length).toBeGreaterThanOrEqual(1);
  });
});

describe("Playlists Page", () => {
  it("renders playlist list when playlists exist", async () => {
    setupAuthenticated();
    server.use(
      http.get("/api/playlists", () =>
        HttpResponse.json([
          {
            id: "1",
            name: "Morning Vibes",
            description: "Upbeat morning music",
            song_count: 10,
            total_duration_seconds: 3600,
            created_by: "1",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
    );
    render(<PlaylistsListPage />, { route: "/playlists" });
    await waitFor(
      () => {
        expect(screen.getByText("Morning Vibes")).toBeInTheDocument();
      },
      { timeout: 5000 },
    );
  });
});
