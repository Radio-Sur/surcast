import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { AutoDJSection } from "@/pages/stations/auto-dj-section";
import { render, screen, setupAuth, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

const mockPlaylists = [
  {
    id: "pl1",
    name: "Rock",
    slug: "rock",
    description: "",
    song_count: 10,
    total_duration_seconds: 3600,
    created_by: "1",
    created_at: "",
    updated_at: "",
  },
  {
    id: "pl2",
    name: "Jazz",
    slug: "jazz",
    description: "",
    song_count: 5,
    total_duration_seconds: 1800,
    created_by: "1",
    created_at: "",
    updated_at: "",
  },
];

describe("AutoDJSection", () => {
  it("shows loading state", () => {
    server.use(http.get("/api/stations/s1/auto-fill", () => new Promise(() => {})));
    render(<AutoDJSection stationId="s1" playlists={mockPlaylists} />);
    expect(screen.getByText(/loading/i)).toBeInTheDocument();
  });

  it("renders form when config loads", async () => {
    server.use(
      http.get("/api/stations/s1/auto-fill", () =>
        HttpResponse.json({
          enabled: true,
          mode: "random",
          source_type: "station_library",
          source_playlist_id: null,
          avoid_artist_repeat: true,
          min_song_gap: 5,
          songs_ahead: 10,
          weighted_playlists: [],
        }),
      ),
    );
    render(<AutoDJSection stationId="s1" playlists={mockPlaylists} />);
    await waitFor(() => expect(screen.getByText(/enable/i)).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText(/trigger/i)).toBeInTheDocument();
  });

  it("shows mode select when enabled", async () => {
    server.use(
      http.get("/api/stations/s1/auto-fill", () =>
        HttpResponse.json({
          enabled: true,
          mode: "random",
          source_type: "station_library",
          source_playlist_id: null,
          avoid_artist_repeat: true,
          min_song_gap: 5,
          songs_ahead: 10,
          weighted_playlists: [],
        }),
      ),
    );
    render(<AutoDJSection stationId="s1" playlists={mockPlaylists} />);
    await waitFor(() => expect(screen.getByText(/enable autodj/i)).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText("Random")).toBeInTheDocument();
    expect(screen.getByText(/station library/i)).toBeInTheDocument();
  });

  it("shows playlist select when source is playlist", async () => {
    server.use(
      http.get("/api/stations/s1/auto-fill", () =>
        HttpResponse.json({
          enabled: true,
          mode: "sequential",
          source_type: "playlist",
          source_playlist_id: "pl1",
          avoid_artist_repeat: false,
          min_song_gap: 3,
          songs_ahead: 5,
          weighted_playlists: [],
        }),
      ),
    );
    render(<AutoDJSection stationId="s1" playlists={mockPlaylists} />);
    await waitFor(() => expect(screen.getByText("Rock")).toBeInTheDocument(), { timeout: 5000 });
  });
});
