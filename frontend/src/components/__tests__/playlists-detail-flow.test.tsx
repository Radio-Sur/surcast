import { HttpResponse, http } from "msw";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { PlaylistDetailPage } from "@/pages/playlists/detail";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

const mockPlaylist = {
  id: "playlist-1",
  name: "Morning Vibes",
  description: "Good morning songs",
  song_count: 2,
  duration: 400,
  created_by: "1",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

describe("Playlist Detail Page", () => {
  it("shows not found when playlist does not exist", async () => {
    server.use(http.get("/api/playlists/playlist-1", () => HttpResponse.json({ error: "Not found" }, { status: 404 })));
    render(
      <Routes>
        <Route path="/playlists/:id" element={<PlaylistDetailPage />} />
      </Routes>,
      { route: "/playlists/playlist-1" },
    );
    await waitFor(() => expect(screen.getByText(/Request failed/)).toBeInTheDocument(), { timeout: 5000 });
  });

  it("renders playlist name description and songs", async () => {
    server.use(
      http.get("/api/playlists/playlist-1", () => HttpResponse.json(mockPlaylist)),
      http.get("/api/playlists/playlist-1/songs", ({ request }) => {
        const url = new URL(request.url);
        const page = parseInt(url.searchParams.get("page") || "1", 10);
        const perPage = parseInt(url.searchParams.get("per_page") || "50", 10);
        const allSongs = [
          {
            id: "ps-1",
            song_id: "s1",
            title: "Song One",
            artist: "Artist A",
            album: "Album A",
            duration: 200,
            position: 0,
          },
          {
            id: "ps-2",
            song_id: "s2",
            title: "Song Two",
            artist: "Artist B",
            album: "Album B",
            duration: 200,
            position: 1,
          },
        ];
        const total = allSongs.length;
        const start = (page - 1) * perPage;
        const songs = allSongs.slice(start, start + perPage);
        return HttpResponse.json({ songs, total, page, per_page: perPage });
      }),
      http.get("/api/songs", () => HttpResponse.json([])),
      http.get("/api/stations", () => HttpResponse.json([])),
    );
    render(
      <Routes>
        <Route path="/playlists/:id" element={<PlaylistDetailPage />} />
      </Routes>,
      { route: "/playlists/playlist-1" },
    );
    await waitFor(() => expect(screen.getByText("Morning Vibes")).toBeInTheDocument(), { timeout: 5000 });
    await waitFor(() => expect(screen.getByText("Song One")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText("Song Two")).toBeInTheDocument();
  });

  it("shows empty state when playlist has no songs", async () => {
    server.use(
      http.get("/api/playlists/playlist-1", () => HttpResponse.json({ ...mockPlaylist, song_count: 0 })),
      http.get("/api/playlists/playlist-1/songs", () =>
        HttpResponse.json({ songs: [], total: 0, page: 1, per_page: 50 }),
      ),
      http.get("/api/songs", () => HttpResponse.json([])),
      http.get("/api/stations", () => HttpResponse.json([])),
    );
    render(
      <Routes>
        <Route path="/playlists/:id" element={<PlaylistDetailPage />} />
      </Routes>,
      { route: "/playlists/playlist-1" },
    );
    await waitFor(() => expect(screen.getByText(/no songs/i)).toBeInTheDocument(), { timeout: 5000 });
  });

  it("opens edit dialog and saves changes", async () => {
    server.use(
      http.get("/api/playlists/playlist-1", () => HttpResponse.json(mockPlaylist)),
      http.get("/api/playlists/playlist-1/songs", () =>
        HttpResponse.json({ songs: [], total: 0, page: 1, per_page: 50 }),
      ),
      http.get("/api/songs", () => HttpResponse.json([])),
      http.get("/api/stations", () => HttpResponse.json([])),
      http.put("/api/playlists/playlist-1", () => HttpResponse.json({ ...mockPlaylist, name: "Updated Name" })),
    );
    render(
      <Routes>
        <Route path="/playlists/:id" element={<PlaylistDetailPage />} />
      </Routes>,
      { route: "/playlists/playlist-1" },
    );
    await waitFor(() => expect(screen.getByText("Morning Vibes")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /edit/i }));
    const nameInput = screen.getByLabelText(/name/i);
    await user.clear(nameInput);
    await user.type(nameInput, "Updated Name");
    await user.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument(), { timeout: 3000 });
  });

  it("removes a song from the playlist", async () => {
    let deleteCalled = false;
    server.use(
      http.get("/api/playlists/playlist-1", () => HttpResponse.json(mockPlaylist)),
      http.get("/api/playlists/playlist-1/songs", () =>
        HttpResponse.json({
          songs: [
            {
              id: "ps-1",
              song_id: "s1",
              title: "Song One",
              artist: "Artist A",
              album: "Album A",
              duration: 200,
              position: 0,
            },
          ],
          total: 1,
          page: 1,
          per_page: 50,
        }),
      ),
      http.get("/api/songs", () => HttpResponse.json([])),
      http.get("/api/stations", () => HttpResponse.json([])),
      http.delete("/api/playlists/playlist-1/songs/s1", () => {
        deleteCalled = true;
        return HttpResponse.json({ success: true });
      }),
    );
    render(
      <Routes>
        <Route path="/playlists/:id" element={<PlaylistDetailPage />} />
      </Routes>,
      { route: "/playlists/playlist-1" },
    );
    await waitFor(() => expect(screen.getByText("Morning Vibes")).toBeInTheDocument(), { timeout: 5000 });
    await waitFor(() => expect(screen.getByText("Song One")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const deleteBtn = screen.getAllByRole("button").find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeTruthy();
    await user.click(deleteBtn!);
    await waitFor(() => expect(deleteCalled).toBe(true), { timeout: 3000 });
  });
});
