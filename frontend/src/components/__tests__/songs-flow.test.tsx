import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { fakeSongDetails } from "@/mocks/data";
import { server } from "@/mocks/server";
import { SongsPage } from "@/pages/songs";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("Songs Page", () => {
  it("shows empty library when no songs exist", async () => {
    server.use(http.get("/api/songs", () => HttpResponse.json([])));
    server.use(http.get("/api/stations", () => HttpResponse.json([])));
    render(<SongsPage />, { route: "/songs" });
    await waitFor(() => expect(screen.getByText(/no songs/i)).toBeInTheDocument(), { timeout: 5000 });
  });

  it("renders song list when songs are available", async () => {
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "1",
            title: "Song A",
            artist: "Artist A",
            album: "Album A",
            duration: 200,
            has_cover: false,
            station_ids: [],
          },
          {
            id: "2",
            title: "Song B",
            artist: "Artist B",
            album: "Album B",
            duration: 180,
            has_cover: false,
            station_ids: [1],
          },
        ]),
      ),
      http.get("/api/stations", () => HttpResponse.json([])),
    );
    render(<SongsPage />, { route: "/songs" });
    await waitFor(() => expect(screen.getByText("Song A")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText("Artist A")).toBeInTheDocument();
  });

  it("filters songs by search input", async () => {
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "1",
            title: "Bohemian Rhapsody",
            artist: "Queen",
            album: "A Night at the Opera",
            duration: 354,
            has_cover: false,
            station_ids: [],
          },
          {
            id: "2",
            title: "Imagine",
            artist: "John Lennon",
            album: "Imagine",
            duration: 183,
            has_cover: false,
            station_ids: [],
          },
        ]),
      ),
      http.get("/api/stations", () => HttpResponse.json([])),
    );
    render(<SongsPage />, { route: "/songs" });
    await waitFor(() => expect(screen.getByText("Bohemian Rhapsody")).toBeInTheDocument(), { timeout: 5000 });
    const searchInput = screen.getByPlaceholderText(/search/i);
    const user = userEvent.setup();
    await user.type(searchInput, "Bohemian");
    expect(screen.getByText("Bohemian Rhapsody")).toBeInTheDocument();
    expect(screen.queryByText("Imagine")).not.toBeInTheDocument();
  });

  it("opens upload dialog when clicking upload button", async () => {
    server.use(http.get("/api/songs", () => HttpResponse.json([])));
    server.use(http.get("/api/stations", () => HttpResponse.json([])));
    render(<SongsPage />, { route: "/songs" });
    await waitFor(() => expect(screen.getByText(/no songs/i)).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /upload/i }));
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("opens delete dialog and deletes a song", async () => {
    let deleteCalled = false;
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "1",
            title: "Song A",
            artist: "Artist A",
            album: "Album A",
            duration: 200,
            has_cover: false,
            station_ids: [],
          },
        ]),
      ),
      http.get("/api/stations", () => HttpResponse.json([])),
      http.delete("/api/songs/1", () => {
        deleteCalled = true;
        return HttpResponse.json({ success: true });
      }),
    );
    render(<SongsPage />, { route: "/songs" });
    await waitFor(() => expect(screen.getByText("Song A")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const deleteBtn = screen.getAllByRole("button").find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeTruthy();
    await user.click(deleteBtn!);
    await screen.findByRole("dialog");
    await user.click(screen.getByRole("button", { name: /delete/i }));
    await waitFor(() => expect(deleteCalled).toBe(true), { timeout: 3000 });
  });

  it("cancels song delete dialog", async () => {
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "1",
            title: "Song A",
            artist: "Artist A",
            album: "Album A",
            duration: 200,
            has_cover: false,
            station_ids: [],
          },
        ]),
      ),
      http.get("/api/stations", () => HttpResponse.json([])),
    );
    render(<SongsPage />, { route: "/songs" });
    await waitFor(() => expect(screen.getByText("Song A")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const deleteBtn = screen.getAllByRole("button").find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    await user.click(deleteBtn!);
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /cancel/i }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument(), { timeout: 3000 });
  });

  it("shows only missing songs when the missing filter is active", async () => {
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "1",
            title: "Present Song",
            artist: "Artist A",
            album: "Album A",
            duration: 200,
            has_cover: false,
            station_ids: [],
            file_path: "present.mp3",
            missing_since: null,
          },
          {
            id: "2",
            title: "Gone Song",
            artist: "Artist B",
            album: "Album B",
            duration: 180,
            has_cover: false,
            station_ids: [],
            file_path: "gone.mp3",
            missing_since: "2026-02-01T00:00:00Z",
          },
        ]),
      ),
      http.get("/api/stations", () => HttpResponse.json([])),
    );
    render(<SongsPage />, { route: "/songs" });
    await waitFor(() => expect(screen.getByText("Present Song")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /show missing only/i }));
    expect(screen.getByText("Gone Song")).toBeInTheDocument();
    expect(screen.queryByText("Present Song")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /delete all missing/i })).toBeInTheDocument();
  });

  it("opens the song details dialog with file metadata", async () => {
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "1",
            title: "Song A",
            artist: "Artist A",
            album: "Album A",
            duration: 200,
            has_cover: false,
            station_ids: [],
            file_path: "folder/song-a.mp3",
            missing_since: null,
          },
        ]),
      ),
      http.get("/api/songs/1/details", () =>
        HttpResponse.json(fakeSongDetails({ id: "1", title: "Song A", file_path: "folder/song-a.mp3" })),
      ),
      http.get("/api/stations", () => HttpResponse.json([])),
    );
    render(<SongsPage />, { route: "/songs" });
    await waitFor(() => expect(screen.getByText("Song A")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const infoBtn = screen.getAllByRole("button").find((b) => b.querySelector('[data-testid="InfoOutlinedIcon"]'));
    expect(infoBtn).toBeTruthy();
    await user.click(infoBtn!);
    await waitFor(() => expect(screen.getByText("folder/song-a.mp3")).toBeInTheDocument(), { timeout: 5000 });
  });
});
