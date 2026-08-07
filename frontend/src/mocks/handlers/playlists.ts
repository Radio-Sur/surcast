import { HttpResponse, http } from "msw";
import { fakePlaylist } from "@/mocks/data";

const playlists = new Map<string, ReturnType<typeof fakePlaylist>>();

export function seedPlaylists(count = 2) {
  for (let i = 0; i < count; i++) {
    const p = fakePlaylist({ name: `Playlist ${i + 1}` });
    playlists.set(p.id, p);
  }
}

export const playlistsHandlers = [
  http.get("/api/playlists", () => {
    return HttpResponse.json(Array.from(playlists.values()));
  }),

  http.get("/api/playlists/:id", ({ params }) => {
    const playlist = playlists.get(params.id as string);
    if (!playlist) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json(playlist);
  }),

  http.post("/api/playlists", async ({ request }) => {
    const body = (await request.json()) as { name: string; description?: string };
    const playlist = fakePlaylist({ name: body.name, description: body.description || "" });
    playlists.set(playlist.id, playlist);
    return HttpResponse.json(playlist, { status: 201 });
  }),

  http.put("/api/playlists/:id", async ({ params, request }) => {
    const body = (await request.json()) as Partial<ReturnType<typeof fakePlaylist>>;
    const existing = playlists.get(params.id as string);
    if (!existing) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const updated = { ...existing, ...body, updated_at: new Date().toISOString() };
    playlists.set(params.id as string, updated);
    return HttpResponse.json(updated);
  }),

  http.delete("/api/playlists/:id", ({ params }) => {
    playlists.delete(params.id as string);
    return HttpResponse.json({ success: true });
  }),

  http.get("/api/playlists/:id/songs", () => {
    return HttpResponse.json([]);
  }),

  http.post("/api/playlists/:id/songs", async ({ request }) => {
    const body = (await request.json()) as { song_ids: string[] };
    return HttpResponse.json(
      body.song_ids.map((songId) => ({
        id: `ps_${songId}`,
        playlist_id: "1",
        song_id: songId,
        position: 0,
        title: "Test Song",
        artist: "Test Artist",
        album: "Test Album",
        duration: 180,
        has_cover: false,
        mime_type: "audio/mpeg",
      })),
      { status: 201 },
    );
  }),

  http.delete("/api/playlists/:id/songs/:songId", () => {
    return HttpResponse.json({ success: true });
  }),

  http.post("/api/playlists/:playlistId/add-to-queue/:stationId", () => {
    return HttpResponse.json({ success: true, added: 1 }, { status: 200 });
  }),
];
