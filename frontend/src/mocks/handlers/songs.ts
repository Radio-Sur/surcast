import { HttpResponse, http } from "msw";
import { fakeSong } from "@/mocks/data";

const songs = new Map<string, ReturnType<typeof fakeSong>>();

export function seedSongs(count = 3) {
  for (let i = 0; i < count; i++) {
    const s = fakeSong({ title: `Song ${i + 1}`, artist: "Test Artist" });
    songs.set(s.id, s);
  }
}

export const songsHandlers = [
  http.get("/api/songs", () => {
    return HttpResponse.json(Array.from(songs.values()));
  }),

  http.get("/api/songs/:id", ({ params }) => {
    const song = songs.get(params.id as string);
    if (!song) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json(song);
  }),

  http.post("/api/songs", async ({ request }) => {
    const body = await request.formData();
    const song = fakeSong({
      title: (body.get("title") as string) || "Uploaded Song",
      artist: (body.get("artist") as string) || "Uploaded Artist",
      album: (body.get("album") as string) || "Uploaded Album",
    });
    songs.set(song.id, song);
    return HttpResponse.json(song, { status: 201 });
  }),

  http.post("/api/songs/zip", async () => {
    const results = [fakeSong({ title: "Zipped Song 1" }), fakeSong({ title: "Zipped Song 2" })];
    for (const s of results) songs.set(s.id, s);
    return HttpResponse.json(results, { status: 201 });
  }),

  http.put("/api/songs/:id", async ({ params, request }) => {
    const body = (await request.json()) as Partial<ReturnType<typeof fakeSong>>;
    const existing = songs.get(params.id as string);
    if (!existing) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const updated = { ...existing, ...body, updated_at: new Date().toISOString() };
    songs.set(params.id as string, updated);
    return HttpResponse.json(updated);
  }),

  http.delete("/api/songs/:id", ({ params }) => {
    songs.delete(params.id as string);
    return HttpResponse.json({ success: true });
  }),
];
