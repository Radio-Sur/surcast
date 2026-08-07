import { HttpResponse, http } from "msw";
import { fakeQueueItem, fakeStation, fakeStationSong } from "@/mocks/data";

const stations = new Map<string, ReturnType<typeof fakeStation>>();
const stationSongs = new Map<string, ReturnType<typeof fakeStationSong>[]>();
const stationQueue = new Map<string, ReturnType<typeof fakeQueueItem>[]>();

export function seedStations(count = 2) {
  for (let i = 0; i < count; i++) {
    const s = fakeStation({ name: `Station ${i + 1}` });
    stations.set(s.id, s);
    stationSongs.set(s.id, [fakeStationSong()]);
    stationQueue.set(s.id, [fakeQueueItem({ station_id: s.id })]);
  }
}

export const stationsHandlers = [
  http.get("/api/stations", () => {
    return HttpResponse.json(Array.from(stations.values()));
  }),

  http.post("/api/stations", async ({ request }) => {
    const body = (await request.json()) as { name: string; description?: string };
    const station = fakeStation({ name: body.name, description: body.description || "" });
    stations.set(station.id, station);
    stationSongs.set(station.id, []);
    stationQueue.set(station.id, []);
    return HttpResponse.json(station, { status: 201 });
  }),

  http.get("/api/stations/:id", ({ params }) => {
    const station = stations.get(params.id as string);
    if (!station) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json(station);
  }),

  http.put("/api/stations/:id", async ({ params, request }) => {
    const body = (await request.json()) as Partial<ReturnType<typeof fakeStation>>;
    const existing = stations.get(params.id as string);
    if (!existing) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const updated = { ...existing, ...body, updated_at: new Date().toISOString() };
    stations.set(params.id as string, updated);
    return HttpResponse.json(updated);
  }),

  http.delete("/api/stations/:id", ({ params }) => {
    stations.delete(params.id as string);
    stationSongs.delete(params.id as string);
    stationQueue.delete(params.id as string);
    return HttpResponse.json({ success: true });
  }),

  http.get("/api/stations/:id/songs", ({ params }) => {
    return HttpResponse.json(stationSongs.get(params.id as string) || []);
  }),

  http.post("/api/stations/:id/songs", async ({ params, request }) => {
    const body = (await request.json()) as { song_ids: string[] };
    const current = stationSongs.get(params.id as string) || [];
    for (const songId of body.song_ids) {
      current.push(fakeStationSong({ song_id: songId }));
    }
    stationSongs.set(params.id as string, current);
    return HttpResponse.json(current, { status: 201 });
  }),

  http.delete("/api/stations/:id/songs/:songId", ({ params }) => {
    const current = stationSongs.get(params.id as string) || [];
    stationSongs.set(
      params.id as string,
      current.filter((s) => s.song_id !== (params.songId as string)),
    );
    return HttpResponse.json({ success: true });
  }),

  http.get("/api/stations/:id/queue", ({ params }) => {
    return HttpResponse.json(stationQueue.get(params.id as string) || []);
  }),

  http.post("/api/stations/:id/queue", async ({ params, request }) => {
    const body = (await request.json()) as { song_id: string; title?: string; artist?: string };
    const current = stationQueue.get(params.id as string) || [];
    const item = fakeQueueItem({
      station_id: params.id as string,
      song_id: body.song_id,
      title: body.title || "Test Song",
      artist: body.artist || "Test Artist",
      position: current.length,
    });
    current.push(item);
    stationQueue.set(params.id as string, current);
    return HttpResponse.json(item, { status: 201 });
  }),

  http.delete("/api/stations/:id/queue/:itemId", ({ params }) => {
    const current = stationQueue.get(params.id as string) || [];
    stationQueue.set(
      params.id as string,
      current.filter((q) => q.id !== (params.itemId as string)),
    );
    return HttpResponse.json({ success: true });
  }),

  http.put("/api/stations/:id/queue/reorder", async ({ params, request }) => {
    const body = (await request.json()) as { item_id: string; new_position: number };
    const current = stationQueue.get(params.id as string) || [];
    const idx = current.findIndex((q) => q.id === body.item_id);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const [item] = current.splice(idx, 1);
    current.splice(body.new_position, 0, item);
    current.forEach((q, i) => {
      q.position = i;
    });
    stationQueue.set(params.id as string, current);
    return HttpResponse.json(current);
  }),

  http.post("/api/stations/:id/queue/insert", async ({ params, request }) => {
    const body = (await request.json()) as { song_id: string; position: number };
    const current = stationQueue.get(params.id as string) || [];
    const item = fakeQueueItem({ station_id: params.id as string, song_id: body.song_id, position: body.position });
    current.splice(body.position, 0, item);
    current.forEach((q, i) => {
      q.position = i;
    });
    stationQueue.set(params.id as string, current);
    return HttpResponse.json(item, { status: 201 });
  }),

  http.post("/api/stations/:id/queue/insert-multiple", async ({ params, request }) => {
    const body = (await request.json()) as { items: { song_id: string }[]; position: number };
    const current = stationQueue.get(params.id as string) || [];
    const newItems = body.items.map((item, i) =>
      fakeQueueItem({
        station_id: params.id as string,
        song_id: item.song_id,
        position: body.position + i,
      }),
    );
    current.splice(body.position, 0, ...newItems);
    current.forEach((q, i) => {
      q.position = i;
    });
    stationQueue.set(params.id as string, current);
    return HttpResponse.json(newItems, { status: 201 });
  }),

  http.delete("/api/stations/:id/queue/playlist/:playlistId", ({ params }) => {
    const current = stationQueue.get(params.id as string) || [];
    stationQueue.set(
      params.id as string,
      current.filter((q) => q.origin_playlist_id !== (params.playlistId as string)),
    );
    return HttpResponse.json({ success: true });
  }),
];
