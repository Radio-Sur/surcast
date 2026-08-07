import { HttpResponse, http } from "msw";

const now = Date.now();
const HOUR = 3600 * 1000;

function makePoints(count: number, stepMs: number) {
  const points = [];
  for (let i = count - 1; i >= 0; i--) {
    points.push({
      time: new Date(now - i * stepMs).toISOString(),
      listeners: 5 + ((i * 7) % 20),
    });
  }
  return points;
}

export const listenersHandlers = [
  http.get("/api/stations/:id/listeners/history", ({ request }) => {
    const url = new URL(request.url);
    const range = url.searchParams.get("range") ?? "7d";
    const bucket = range === "24h" ? 2 : range === "7d" ? 12 : 48;
    return HttpResponse.json({ points: makePoints(bucket, HOUR) });
  }),

  http.get("/api/listeners/overview", ({ request }) => {
    const url = new URL(request.url);
    const range = url.searchParams.get("range") ?? "7d";
    const bucket = range === "24h" ? 2 : range === "7d" ? 12 : 48;
    return HttpResponse.json({
      range,
      total_now: 27,
      stations: [
        {
          station_id: "1",
          name: "Test Station",
          listeners: 12,
          updated_at: new Date(now).toISOString(),
          online: true,
        },
        {
          station_id: "2",
          name: "Second Station",
          listeners: 15,
          updated_at: new Date(now).toISOString(),
          online: true,
        },
      ],
      by_hour: [0, 1, 2, 3].map((hour) => ({ hour, avg_listeners: 4 + hour })),
      by_weekday: [1, 2, 3, 4, 5, 6, 7].map((weekday) => ({
        weekday,
        avg_listeners: 3 + weekday,
      })),
      series: makePoints(bucket, HOUR),
    });
  }),
];
