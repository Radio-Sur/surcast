import { HttpResponse, http, ws } from "msw";
import { fakeStreamStatus } from "@/mocks/data";

export const streamHandlers = [
  http.get("/api/stations/:id/stream/status", () => {
    return HttpResponse.json(fakeStreamStatus());
  }),

  http.post("/api/stations/:id/stream/skip", () => {
    return HttpResponse.json({ success: true });
  }),

  http.post("/api/stations/:id/stream/play", () => {
    return HttpResponse.json({ success: true });
  }),

  http.post("/api/stations/:id/stream/pause", () => {
    return HttpResponse.json({ success: true });
  }),

  http.post("/api/stations/:id/stream/stop", () => {
    return HttpResponse.json({ success: true });
  }),

  http.post("/api/stations/:id/stream/restart", () => {
    return HttpResponse.json({ success: true });
  }),
];

export const streamWSLink = ws.link("wss://example.com/ws");
