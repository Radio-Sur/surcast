import { HttpResponse, http } from "msw";
import { fakeScheduleEvent } from "@/mocks/data";

const schedules = new Map<string, ReturnType<typeof fakeScheduleEvent>[]>();
const scheduleEntries = new Map<string, { id: string; day_of_week: number; start_time: string; end_time: string }[]>();

export function seedScheduleEvents(stationId = "1", count = 1) {
  const events: ReturnType<typeof fakeScheduleEvent>[] = [];
  for (let i = 0; i < count; i++) {
    events.push(
      fakeScheduleEvent({ station_id: stationId, start_date: "2026-01-01", start_time: "09:00", end_time: "10:00" }),
    );
  }
  schedules.set(stationId, events);
}

export const scheduleHandlers = [
  http.get("/api/stations/:id/schedule-events", ({ params, request }) => {
    const url = new URL(request.url);
    const from = url.searchParams.get("from") || "";
    const to = url.searchParams.get("to") || "";
    const events = schedules.get(params.id as string) || [];
    const filtered = events.filter((e) => {
      if (from && e.start_date < from) return false;
      if (to && e.start_date > to) return false;
      return true;
    });
    return HttpResponse.json(filtered);
  }),

  http.post("/api/stations/:id/schedule-events", async ({ params, request }) => {
    const body = (await request.json()) as Partial<ReturnType<typeof fakeScheduleEvent>>;
    const current = schedules.get(params.id as string) || [];
    const event = fakeScheduleEvent({ station_id: params.id as string, ...body });
    current.push(event);
    schedules.set(params.id as string, current);
    return HttpResponse.json(event, { status: 201 });
  }),

  http.put("/api/stations/:id/schedule-events/:eventId", async ({ params, request }) => {
    const body = (await request.json()) as Partial<ReturnType<typeof fakeScheduleEvent>>;
    const current = schedules.get(params.id as string) || [];
    const idx = current.findIndex((e) => e.id === (params.eventId as string));
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    current[idx] = { ...current[idx], ...body };
    schedules.set(params.id as string, current);
    return HttpResponse.json(current[idx]);
  }),

  http.delete("/api/stations/:id/schedule-events/:eventId", ({ params }) => {
    const current = schedules.get(params.id as string) || [];
    schedules.set(
      params.id as string,
      current.filter((e) => e.id !== (params.eventId as string)),
    );
    return HttpResponse.json({ success: true });
  }),

  http.get("/api/stations/:id/schedules", ({ params }) => {
    return HttpResponse.json(scheduleEntries.get(params.id as string) || []);
  }),

  http.post("/api/stations/:id/schedules", async ({ params, request }) => {
    const body = (await request.json()) as { day_of_week: number; start_time: string; end_time: string };
    const current = scheduleEntries.get(params.id as string) || [];
    const entry = { id: String(Date.now()), ...body };
    current.push(entry);
    scheduleEntries.set(params.id as string, current);
    return HttpResponse.json(entry, { status: 201 });
  }),

  http.put("/api/stations/:id/schedules/:scheduleId", async ({ params, request }) => {
    const body = (await request.json()) as { day_of_week?: number; start_time?: string; end_time?: string };
    const current = scheduleEntries.get(params.id as string) || [];
    const idx = current.findIndex((e) => e.id === (params.scheduleId as string));
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    current[idx] = { ...current[idx], ...body };
    scheduleEntries.set(params.id as string, current);
    return HttpResponse.json(current[idx]);
  }),

  http.delete("/api/stations/:id/schedules/:scheduleId", ({ params }) => {
    const current = scheduleEntries.get(params.id as string) || [];
    scheduleEntries.set(
      params.id as string,
      current.filter((e) => e.id !== (params.scheduleId as string)),
    );
    return HttpResponse.json({ success: true });
  }),
];
