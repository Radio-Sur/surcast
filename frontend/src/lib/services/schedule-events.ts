import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { CreateScheduleEventRequest, ScheduleEvent } from "@/types";

export function createScheduleEventsService(client: HttpClient) {
  return {
    list: (stationId: string, from?: string, to?: string) => {
      const params = new URLSearchParams();
      if (from) params.set("from", from);
      if (to) params.set("to", to);
      return client.get<ScheduleEvent[]>(`/stations/${stationId}/schedule-events?${params}`);
    },
    create: (stationId: string, data: CreateScheduleEventRequest) =>
      client.post<ScheduleEvent>(`/stations/${stationId}/schedule-events`, data),
    update: (stationId: string, id: string, data: Partial<CreateScheduleEventRequest>) =>
      client.put<ScheduleEvent>(`/stations/${stationId}/schedule-events/${id}`, data),
    delete: (stationId: string, id: string) => client.delete(`/stations/${stationId}/schedule-events/${id}`),
  };
}

export const scheduleEventsService = createScheduleEventsService(httpClient);
