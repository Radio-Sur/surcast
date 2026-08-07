import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { ScheduleEntry, ScheduleSourceType } from "@/types";

export function createSchedulesService(client: HttpClient) {
  return {
    list: (stationId: string) => client.get<ScheduleEntry[]>(`/stations/${stationId}/schedules`),
    create: (
      stationId: string,
      data: {
        day_of_week: number;
        start_time: string;
        end_time: string;
        source_type?: ScheduleSourceType;
        playlist_id?: string | null;
        auto_dj_mode?: string | null;
        auto_dj_avoid_repeat?: boolean | null;
        auto_dj_min_gap?: number | null;
        auto_dj_songs_ahead?: number | null;
      },
    ) => client.post<ScheduleEntry>(`/stations/${stationId}/schedules`, data),
    update: (
      stationId: string,
      id: string,
      data: {
        day_of_week?: number;
        start_time?: string;
        end_time?: string;
        source_type?: ScheduleSourceType;
        playlist_id?: string | null;
        auto_dj_mode?: string | null;
        auto_dj_avoid_repeat?: boolean | null;
        auto_dj_min_gap?: number | null;
        auto_dj_songs_ahead?: number | null;
      },
    ) => client.put<ScheduleEntry>(`/stations/${stationId}/schedules/${id}`, data),
    delete: (stationId: string, id: string) => client.delete(`/stations/${stationId}/schedules/${id}`),
  };
}

export const schedulesService = createSchedulesService(httpClient);
