import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { AutoFillConfig, AutoFillPlaylistEntry, ScheduleSourceType } from "@/types";

export function createAutoFillService(client: HttpClient) {
  return {
    get: (stationId: string) => client.get<AutoFillConfig>(`/stations/${stationId}/auto-fill`),
    update: (
      stationId: string,
      data: {
        enabled?: boolean;
        mode?: string;
        source_type?: ScheduleSourceType;
        source_playlist_id?: string | null;
        avoid_artist_repeat?: boolean;
        min_song_gap?: number;
        songs_ahead?: number;
      },
    ) => client.put<AutoFillConfig>(`/stations/${stationId}/auto-fill`, data),
    addPlaylist: (stationId: string, data: { playlist_id: string; weight?: number }) =>
      client.post<AutoFillPlaylistEntry>(`/stations/${stationId}/auto-fill/playlists`, data),
    updatePlaylist: (stationId: string, id: string, data: { weight?: number }) =>
      client.put<AutoFillPlaylistEntry>(`/stations/${stationId}/auto-fill/playlists/${id}`, data),
    deletePlaylist: (stationId: string, id: string) =>
      client.delete(`/stations/${stationId}/auto-fill/playlists/${id}`),
    trigger: (stationId: string) => client.post(`/stations/${stationId}/auto-fill/trigger`),
  };
}

export const autoFillService = createAutoFillService(httpClient);
