import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { PaginatedStationSongs, SongSelections, StationSong } from "@/types";

export function createStationLibraryService(client: HttpClient) {
  return {
    list: (stationId: string, params?: { page?: number; per_page?: number }) => {
      const q = params
        ? "?" +
          Object.entries(params)
            .filter(([_, v]) => v !== undefined)
            .map(([k, v]) => `${k}=${v}`)
            .join("&")
        : "";
      return client.get<PaginatedStationSongs>(`/stations/${stationId}/songs${q}`);
    },
    add: (stationId: string, data: SongSelections) =>
      client.post<StationSong[]>(`/stations/${stationId}/songs`, {
        song_ids: data.songIds,
        artist_names: data.artistNames,
        album_selectors: data.albumSelectors,
      }),
    remove: (stationId: string, songId: string) => client.delete(`/stations/${stationId}/songs/${songId}`),
  };
}

export const stationLibraryService = createStationLibraryService(httpClient);
