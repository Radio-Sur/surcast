import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { QueueItem } from "@/types";

export function createStationQueueService(client: HttpClient) {
  return {
    list: (stationId: string) => client.get<QueueItem[]>(`/stations/${stationId}/queue`),
    add: (stationId: string, song_ids: string[]) => client.post(`/stations/${stationId}/queue`, { song_ids }),
    remove: (stationId: string, itemId: string) => client.delete(`/stations/${stationId}/queue/${itemId}`),
    reorder: (stationId: string, queue_item_ids: string[]) =>
      client.put(`/stations/${stationId}/queue/reorder`, { queue_item_ids }),
    insert: (stationId: string, params: { song_id: string; position: number }) =>
      client.post(`/stations/${stationId}/queue/insert`, params),
    removePlaylist: (stationId: string, playlistId: string) =>
      client.delete(`/stations/${stationId}/queue/playlist/${playlistId}`),
  };
}

export const stationQueueService = createStationQueueService(httpClient);
