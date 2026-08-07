import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { ListenerRange, ListenersHistoryPoint, ListenersOverview } from "@/types";

export function createListenersService(client: HttpClient) {
  return {
    stationHistory: (stationId: string, range: ListenerRange) =>
      client.get<{ points: ListenersHistoryPoint[] }>(`/stations/${stationId}/listeners/history?range=${range}`),
    overview: (range: ListenerRange) => client.get<ListenersOverview>(`/listeners/overview?range=${range}`),
  };
}

export const listenersService = createListenersService(httpClient);
