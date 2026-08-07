import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { StreamStatus } from "@/types";

export function createStreamService(client: HttpClient) {
  return {
    status: (stationId: string) => client.get<StreamStatus>(`/stations/${stationId}/stream/status`),
    skip: (stationId: string) => client.post(`/stations/${stationId}/stream/skip`),
    play: (stationId: string) => client.post(`/stations/${stationId}/stream/play`),
    pause: (stationId: string) => client.post(`/stations/${stationId}/stream/pause`),
    stop: (stationId: string) => client.post(`/stations/${stationId}/stream/stop`),
    restart: (stationId: string) => client.post(`/stations/${stationId}/stream/restart`),
  };
}

export const streamService = createStreamService(httpClient);
