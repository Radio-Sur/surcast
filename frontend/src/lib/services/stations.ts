import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { Station } from "@/types";

export function createStationsService(client: HttpClient) {
  return {
    list: () => client.get<Station[]>("/stations"),
    get: (id: string) => client.get<Station>(`/stations/${id}`),
    create: (data: { name: string; description?: string; stream_url?: string; played_limit?: number }) =>
      client.post<Station>("/stations", data),
    update: (
      id: string,
      data: {
        name?: string;
        description?: string;
        stream_url?: string;
        prebuffer_bytes?: number;
        played_limit?: number;
        default_fade_ms?: number;
        transition_mode?: "crossfade" | "autocue" | "off";
        autocue_fade_max_ms?: number;
      },
    ) => client.put<Station>(`/stations/${id}`, data),
    delete: (id: string) => client.delete(`/stations/${id}`),
  };
}

export const stationsService = createStationsService(httpClient);
