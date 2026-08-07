import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";

export interface AppConfig {
  icecast_public_url: string;
}

export function createConfigService(client: HttpClient) {
  return {
    get: () => client.get<AppConfig>("/config"),
  };
}

export const configService = createConfigService(httpClient);
