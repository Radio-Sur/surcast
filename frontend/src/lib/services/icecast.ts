import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";

export interface IcecastSettings {
  id: string;
  enabled: boolean;
  mode: string;
  port: number;
  source_password: string;
  admin_user: string;
  admin_password: string;
  external_url: string | null;
  external_source_pw: string | null;
  external_admin_pw: string | null;
}

export interface IcecastStatusResponse {
  settings: IcecastSettings;
  running: boolean;
}

export interface IcecastSettingsUpdate {
  enabled?: boolean;
  mode?: string;
  port?: number;
  source_password?: string;
  admin_user?: string;
  admin_password?: string;
  external_url?: string | null;
  external_source_pw?: string | null;
  external_admin_pw?: string | null;
}

export interface IcecastActionResponse {
  message?: string;
}

export function createIcecastService(client: HttpClient) {
  return {
    status: () => client.get<IcecastStatusResponse>("/admin/icecast"),
    update: (update: IcecastSettingsUpdate) => client.patch<IcecastActionResponse>("/admin/icecast", update),
    start: () => client.post<IcecastActionResponse>("/admin/icecast/start"),
    stop: () => client.post<IcecastActionResponse>("/admin/icecast/stop"),
    test: () => client.post<IcecastActionResponse>("/admin/icecast/test"),
  };
}

export const icecastService = createIcecastService(httpClient);
