import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { ApiKey, ApiKeyCreated } from "@/types";

export function createApiKeysService(client: HttpClient) {
  return {
    list: () => client.get<ApiKey[]>("/api-keys"),
    create: (data: { name: string; expires_at?: string }) => client.post<ApiKeyCreated>("/api-keys", data),
    update: (id: string, data: { name?: string; is_active?: boolean }) => client.put<ApiKey>(`/api-keys/${id}`, data),
    delete: (id: string) => client.delete(`/api-keys/${id}`),
  };
}

export const apiKeysService = createApiKeysService(httpClient);
