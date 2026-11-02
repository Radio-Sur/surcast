import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { User } from "@/types";

export function createUsersService(client: HttpClient) {
  return {
    list: () => client.get<User[]>("/users"),
    create: (data: { username: string; password: string; role: "admin" | "demo" }) => client.post<User>("/users", data),
    update: (id: string, data: { role?: string }) => client.put<User>(`/users/${id}`, data),
    delete: (id: string) => client.delete(`/users/${id}`),
  };
}

export const usersService = createUsersService(httpClient);
