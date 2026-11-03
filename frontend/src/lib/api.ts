import axios from "axios";
import type { AuthResponse } from "@/types";
import type { HttpClient } from "./http-client";
import { localTokenStorage } from "./token-storage";

const api = axios.create({
  baseURL: "/api",
  headers: { "Content-Type": "application/json" },
});

api.interceptors.request.use((config) => {
  const token = localTokenStorage.getAccessToken();
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

let isRefreshing = false;
let failedQueue: Array<{ resolve: (token: string) => void; reject: (err: unknown) => void }> = [];

api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;
    if (
      error.response?.status === 401 &&
      originalRequest &&
      !originalRequest._retry &&
      !["/auth/login", "/auth/refresh"].includes(originalRequest.url)
    ) {
      originalRequest._retry = true;
      if (isRefreshing) {
        return new Promise<string>((resolve, reject) => {
          failedQueue.push({ resolve, reject });
        }).then((token) => {
          originalRequest.headers.Authorization = `Bearer ${token}`;
          return api(originalRequest);
        });
      }

      const refreshToken = localTokenStorage.getRefreshToken();
      if (refreshToken) {
        isRefreshing = true;
        try {
          const { data } = await axios.post<AuthResponse>("/api/auth/refresh", { refresh_token: refreshToken });
          if (localTokenStorage.getRefreshToken() !== refreshToken) throw new Error("Session changed during refresh");
          localTokenStorage.setRefreshToken(data.refresh_token);
          localTokenStorage.setAccessToken(data.access_token);

          for (const { resolve } of failedQueue) {
            resolve(data.access_token);
          }
          failedQueue = [];

          originalRequest.headers.Authorization = `Bearer ${data.access_token}`;
          return api(originalRequest);
        } catch (err) {
          for (const { reject } of failedQueue) {
            reject(err);
          }
          failedQueue = [];
          console.warn("Token refresh failed");
          if (localTokenStorage.getRefreshToken() === refreshToken) {
            localTokenStorage.clear();
          }
        } finally {
          isRefreshing = false;
        }
      } else {
        localTokenStorage.clear();
      }
    }
    return Promise.reject(error);
  },
);

export const httpClient: HttpClient = {
  get: <T>(url: string) => api.get<T>(url).then((r) => r.data),
  post: <T>(url: string, data?: unknown) => api.post<T>(url, data).then((r) => r.data),
  put: <T>(url: string, data?: unknown) => api.put<T>(url, data).then((r) => r.data),
  patch: <T>(url: string, data?: unknown) => api.patch<T>(url, data).then((r) => r.data),
  delete: (url: string, data?: unknown) => api.delete(url, data ? { data } : undefined).then(() => undefined),
  postFormData: <T>(url: string, data: FormData) =>
    api.post<T>(url, data, { headers: { "Content-Type": "multipart/form-data" } }).then((r) => r.data),
};

export { api };
