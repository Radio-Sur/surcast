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
    if (error.response?.status === 401 && !originalRequest._retry) {
      if (isRefreshing) {
        return new Promise<string>((resolve, reject) => {
          failedQueue.push({ resolve, reject });
        }).then((token) => {
          originalRequest.headers.Authorization = `Bearer ${token}`;
          return api(originalRequest);
        });
      }

      originalRequest._retry = true;
      isRefreshing = true;

      const refreshToken = localTokenStorage.getRefreshToken();
      if (refreshToken) {
        try {
          const { data } = await axios.post<AuthResponse>("/api/auth/refresh", { refresh_token: refreshToken });
          localTokenStorage.setAccessToken(data.access_token);
          localTokenStorage.setRefreshToken(data.refresh_token);

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
          console.warn("Token refresh failed, redirecting to login");
          localTokenStorage.clear();
          window.location.href = "/login";
        } finally {
          isRefreshing = false;
        }
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
