import { STORAGE_KEYS } from "@/lib/constants";
import type { TokenStorage } from "./http-client";

export const localTokenStorage: TokenStorage = {
  getAccessToken: () => localStorage.getItem(STORAGE_KEYS.ACCESS_TOKEN),
  getRefreshToken: () => localStorage.getItem(STORAGE_KEYS.REFRESH_TOKEN),
  setAccessToken: (token: string) => localStorage.setItem(STORAGE_KEYS.ACCESS_TOKEN, token),
  setRefreshToken: (token: string) => localStorage.setItem(STORAGE_KEYS.REFRESH_TOKEN, token),
  clear: () => {
    localStorage.removeItem(STORAGE_KEYS.ACCESS_TOKEN);
    localStorage.removeItem(STORAGE_KEYS.REFRESH_TOKEN);
  },
};
