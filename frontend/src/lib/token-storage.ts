import { STORAGE_KEYS } from "@/lib/constants";
import type { TokenStorage } from "./http-client";

const TOKEN_CHANGED = "surcast-token-changed";
export function subscribeToken(listener: () => void) {
  window.addEventListener(TOKEN_CHANGED, listener);
  window.addEventListener("storage", listener);
  return () => {
    window.removeEventListener(TOKEN_CHANGED, listener);
    window.removeEventListener("storage", listener);
  };
}

export const localTokenStorage: TokenStorage = {
  getAccessToken: () => localStorage.getItem(STORAGE_KEYS.ACCESS_TOKEN),
  getRefreshToken: () => localStorage.getItem(STORAGE_KEYS.REFRESH_TOKEN),
  setAccessToken: (token: string) => {
    localStorage.setItem(STORAGE_KEYS.ACCESS_TOKEN, token);
    window.dispatchEvent(new Event(TOKEN_CHANGED));
  },
  setRefreshToken: (token: string) => localStorage.setItem(STORAGE_KEYS.REFRESH_TOKEN, token),
  clear: () => {
    localStorage.removeItem(STORAGE_KEYS.ACCESS_TOKEN);
    localStorage.removeItem(STORAGE_KEYS.REFRESH_TOKEN);
    window.dispatchEvent(new Event(TOKEN_CHANGED));
  },
};
