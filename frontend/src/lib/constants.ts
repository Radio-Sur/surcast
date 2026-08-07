export const STORAGE_KEYS = {
  ACCESS_TOKEN: "access_token",
  REFRESH_TOKEN: "refresh_token",
  THEME: "surcast-theme",
  ACCENT: "surcast-accent",
} as const;

export const WS_RECONNECT_BASE_MS = 1000;
export const WS_RECONNECT_MAX_MS = 30000;

export const UPCOMING_KEY_PREFIX = "queue_upcoming_open_";
