import { ThemeProvider } from "@emotion/react";
import { createTheme } from "@mui/material";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { I18nextProvider } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { useScheduleEvents } from "@/hooks/use-schedule-events";
import i18n from "@/i18n";
import { server } from "@/mocks/server";
import { AuthProvider } from "@/providers/auth-provider";
import { setupAuth } from "@/test/test-utils";

function createWrapper() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <I18nextProvider i18n={i18n}>
          <ThemeProvider theme={createTheme()}>
            <MemoryRouter>
              <AuthProvider>{children}</AuthProvider>
            </MemoryRouter>
          </ThemeProvider>
        </I18nextProvider>
      </QueryClientProvider>
    );
  };
}

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("useScheduleEvents", () => {
  it("returns empty array when no events exist", async () => {
    server.use(http.get("/api/stations/s1/schedule-events", () => HttpResponse.json([])));
    const { result } = renderHook(() => useScheduleEvents("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toEqual([]));
  });

  it("returns schedule events for a station", async () => {
    server.use(
      http.get("/api/stations/s1/schedule-events", () =>
        HttpResponse.json([
          {
            id: "e1",
            station_id: "s1",
            title: null,
            start_date: "2026-01-01",
            start_time: "09:00",
            end_time: "10:00",
            source_type: "playlist",
            playlist_id: "p1",
            playlist_name: "Morning Mix",
            auto_dj_mode: null,
            auto_dj_avoid_repeat: null,
            auto_dj_min_gap: null,
            auto_dj_songs_ahead: null,
            recurrence_type: "none",
            recurrence_interval: null,
            recurrence_days: null,
            recurrence_end_date: null,
            recurrence_count: null,
            created_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
    );
    const { result } = renderHook(() => useScheduleEvents("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toHaveLength(1));
    expect(result.current.data?.[0].playlist_name).toBe("Morning Mix");
  });
});
