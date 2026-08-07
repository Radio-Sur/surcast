import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import { useCreateSchedule, useDeleteSchedule, useSchedules, useUpdateSchedule } from "@/hooks/use-schedules";
import { server } from "@/mocks/server";
import { setupAuth } from "@/test/test-utils";

function createWrapper() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("useSchedules", () => {
  it("returns schedules for a station", async () => {
    server.use(
      http.get("/api/stations/s1/schedules", () =>
        HttpResponse.json([
          {
            id: "schedule1",
            station_id: "s1",
            day_of_week: 1,
            start_time: "09:00",
            end_time: "17:00",
            source_type: "playlist",
            playlist_id: "pl1",
            auto_dj_mode: null,
            auto_dj_avoid_repeat: null,
            auto_dj_min_gap: null,
            auto_dj_songs_ahead: null,
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
    );
    const { result } = renderHook(() => useSchedules("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(1);
  });

  it("is not enabled when stationId is empty", () => {
    const { result } = renderHook(() => useSchedules(""), { wrapper: createWrapper() });
    expect(result.current.fetchStatus).toBe("idle");
  });
});

describe("useCreateSchedule", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/schedules", () => HttpResponse.json({ id: "new" })));
    const { result } = renderHook(() => useCreateSchedule("s1"), { wrapper: createWrapper() });
    result.current.mutate({ day_of_week: 1, start_time: "09:00", end_time: "17:00" });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useUpdateSchedule", () => {
  it("mutates successfully", async () => {
    server.use(http.put("/api/stations/s1/schedules/s1", () => HttpResponse.json({})));
    const { result } = renderHook(() => useUpdateSchedule("s1"), { wrapper: createWrapper() });
    result.current.mutate({ id: "s1", data: { start_time: "10:00" } });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useDeleteSchedule", () => {
  it("mutates successfully", async () => {
    server.use(http.delete("/api/stations/s1/schedules/s1", () => HttpResponse.json({})));
    const { result } = renderHook(() => useDeleteSchedule("s1"), { wrapper: createWrapper() });
    result.current.mutate("s1");
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});
