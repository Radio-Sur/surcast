import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import {
  useAddToQueue,
  useInsertIntoQueue,
  useRemoveFromQueue,
  useRemovePlaylistFromQueue,
  useReorderQueue,
  useStationQueue,
} from "@/hooks/use-station-queue";
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

describe("useStationQueue", () => {
  it("returns queue items", async () => {
    server.use(
      http.get("/api/stations/s1/queue", () =>
        HttpResponse.json([
          {
            id: "q1",
            station_id: "s1",
            song_id: "s1",
            position: 0,
            title: "Test",
            artist: "A",
            album: "B",
            duration: 200,
            has_cover: false,
            mime_type: "audio/mpeg",
            origin_playlist_id: null,
            playlist_name: null,
            is_auto_dj: false,
          },
        ]),
      ),
    );
    const { result } = renderHook(() => useStationQueue("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(1);
  });

  it("is not enabled when stationId is empty", () => {
    const { result } = renderHook(() => useStationQueue(""), { wrapper: createWrapper() });
    expect(result.current.fetchStatus).toBe("idle");
  });
});

describe("useAddToQueue", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/queue", () => HttpResponse.json({})));
    const { result } = renderHook(() => useAddToQueue("s1"), { wrapper: createWrapper() });
    result.current.mutate(["song1"]);
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useRemoveFromQueue", () => {
  it("mutates successfully", async () => {
    server.use(http.delete("/api/stations/s1/queue/q1", () => HttpResponse.json({})));
    const { result } = renderHook(() => useRemoveFromQueue("s1"), { wrapper: createWrapper() });
    result.current.mutate("q1");
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useReorderQueue", () => {
  it("mutates successfully", async () => {
    server.use(http.put("/api/stations/s1/queue/reorder", () => HttpResponse.json({})));
    const { result } = renderHook(() => useReorderQueue("s1"), { wrapper: createWrapper() });
    result.current.mutate(["q1", "q2"]);
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useInsertIntoQueue", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/queue/insert", () => HttpResponse.json({})));
    const { result } = renderHook(() => useInsertIntoQueue("s1"), { wrapper: createWrapper() });
    result.current.mutate({ song_id: "s1", position: 0 });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useRemovePlaylistFromQueue", () => {
  it("mutates successfully", async () => {
    server.use(http.delete("/api/stations/s1/queue/playlist/pl1", () => HttpResponse.json({})));
    const { result } = renderHook(() => useRemovePlaylistFromQueue("s1"), { wrapper: createWrapper() });
    result.current.mutate("pl1");
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});
