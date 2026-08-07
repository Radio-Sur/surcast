import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import { useAddStationSongs, useRemoveStationSong, useStationSongs } from "@/hooks/use-station-library";
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

describe("useStationSongs", () => {
  it("returns library songs", async () => {
    server.use(
      http.get("/api/stations/s1/songs", () =>
        HttpResponse.json([
          {
            id: "ss1",
            station_id: "s1",
            song_id: "song1",
            title: "Song One",
            artist: "Artist",
            album: "Album",
            duration: 200,
            has_cover: false,
            mime_type: "audio/mpeg",
            added_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
    );
    const { result } = renderHook(() => useStationSongs("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(1);
  });

  it("is not enabled when stationId is empty", () => {
    const { result } = renderHook(() => useStationSongs(""), { wrapper: createWrapper() });
    expect(result.current.fetchStatus).toBe("idle");
  });
});

describe("useAddStationSongs", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/songs", () => HttpResponse.json({})));
    const { result } = renderHook(() => useAddStationSongs("s1"), { wrapper: createWrapper() });
    result.current.mutate({ songIds: ["song1", "song2"], artistNames: [], albumSelectors: [] });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useRemoveStationSong", () => {
  it("mutates successfully", async () => {
    server.use(http.delete("/api/stations/s1/songs/song1", () => HttpResponse.json({})));
    const { result } = renderHook(() => useRemoveStationSong("s1"), { wrapper: createWrapper() });
    result.current.mutate("song1");
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});
