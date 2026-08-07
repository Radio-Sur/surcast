import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import {
  useAddAutoFillPlaylist,
  useAutoFill,
  useDeleteAutoFillPlaylist,
  useTriggerAutoFill,
  useUpdateAutoFill,
  useUpdateAutoFillPlaylist,
} from "@/hooks/use-auto-fill";
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

describe("useAutoFill", () => {
  it("returns auto-fill config", async () => {
    server.use(
      http.get("/api/stations/s1/auto-fill", () =>
        HttpResponse.json({
          enabled: true,
          mode: "random",
          source_type: null,
          source_playlist_id: null,
          avoid_artist_repeat: true,
          min_song_gap: 5,
          songs_ahead: 10,
          entries: [],
        }),
      ),
    );
    const { result } = renderHook(() => useAutoFill("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.enabled).toBe(true);
  });

  it("is not enabled when stationId is empty", () => {
    const { result } = renderHook(() => useAutoFill(""), { wrapper: createWrapper() });
    expect(result.current.fetchStatus).toBe("idle");
  });
});

describe("useUpdateAutoFill", () => {
  it("mutates successfully", async () => {
    server.use(http.put("/api/stations/s1/auto-fill", () => HttpResponse.json({})));
    const { result } = renderHook(() => useUpdateAutoFill("s1"), { wrapper: createWrapper() });
    result.current.mutate({ enabled: true, mode: "random" });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useAddAutoFillPlaylist", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/auto-fill/playlists", () => HttpResponse.json({ id: "new" })));
    const { result } = renderHook(() => useAddAutoFillPlaylist("s1"), { wrapper: createWrapper() });
    result.current.mutate({ playlist_id: "pl1", weight: 50 });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useUpdateAutoFillPlaylist", () => {
  it("mutates successfully", async () => {
    server.use(http.put("/api/stations/s1/auto-fill/playlists/entry1", () => HttpResponse.json({})));
    const { result } = renderHook(() => useUpdateAutoFillPlaylist("s1"), { wrapper: createWrapper() });
    result.current.mutate({ id: "entry1", data: { weight: 75 } });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useDeleteAutoFillPlaylist", () => {
  it("mutates successfully", async () => {
    server.use(http.delete("/api/stations/s1/auto-fill/playlists/entry1", () => HttpResponse.json({})));
    const { result } = renderHook(() => useDeleteAutoFillPlaylist("s1"), { wrapper: createWrapper() });
    result.current.mutate("entry1");
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useTriggerAutoFill", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/auto-fill/trigger", () => HttpResponse.json({})));
    const { result } = renderHook(() => useTriggerAutoFill("s1"), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});
