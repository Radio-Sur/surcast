import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import { useDeleteSong, useSong, useSongs, useUpdateSong, useUploadSong, useUploadZip } from "@/hooks/use-songs";
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

describe("useSongs", () => {
  it("returns songs list", async () => {
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "s1",
            title: "Song One",
            artist: "Artist",
            album: "Album",
            duration: 200,
            has_cover: false,
            mime_type: "audio/mpeg",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
          },
        ]),
      ),
    );
    const { result } = renderHook(() => useSongs(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(1);
  });
});

describe("useSong", () => {
  it("returns a single song", async () => {
    server.use(
      http.get("/api/songs/s1", () =>
        HttpResponse.json({
          id: "s1",
          title: "Song One",
          artist: "Artist",
          album: "Album",
          duration: 200,
          has_cover: false,
          mime_type: "audio/mpeg",
          created_at: "2026-01-01T00:00:00Z",
          updated_at: "2026-01-01T00:00:00Z",
        }),
      ),
    );
    const { result } = renderHook(() => useSong("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.title).toBe("Song One");
  });

  it("is not enabled when id is empty", () => {
    const { result } = renderHook(() => useSong(""), { wrapper: createWrapper() });
    expect(result.current.fetchStatus).toBe("idle");
  });
});

describe("useDeleteSong", () => {
  it("mutates successfully", async () => {
    server.use(http.delete("/api/songs/s1", () => HttpResponse.json({})));
    const { result } = renderHook(() => useDeleteSong(), { wrapper: createWrapper() });
    result.current.mutate("s1");
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useUploadSong", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/songs", () => HttpResponse.json({ id: "new", title: "Uploaded" })));
    const { result } = renderHook(() => useUploadSong(), { wrapper: createWrapper() });
    result.current.mutate({ file: new File([], "test.mp3"), title: "New Song" });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useUploadZip", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/songs/zip", () => HttpResponse.json([{ id: "s1" }])));
    const { result } = renderHook(() => useUploadZip(), { wrapper: createWrapper() });
    result.current.mutate({ file: new File([], "songs.zip") });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useUpdateSong", () => {
  it("mutates successfully", async () => {
    server.use(http.put("/api/songs/s1", () => HttpResponse.json({ id: "s1", title: "Updated" })));
    const { result } = renderHook(() => useUpdateSong(), { wrapper: createWrapper() });
    result.current.mutate({ id: "s1", data: { title: "Updated" } });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});
