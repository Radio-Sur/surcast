import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import { useStreamPause, useStreamPlay, useStreamRestart, useStreamSkip, useStreamStop } from "@/hooks/use-stream";
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

describe("useStreamSkip", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/stream/skip", () => HttpResponse.json({})));
    const { result } = renderHook(() => useStreamSkip("s1"), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useStreamStop", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/stream/stop", () => HttpResponse.json({})));
    const { result } = renderHook(() => useStreamStop("s1"), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useStreamPlay", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/stream/play", () => HttpResponse.json({})));
    const { result } = renderHook(() => useStreamPlay("s1"), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useStreamPause", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/stream/pause", () => HttpResponse.json({})));
    const { result } = renderHook(() => useStreamPause("s1"), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useStreamRestart", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/stations/s1/stream/restart", () => HttpResponse.json({})));
    const { result } = renderHook(() => useStreamRestart("s1"), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});
