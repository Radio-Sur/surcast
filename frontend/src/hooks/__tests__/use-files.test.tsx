import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { useDirectory, useFolders, useMoveEntry } from "@/hooks/use-files";
import { setupAuth } from "@/test/test-utils";

function createClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
}

function createWrapper(queryClient = createClient()) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

beforeEach(() => {
  setupAuth();
});

describe("useDirectory", () => {
  it("loads a directory listing", async () => {
    const { result } = renderHook(() => useDirectory("", "name", "asc"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.path).toBe("");
    expect(result.current.data?.folders).toEqual(expect.any(Array));
  });
});

describe("useFolders", () => {
  it("loads all library folders", async () => {
    const { result } = renderHook(() => useFolders(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(expect.any(Array));
  });
});

describe("useMoveEntry", () => {
  it("invalidates the files and songs caches after moving", async () => {
    const queryClient = createClient();
    const invalidate = vi.spyOn(queryClient, "invalidateQueries");
    const { result } = renderHook(() => useMoveEntry(), { wrapper: createWrapper(queryClient) });
    result.current.mutate({ from: "rock/a.mp3", to: "pop/a.mp3" });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["files"] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["songs"] });
  });
});
