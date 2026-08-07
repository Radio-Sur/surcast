import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import {
  useIcecastStatus,
  useStartIcecast,
  useStopIcecast,
  useTestIcecast,
  useUpdateIcecast,
} from "@/hooks/use-icecast";
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

describe("useIcecastStatus", () => {
  it("returns icecast settings and status", async () => {
    server.use(
      http.get("/api/admin/icecast", () =>
        HttpResponse.json({
          settings: {
            id: "1",
            enabled: true,
            mode: "managed",
            port: 8000,
            source_password: "pass",
            admin_user: "admin",
            admin_password: "admin",
            external_url: null,
            external_source_pw: null,
            external_admin_pw: null,
          },
          running: true,
        }),
      ),
    );
    const { result } = renderHook(() => useIcecastStatus(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.running).toBe(true);
    expect(result.current.data?.settings.port).toBe(8000);
  });
});

describe("useUpdateIcecast", () => {
  it("mutates successfully", async () => {
    server.use(http.patch("/api/admin/icecast", () => HttpResponse.json({})));
    const { result } = renderHook(() => useUpdateIcecast(), { wrapper: createWrapper() });
    result.current.mutate({ port: 8001 });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useStartIcecast", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/admin/icecast/start", () => HttpResponse.json({})));
    const { result } = renderHook(() => useStartIcecast(), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useStopIcecast", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/admin/icecast/stop", () => HttpResponse.json({})));
    const { result } = renderHook(() => useStopIcecast(), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});

describe("useTestIcecast", () => {
  it("mutates successfully", async () => {
    server.use(http.post("/api/admin/icecast/test", () => HttpResponse.json({})));
    const { result } = renderHook(() => useTestIcecast(), { wrapper: createWrapper() });
    result.current.mutate();
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });
});
