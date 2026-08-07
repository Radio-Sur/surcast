import { ThemeProvider } from "@emotion/react";
import { createTheme } from "@mui/material";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { I18nextProvider } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { useStation, useStations } from "@/hooks/use-stations";
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

describe("useStations", () => {
  it("returns empty array when no stations exist", async () => {
    server.use(http.get("/api/stations", () => HttpResponse.json([])));
    const { result } = renderHook(() => useStations(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toEqual([]));
  });

  it("returns stations list", async () => {
    server.use(
      http.get("/api/stations", () =>
        HttpResponse.json([
          {
            id: "1",
            name: "Station 1",
            description: "",
            slug: "station-1",
            stream_url: "",
            current_song_index: 0,
            prebuffer_bytes: 0,
            played_limit: 100,
            default_fade_ms: 2000,
            created_by: "1",
            created_at: "",
            updated_at: "",
          },
        ]),
      ),
    );
    const { result } = renderHook(() => useStations(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toHaveLength(1));
    expect(result.current.data?.[0].name).toBe("Station 1");
  });
});

describe("useStation", () => {
  it("returns null when station not found", async () => {
    server.use(http.get("/api/stations/unknown", () => HttpResponse.json(null, { status: 404 })));
    const { result } = renderHook(() => useStation("unknown"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isError).toBe(true));
  });
});
