import { ThemeProvider } from "@emotion/react";
import { createTheme } from "@mui/material";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { I18nextProvider } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { usePlaylists } from "@/hooks/use-playlists";
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

describe("usePlaylists", () => {
  it("returns empty array when no playlists exist", async () => {
    server.use(http.get("/api/playlists", () => HttpResponse.json([])));
    const { result } = renderHook(() => usePlaylists(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toEqual([]));
  });

  it("returns playlists list", async () => {
    server.use(
      http.get("/api/playlists", () =>
        HttpResponse.json([
          {
            id: "1",
            name: "My Playlist",
            description: "",
            song_count: 3,
            total_duration_seconds: 600,
            created_by: "1",
            created_at: "",
            updated_at: "",
          },
        ]),
      ),
    );
    const { result } = renderHook(() => usePlaylists(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toHaveLength(1));
    expect(result.current.data?.[0].name).toBe("My Playlist");
  });
});
