import { ThemeProvider } from "@emotion/react";
import { createTheme } from "@mui/material";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { I18nextProvider } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { useApiKeys } from "@/hooks/use-api-keys";
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

describe("useApiKeys", () => {
  it("returns empty array when no keys exist", async () => {
    server.use(http.get("/api/api-keys", () => HttpResponse.json([])));
    const { result } = renderHook(() => useApiKeys(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toEqual([]));
  });

  it("returns api keys list", async () => {
    server.use(
      http.get("/api/api-keys", () =>
        HttpResponse.json([
          {
            id: "1",
            name: "My Key",
            key_prefix: "sk_test",
            is_active: true,
            last_used_at: null,
            expires_at: null,
            created_at: "",
          },
        ]),
      ),
    );
    const { result } = renderHook(() => useApiKeys(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toHaveLength(1));
    expect(result.current.data?.[0].name).toBe("My Key");
  });
});
