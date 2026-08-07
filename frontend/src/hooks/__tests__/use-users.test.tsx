import { ThemeProvider } from "@emotion/react";
import { createTheme } from "@mui/material";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { I18nextProvider } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { useUsers } from "@/hooks/use-users";
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

describe("useUsers", () => {
  it("returns empty array when no users exist", async () => {
    server.use(http.get("/api/users", () => HttpResponse.json([])));
    const { result } = renderHook(() => useUsers(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toEqual([]));
  });

  it("returns users list", async () => {
    server.use(
      http.get("/api/users", () =>
        HttpResponse.json([
          { id: "1", username: "admin", name: "Admin User", role: "admin", created_at: "2026-01-01T00:00:00Z" },
        ]),
      ),
    );
    const { result } = renderHook(() => useUsers(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toHaveLength(1));
    expect(result.current.data?.[0].username).toBe("admin");
  });
});
