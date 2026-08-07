import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { RenderOptions } from "@testing-library/react";
import { render as rtlRender, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { ReactElement, ReactNode } from "react";
import { I18nextProvider } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import i18n from "@/i18n";
import { server } from "@/mocks/server";
import { AuthProvider } from "@/providers/auth-provider";
import { LiveProvider } from "@/providers/live-provider";
import { SnackbarProvider } from "@/providers/snackbar-provider";
import { ThemeProvider } from "@/providers/theme-provider";

export function setupAuth() {
  localStorage.setItem("access_token", "mock-token");
  localStorage.setItem("refresh_token", "mock-refresh");
  server.use(
    http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: true })),
    http.get("/api/auth/me", () =>
      HttpResponse.json({
        id: "1",
        username: "admin",
        name: "Admin",
        role: "admin",
        created_at: "2026-01-01T00:00:00Z",
      }),
    ),
  );
}

export function createTestQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
}

interface WrapperProps {
  children: ReactNode;
}

export interface CustomRenderOptions extends Omit<RenderOptions, "wrapper"> {
  route?: string;
}

function createWrapper(route = "/") {
  const queryClient = createTestQueryClient();

  function Wrapper({ children }: WrapperProps) {
    return (
      <QueryClientProvider client={queryClient}>
        <I18nextProvider i18n={i18n}>
          <ThemeProvider>
            <MemoryRouter initialEntries={[route]}>
              <AuthProvider>
                <LiveProvider>
                  <SnackbarProvider>{children}</SnackbarProvider>
                </LiveProvider>
              </AuthProvider>
            </MemoryRouter>
          </ThemeProvider>
        </I18nextProvider>
      </QueryClientProvider>
    );
  }

  return Wrapper;
}

function customRender(ui: ReactElement, options?: CustomRenderOptions) {
  const { route, ...renderOptions } = options || {};
  return rtlRender(ui, { wrapper: createWrapper(route), ...renderOptions });
}

export { customRender as render, rtlRender, screen, userEvent, waitFor, within };
