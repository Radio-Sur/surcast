import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import { useContext } from "react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { AuthContext, AuthProvider } from "@/providers/auth-provider";
import { setupAuth } from "@/test/test-utils";

function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
}

beforeEach(() => {
  server.resetHandlers();
});

afterEach(() => {
  localStorage.clear();
});

function TestConsumer() {
  const ctx = useContext(AuthContext);
  if (!ctx) return <div>No context</div>;
  return (
    <div>
      <div data-testid="isLoading">{String(ctx.isLoading)}</div>
      <div data-testid="isAuthenticated">{String(ctx.isAuthenticated)}</div>
      <div data-testid="setupComplete">{String(ctx.setupComplete)}</div>
      <div data-testid="token">{ctx.token ?? "null"}</div>
      <div data-testid="user">{ctx.user ? ctx.user.username : "null"}</div>
      <button type="button" data-testid="login" onClick={() => ctx.login("admin", "pass")}>
        Login
      </button>
      <button type="button" data-testid="logout" onClick={() => ctx.logout()}>
        Logout
      </button>
      <button type="button" data-testid="refreshStatus" onClick={() => ctx.refreshSetupStatus()}>
        Refresh
      </button>
    </div>
  );
}

function renderProvider(handlers?: Parameters<typeof server.use>[0][]) {
  if (handlers) server.use(...handlers);
  const queryClient = createQueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <TestConsumer />
      </AuthProvider>
    </QueryClientProvider>,
  );
}

describe("AuthProvider", () => {
  it("provides default context to children", () => {
    render(
      <QueryClientProvider client={createQueryClient()}>
        <AuthProvider>
          <div>child</div>
        </AuthProvider>
      </QueryClientProvider>,
    );
    expect(screen.getByText("child")).toBeInTheDocument();
  });

  it("fetches setup status and user on mount when token exists", async () => {
    setupAuth();
    renderProvider();
    await waitFor(() => expect(screen.getByTestId("isLoading")).toHaveTextContent("false"));
    expect(screen.getByTestId("setupComplete")).toHaveTextContent("true");
    expect(screen.getByTestId("isAuthenticated")).toHaveTextContent("true");
    expect(screen.getByTestId("user")).toHaveTextContent("admin");
    expect(screen.getByTestId("token")).not.toHaveTextContent("null");
  });

  it("shows setupComplete=false when setup is not complete", async () => {
    setupAuth();
    server.use(http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: false })));
    renderProvider();
    await waitFor(() => expect(screen.getByTestId("isLoading")).toHaveTextContent("false"));
    expect(screen.getByTestId("setupComplete")).toHaveTextContent("false");
  });

  it("shows loading state initially before API calls complete", () => {
    server.use(http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: true })));
    renderProvider();
    expect(screen.getByTestId("isLoading")).toHaveTextContent("true");
  });

  it("login stores tokens and sets user", async () => {
    server.use(
      http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: true })),
      http.get("/api/auth/me", () => HttpResponse.json(null, { status: 401 })),
      http.post("/api/auth/login", () =>
        HttpResponse.json({
          access_token: "login-access",
          refresh_token: "login-refresh",
          user: { id: "2", username: "jdoe", name: "John", role: "viewer", created_at: "2026-01-01T00:00:00Z" },
        }),
      ),
    );
    renderProvider();
    await waitFor(() => expect(screen.getByTestId("isLoading")).toHaveTextContent("false"));
    expect(screen.getByTestId("isAuthenticated")).toHaveTextContent("false");
    screen.getByTestId("login").click();
    await waitFor(() => expect(screen.getByTestId("isAuthenticated")).toHaveTextContent("true"));
    expect(screen.getByTestId("user")).toHaveTextContent("jdoe");
    expect(localStorage.getItem("access_token")).toBe("login-access");
    expect(localStorage.getItem("refresh_token")).toBe("login-refresh");
  });

  it("logout clears tokens and user", async () => {
    setupAuth();
    renderProvider();
    await waitFor(() => expect(screen.getByTestId("isAuthenticated")).toHaveTextContent("true"));
    screen.getByTestId("logout").click();
    await waitFor(() => expect(screen.getByTestId("isAuthenticated")).toHaveTextContent("false"));
    expect(screen.getByTestId("user")).toHaveTextContent("null");
    expect(localStorage.getItem("access_token")).toBeNull();
    expect(localStorage.getItem("refresh_token")).toBeNull();
  });

  it("refreshSetupStatus updates setup status", async () => {
    let status = false;
    server.use(http.get("/api/setup/status", () => HttpResponse.json({ setup_complete: status })));
    renderProvider();
    await waitFor(() => expect(screen.getByTestId("setupComplete")).toHaveTextContent("false"));
    status = true;
    screen.getByTestId("refreshStatus").click();
    await waitFor(() => expect(screen.getByTestId("setupComplete")).toHaveTextContent("true"));
  });
});
