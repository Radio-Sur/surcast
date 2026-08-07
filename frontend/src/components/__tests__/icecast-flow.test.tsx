import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { AdminIcecastPage } from "@/pages/admin/icecast";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

const defaultSettings = {
  settings: {
    enabled: false,
    mode: "managed",
    port: 8000,
    source_password: "hackme",
    admin_user: "admin",
    admin_password: "admin",
    external_url: null,
    external_source_pw: null,
    external_admin_pw: null,
  },
  running: false,
};

describe("Admin Icecast Page", () => {
  it("renders icecast settings form and shows stopped status", async () => {
    server.use(http.get("/api/admin/icecast", () => HttpResponse.json(defaultSettings)));
    render(<AdminIcecastPage />, { route: "/admin/icecast" });
    await waitFor(() => expect(screen.getByText(/stopped/i)).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByRole("button", { name: /start/i })).toBeInTheDocument();
  });

  it("shows running status when icecast is running", async () => {
    server.use(http.get("/api/admin/icecast", () => HttpResponse.json({ ...defaultSettings, running: true })));
    render(<AdminIcecastPage />, { route: "/admin/icecast" });
    await waitFor(() => expect(screen.getByText(/running/i)).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByRole("button", { name: /stop/i })).toBeInTheDocument();
  });

  it("switches to external mode", async () => {
    server.use(http.get("/api/admin/icecast", () => HttpResponse.json(defaultSettings)));
    render(<AdminIcecastPage />, { route: "/admin/icecast" });
    await waitFor(() => expect(screen.getByText(/stopped/i)).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    await user.click(screen.getByText(/managed/i));
    await user.click(screen.getByText(/external/i));
    expect(screen.getByText(/test connection/i)).toBeInTheDocument();
  });

  it("shows save button", async () => {
    server.use(http.get("/api/admin/icecast", () => HttpResponse.json(defaultSettings)));
    render(<AdminIcecastPage />, { route: "/admin/icecast" });
    await waitFor(() => expect(screen.getByText(/stopped/i)).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByRole("button", { name: /save/i })).toBeInTheDocument();
  });
});
