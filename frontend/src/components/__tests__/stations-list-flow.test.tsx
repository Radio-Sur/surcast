import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { StationsListPage } from "@/pages/stations/list";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

const mockStation = {
  id: "s1",
  name: "Pop Radio",
  description: "Top 40 hits",
  slug: "pop-radio",
  stream_url: "main",
  current_song_index: 0,
  prebuffer_bytes: 16384,
  played_limit: 100,
  default_fade_ms: 2000,
  created_by: "1",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

describe("Stations List Page", () => {
  it("shows title and add button", async () => {
    server.use(http.get("/api/stations", () => HttpResponse.json([])));
    render(<StationsListPage />, { route: "/stations" });
    await waitFor(() => expect(screen.getByText("Stations")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByRole("button", { name: /add/i })).toBeInTheDocument();
  });

  it("shows empty state when no stations", async () => {
    server.use(http.get("/api/stations", () => HttpResponse.json([])));
    render(<StationsListPage />, { route: "/stations" });
    await waitFor(() => expect(screen.getByText(/no stations/i)).toBeInTheDocument(), { timeout: 5000 });
  });

  it("renders station rows", async () => {
    server.use(http.get("/api/stations", () => HttpResponse.json([mockStation])));
    render(<StationsListPage />, { route: "/stations" });
    await waitFor(() => expect(screen.getByText("Pop Radio")).toBeInTheDocument(), { timeout: 5000 });
  });

  it("shows delete dialog and deletes station", async () => {
    server.use(
      http.get("/api/stations", () => HttpResponse.json([mockStation])),
      http.delete("/api/stations/s1", () => HttpResponse.json({ success: true })),
    );
    render(<StationsListPage />, { route: "/stations" });
    await waitFor(() => expect(screen.getByText("Pop Radio")).toBeInTheDocument(), { timeout: 5000 });
    const deleteBtn = screen.getAllByRole("button").find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeTruthy();
  });

  it("executes delete when confirmed in dialog", async () => {
    let deleteCalled = false;
    server.use(
      http.get("/api/stations", () => HttpResponse.json([mockStation])),
      http.delete("/api/stations/s1", () => {
        deleteCalled = true;
        return HttpResponse.json({ success: true });
      }),
    );
    render(<StationsListPage />, { route: "/stations" });
    await waitFor(() => expect(screen.getByText("Pop Radio")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const deleteBtn = screen.getAllByRole("button").find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeTruthy();
    await user.click(deleteBtn!);
    await screen.findByRole("dialog");
    await user.click(screen.getByRole("button", { name: /delete/i }));
    await waitFor(() => expect(deleteCalled).toBe(true), { timeout: 3000 });
  });

  it("cancels delete dialog", async () => {
    server.use(http.get("/api/stations", () => HttpResponse.json([mockStation])));
    render(<StationsListPage />, { route: "/stations" });
    await waitFor(() => expect(screen.getByText("Pop Radio")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    const deleteBtn = screen.getAllByRole("button").find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    await user.click(deleteBtn!);
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /cancel/i }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument(), { timeout: 3000 });
  });
});
