import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { EditStationDialog } from "@/components/edit-station-dialog";
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
  transition_mode: "crossfade" as const,
  autocue_fade_max_ms: 6000,
  created_by: "1",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

describe("Edit Station Dialog", () => {
  it("renders the station data and saves changes", async () => {
    const onClose = vi.fn();
    server.use(
      http.put("/api/stations/s1", async ({ request }) => {
        const body = (await request.json()) as { name: string };
        return HttpResponse.json({ ...mockStation, name: body.name });
      }),
    );
    render(<EditStationDialog open station={mockStation} onClose={onClose} />);

    expect(screen.getByRole("heading", { name: /edit station/i })).toBeInTheDocument();
    expect(screen.getByDisplayValue("Pop Radio")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Top 40 hits")).toBeInTheDocument();
    expect(screen.getByDisplayValue("main")).toBeInTheDocument();

    const user = userEvent.setup();
    await user.clear(screen.getByLabelText(/station name/i));
    await user.type(screen.getByLabelText(/station name/i), "Updated Radio");
    await user.click(screen.getByRole("button", { name: /save changes/i }));
    await waitFor(() => expect(onClose).toHaveBeenCalled());
  });

  it("shows a localized error on save failure", async () => {
    server.use(
      http.put("/api/stations/s1", () =>
        HttpResponse.json({ code: "BAD_REQUEST", error: "Update failed" }, { status: 400 }),
      ),
    );
    render(<EditStationDialog open station={mockStation} onClose={() => {}} />);

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /save changes/i }));
    expect(await screen.findByText("Invalid request. Check the entered data.")).toBeInTheDocument();
  });

  it("disables station identity editing for demo accounts", async () => {
    server.use(
      http.get("/api/auth/me", () =>
        HttpResponse.json({ id: "demo", username: "demo", role: "demo", created_at: "2026-01-01T00:00:00Z" }),
      ),
    );
    render(<EditStationDialog open station={mockStation} onClose={() => {}} />);

    await screen.findByText(/station name, description and mountpoint cannot be changed/i);
    expect(screen.getByDisplayValue("Pop Radio")).toBeDisabled();
    expect(screen.getByDisplayValue("Top 40 hits")).toBeDisabled();
    expect(screen.getByDisplayValue("main")).toBeDisabled();
    expect(screen.getByRole("button", { name: /save changes/i })).toBeDisabled();
  });
});

describe("Stations list edit action", () => {
  it("opens the edit dialog instead of navigating to a separate page", async () => {
    server.use(http.get("/api/stations", () => HttpResponse.json([mockStation])));
    render(<StationsListPage />, { route: "/stations" });

    await screen.findByText("Pop Radio");
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /edit/i }));

    expect(await screen.findByRole("dialog")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Pop Radio")).toBeInTheDocument();
  });
});
