import { HttpResponse, http } from "msw";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/mocks/server";
import { EditStationPage } from "@/pages/stations/edit";
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

describe("Edit Station Page", () => {
  it("shows not found when station does not exist", async () => {
    server.use(http.get("/api/stations/unknown", () => HttpResponse.json(null, { status: 404 })));
    render(
      <Routes>
        <Route path="/stations/:id/edit" element={<EditStationPage />} />
      </Routes>,
      { route: "/stations/unknown/edit" },
    );
    await waitFor(() => expect(screen.getByText("Station not found.")).toBeInTheDocument(), { timeout: 5000 });
  });

  it("renders edit form with station data", async () => {
    server.use(
      http.get("/api/stations/s1", () => HttpResponse.json(mockStation)),
      http.put("/api/stations/s1", () => HttpResponse.json(mockStation)),
    );
    render(
      <Routes>
        <Route path="/stations/:id/edit" element={<EditStationPage />} />
      </Routes>,
      { route: "/stations/s1/edit" },
    );
    await waitFor(() => expect(screen.getByDisplayValue("Pop Radio")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByDisplayValue("Top 40 hits")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /save/i })).toBeInTheDocument();
  });

  it("saves changes and navigates on success", async () => {
    server.use(
      http.get("/api/stations/s1", () => HttpResponse.json(mockStation)),
      http.put("/api/stations/s1", () => HttpResponse.json({ ...mockStation, name: "Updated Radio" })),
    );
    render(
      <Routes>
        <Route path="/stations/:id/edit" element={<EditStationPage />} />
      </Routes>,
      { route: "/stations/s1/edit" },
    );
    await waitFor(() => expect(screen.getByDisplayValue("Pop Radio")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    await user.clear(screen.getByLabelText(/station name/i));
    await user.type(screen.getByLabelText(/station name/i), "Updated Radio");
    await user.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(screen.queryByText(/save/i)).not.toBeInTheDocument(), { timeout: 3000 });
  });

  it("shows error on save failure", async () => {
    server.use(
      http.get("/api/stations/s1", () => HttpResponse.json(mockStation)),
      http.put("/api/stations/s1", () => HttpResponse.json({ error: "Update failed" }, { status: 400 })),
    );
    render(
      <Routes>
        <Route path="/stations/:id/edit" element={<EditStationPage />} />
      </Routes>,
      { route: "/stations/s1/edit" },
    );
    await waitFor(() => expect(screen.getByDisplayValue("Pop Radio")).toBeInTheDocument(), { timeout: 5000 });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(screen.getByRole("alert")).toBeInTheDocument(), { timeout: 5000 });
  });
});
