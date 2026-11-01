import { HttpResponse, http } from "msw";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fakeStation, fakeStreamStatus } from "@/mocks/data";
import { server } from "@/mocks/server";
import { StationDetailPage } from "@/pages/stations/detail";
import { render, screen, setupAuth, userEvent, waitFor, within } from "@/test/test-utils";

function status(playing: boolean) {
  return {
    ...fakeStreamStatus({ playing }),
    broadcasting: playing,
    source_connected: playing,
    is_started: playing,
    runtime_active: playing,
  };
}

beforeEach(() => {
  setupAuth();
  server.use(
    http.get("/api/stations/s1", () => HttpResponse.json(fakeStation({ id: "s1", name: "Test station" }))),
    http.get("/api/stations/s1/songs", () => HttpResponse.json({ songs: [], total: 0 })),
  );
});

function renderPage() {
  render(
    <Routes>
      <Route path="/stations/:id" element={<StationDetailPage />} />
    </Routes>,
    { route: "/stations/s1" },
  );
  return userEvent.setup();
}

describe("station stream controls", () => {
  it("keeps spinners and withholds notifications until confirmed; Stop → Start works repeatedly without a WebSocket update", async () => {
    let output = status(false);
    const play = vi.fn(() => HttpResponse.json({ ok: true }));
    const stop = vi.fn(() => HttpResponse.json({ ok: true }));
    const pause = vi.fn(() => HttpResponse.json({ ok: true }));
    server.use(
      http.post("/api/stations/s1/stream/play", play),
      http.post("/api/stations/s1/stream/stop", stop),
      http.post("/api/stations/s1/stream/pause", pause),
      http.get("/api/stations/s1/stream/status", () => HttpResponse.json(output)),
    );
    const user = renderPage();
    for (let cycle = 0; cycle < 2; cycle++) {
      await user.click(await screen.findByRole("button", { name: "Start" }));
      await waitFor(() => expect(play).toHaveBeenCalledTimes(cycle + 1));
      expect(screen.getByRole("button", { name: /Starting/ })).toBeDisabled();
      expect(screen.getByRole("progressbar")).toBeVisible();
      expect(screen.queryByText("Station started")).not.toBeInTheDocument();
      output = status(true);
      expect(await screen.findByText("Station started")).toBeVisible();
      expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Close" }));

      await user.click(screen.getByRole("button", { name: "Stop" }));
      const dialog = await screen.findByRole("dialog");
      await user.click(within(dialog).getByRole("button", { name: "Stop" }));
      await waitFor(() => expect(stop).toHaveBeenCalledTimes(cycle + 1));
      expect(within(dialog).getByRole("progressbar")).toBeVisible();
      expect(within(dialog).getByRole("button", { name: "Cancel" })).toBeDisabled();
      expect(screen.queryByText("Station stopped")).not.toBeInTheDocument();
      await user.keyboard("{Escape}");
      expect(dialog).toBeVisible();
      output = status(false);
      expect(await screen.findByText("Station stopped")).toBeVisible();
      await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
      expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Close" }));
    }
    expect(pause).not.toHaveBeenCalled();
  });

  it("shows a failed command as an error, clears the spinner and permits retry without a success notification", async () => {
    server.use(
      http.post("/api/stations/s1/stream/play", () => HttpResponse.json({ error: "Output failed" }, { status: 500 })),
    );
    const user = renderPage();
    await user.click(await screen.findByRole("button", { name: "Start" }));
    expect(await screen.findByText("Output failed")).toBeVisible();
    expect(screen.getByRole("button", { name: "Start" })).toBeEnabled();
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
    expect(screen.queryByText("Station started")).not.toBeInTheDocument();
  });

  it("keeps Restart pending until the new output is confirmed", async () => {
    let output = status(false);
    server.use(
      http.post("/api/stations/s1/stream/restart", () => HttpResponse.json({ ok: true })),
      http.get("/api/stations/s1/stream/status", () => HttpResponse.json(output)),
    );
    const user = renderPage();
    await user.click(await screen.findByRole("button", { name: "Restart" }));
    const dialog = await screen.findByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: "Restart" }));
    await waitFor(() => expect(within(dialog).getByRole("progressbar")).toBeVisible());
    expect(screen.queryByText("Station restarted")).not.toBeInTheDocument();
    output = status(true);
    expect(await screen.findByText("Station restarted")).toBeVisible();
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Stop" })).toBeEnabled();
  });
});
