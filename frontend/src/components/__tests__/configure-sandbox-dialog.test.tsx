import { HttpResponse, http } from "msw";
import { beforeEach, expect, it, vi } from "vitest";
import { ConfigureSandboxDialog } from "@/components/configure-sandbox-dialog";
import { server } from "@/mocks/server";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  setupAuth();
  server.use(
    http.get("/api/stations", () => HttpResponse.json([{ id: "source", name: "Source station" }])),
    http.get("/api/playlists", () => HttpResponse.json([{ id: "playlist", name: "Source playlist" }])),
    http.get("/api/users/demo/sandbox", () => HttpResponse.json({ ready: true, station: "Demo station", songs: 3 })),
  );
});

it("prepares independent copies from the selected station and playlists", async () => {
  const submitted = vi.fn();
  server.use(
    http.post("/api/users/demo/sandbox", async ({ request }) => {
      submitted(await request.json());
      return HttpResponse.json({ ready: true });
    }),
  );
  render(<ConfigureSandboxDialog userId="demo" onClose={() => {}} />);
  const user = userEvent.setup();
  await screen.findByRole("option", { name: "Source station" });
  await user.selectOptions(screen.getByLabelText("Source station"), "source");
  await user.click(await screen.findByLabelText("Source playlist"));
  await user.click(screen.getByRole("button", { name: "Prepare sandbox" }));
  await waitFor(() => expect(submitted).toHaveBeenCalledWith({ station_id: "source", playlist_ids: ["playlist"] }));
});

it("requires confirmation before resetting guest changes", async () => {
  const reset = vi.fn();
  server.use(
    http.post("/api/users/demo/sandbox/reset", () => {
      reset();
      return HttpResponse.json({ ready: true });
    }),
  );
  render(<ConfigureSandboxDialog userId="demo" onClose={() => {}} />);
  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: "Reset demo state" }));
  expect(reset).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Confirm reset" }));
  await waitFor(() => expect(reset).toHaveBeenCalledOnce());
});
