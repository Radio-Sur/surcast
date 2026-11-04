import { HttpResponse, http } from "msw";
import { beforeEach, expect, it, vi } from "vitest";
import { ConfigureSandboxDialog } from "@/components/configure-sandbox-dialog";
import { server } from "@/mocks/server";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  setupAuth();
  server.use(
    http.get("/api/stations", () => HttpResponse.json([{ id: "source", name: "Source station" }])),
    http.get("/api/playlists", () =>
      HttpResponse.json([
        { id: "playlist", name: "Source playlist" },
        { id: "second", name: "Second playlist" },
        { id: "other", name: "Other music" },
      ]),
    ),
    http.get("/api/users/demo/sandbox", () => HttpResponse.json({ ready: true, station: "Demo station", songs: 3 })),
  );
});

it("assigns a shared station and searches and selects multiple playlists without checkboxes", async () => {
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
  const picker = screen.getByRole("combobox", { name: "Playlists to copy" });
  await user.type(picker, "Source");
  expect(screen.queryByRole("option", { name: "Other music" })).not.toBeInTheDocument();
  await user.click(await screen.findByRole("option", { name: "Source playlist" }));
  await user.type(picker, "Second");
  await user.click(await screen.findByRole("option", { name: "Second playlist" }));
  expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  await user.keyboard("{Escape}");
  await user.click(screen.getByRole("button", { name: "Prepare sandbox" }));
  await waitFor(() =>
    expect(submitted).toHaveBeenCalledWith({ station_id: "source", playlist_ids: ["playlist", "second"] }),
  );
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
