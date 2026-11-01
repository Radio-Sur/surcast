import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { resetIds } from "@/mocks/data";
import { resetFiles, seedFiles } from "@/mocks/handlers/files";
import { server } from "@/mocks/server";
import { FilesPage } from "@/pages/files";
import { render, screen, setupAuth, userEvent, waitFor, within } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  resetIds();
  resetFiles();
  seedFiles();
  setupAuth();
});

describe("Files Page", () => {
  it("renders folders and files at the library root", async () => {
    render(<FilesPage />, { route: "/files" });
    await waitFor(() => expect(screen.getByText("rock")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText("intro.mp3")).toBeInTheDocument();
    expect(screen.getByText("Library")).toBeInTheDocument();
  });

  it("navigates into a folder via breadcrumbs", async () => {
    const user = userEvent.setup();
    render(<FilesPage />, { route: "/files" });
    await waitFor(() => expect(screen.getByText("rock")).toBeInTheDocument(), { timeout: 5000 });
    await user.click(screen.getByText("rock"));
    await waitFor(() => expect(screen.getByText("hit.mp3")).toBeInTheDocument(), { timeout: 5000 });
    expect(screen.getByText("2020")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Library" }));
    await waitFor(() => expect(screen.getByText("intro.mp3")).toBeInTheDocument(), { timeout: 5000 });
  });

  it("creates a new folder", async () => {
    const user = userEvent.setup();
    render(<FilesPage />, { route: "/files" });
    await waitFor(() => expect(screen.getByText("rock")).toBeInTheDocument(), { timeout: 5000 });
    await user.click(screen.getByRole("button", { name: /new folder/i }));
    const dialog = await screen.findByRole("dialog");
    await user.type(within(dialog).getByLabelText(/folder name/i), "jazz");
    await user.click(within(dialog).getByRole("button", { name: /create/i }));
    await waitFor(() => expect(screen.getByText("jazz")).toBeInTheDocument(), { timeout: 5000 });
  });

  it("deletes a file after confirmation", async () => {
    let deletedPath: string | null = null;
    server.use(
      http.delete("/api/files", ({ request }) => {
        deletedPath = new URL(request.url).searchParams.get("path");
        return HttpResponse.json({ deleted: 1 });
      }),
    );
    const user = userEvent.setup();
    render(<FilesPage />, { route: "/files" });
    await waitFor(() => expect(screen.getByText("intro.mp3")).toBeInTheDocument(), { timeout: 5000 });
    const row = screen.getByText("intro.mp3").closest("tr");
    await user.click(within(row as HTMLElement).getByTestId("MoreVertIcon"));
    await user.click(await screen.findByText("Delete"));
    const dialog = await screen.findByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: /delete/i }));
    await waitFor(() => expect(deletedPath).toBe("intro.mp3"), { timeout: 5000 });
  });

  it("renders distinct cover thumbnails for a folder", async () => {
    server.use(
      http.get("/api/files", () =>
        HttpResponse.json({
          path: "",
          parent: null,
          folders: [{ name: "rock", path: "rock", covers: ["c1", "c2"] }],
          files: [],
        }),
      ),
    );
    render(<FilesPage />, { route: "/files" });
    await waitFor(() => expect(screen.getByText("rock")).toBeInTheDocument(), { timeout: 5000 });
    expect(document.querySelectorAll('img[src="/api/songs/c1/cover"]')).toHaveLength(1);
    expect(document.querySelectorAll('img[src="/api/songs/c2/cover"]')).toHaveLength(1);
  });

  it("lists missing songs and purges them", async () => {
    let purged = false;
    server.use(
      http.get("/api/songs", () =>
        HttpResponse.json([
          {
            id: "m1",
            title: "Missing Song",
            artist: "Ghost",
            album: "",
            duration: 100,
            file_size: 10,
            mime_type: "audio/mpeg",
            has_cover: false,
            uploaded_by: null,
            origin: "directory",
            file_path: "gone/missing.mp3",
            missing_since: "2026-02-01T00:00:00Z",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
            station_ids: [],
          },
        ]),
      ),
      http.delete("/api/songs/missing", () => {
        purged = true;
        return HttpResponse.json({ deleted: 1 });
      }),
    );
    const user = userEvent.setup();
    render(<FilesPage />, { route: "/files" });
    await waitFor(() => expect(screen.getByText("rock")).toBeInTheDocument(), { timeout: 5000 });
    await user.click(screen.getByRole("button", { name: /show missing/i }));
    await waitFor(() => expect(screen.getByText("Missing Song")).toBeInTheDocument(), { timeout: 5000 });
    await user.click(screen.getByRole("button", { name: /delete all missing/i }));
    const dialog = await screen.findByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: /delete/i }));
    await waitFor(() => expect(purged).toBe(true), { timeout: 5000 });
  });
});
