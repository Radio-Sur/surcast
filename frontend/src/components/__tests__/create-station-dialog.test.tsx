import { fireEvent } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { CreateStationDialog } from "@/components/create-station-dialog";
import { server } from "@/mocks/server";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("CreateStationDialog", () => {
  it("renders form with all fields", () => {
    render(<CreateStationDialog open={true} onClose={() => {}} />);
    expect(screen.getAllByText("Create Station")).toHaveLength(2);
    expect(screen.getByLabelText(/station name/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/description/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/mount point/i)).toBeInTheDocument();
  });

  it("shows create and cancel buttons", () => {
    render(<CreateStationDialog open={true} onClose={() => {}} />);
    expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /create station/i })).toBeInTheDocument();
  });

  it("does not render when closed", () => {
    render(<CreateStationDialog open={false} onClose={() => {}} />);
    expect(screen.queryByText("Create Station")).not.toBeInTheDocument();
  });

  it("submits form on success", async () => {
    let postCalled = false;
    server.use(
      http.post("/api/stations", () => {
        postCalled = true;
        return HttpResponse.json(
          {
            id: "new-1",
            name: "My Station",
            description: "",
            slug: "my-station",
            stream_url: "main",
            current_song_index: 0,
            prebuffer_bytes: 0,
            played_limit: 100,
            default_fade_ms: 2000,
            created_by: "1",
            created_at: "2026-01-01T00:00:00Z",
            updated_at: "2026-01-01T00:00:00Z",
          },
          { status: 201 },
        );
      }),
    );
    const onClose = vi.fn();
    render(<CreateStationDialog open={true} onClose={onClose} />);
    const user = userEvent.setup();
    await user.type(screen.getByLabelText(/station name/i), "My Station");
    await user.type(screen.getByLabelText(/mount point/i), "live");
    fireEvent.click(screen.getByRole("button", { name: /create station/i }));
    await waitFor(() => expect(postCalled).toBe(true), { timeout: 5000 });
    await waitFor(() => expect(onClose).toHaveBeenCalled(), { timeout: 5000 });
  });

  it("shows error message on submission failure", async () => {
    server.use(http.post("/api/stations", () => HttpResponse.json({ error: "Name already taken" }, { status: 400 })));
    render(<CreateStationDialog open={true} onClose={() => {}} />);
    const user = userEvent.setup();
    await user.type(screen.getByLabelText(/station name/i), "Duplicate");
    await user.type(screen.getByLabelText(/mount point/i), "live");
    fireEvent.click(screen.getByRole("button", { name: /create station/i }));
    await waitFor(() => expect(screen.getByRole("alert")).toBeInTheDocument(), { timeout: 5000 });
  });
});
