import { describe, expect, it, vi } from "vitest";
import { EditSongDialog } from "@/components/edit-song-dialog";
import { render, screen, userEvent } from "@/test/test-utils";

describe("EditSongDialog", () => {
  const song = { id: "1", title: "Test Song", artist: "Test Artist", album: "Test Album" };

  it("renders when song is provided", () => {
    render(<EditSongDialog song={song} isPending={false} onSave={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByRole("heading", { name: /edit song/i })).toBeInTheDocument();
  });

  it("does not render when song is null", () => {
    render(<EditSongDialog song={null} isPending={false} onSave={vi.fn()} onClose={vi.fn()} />);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("displays song data in fields", () => {
    render(<EditSongDialog song={song} isPending={false} onSave={vi.fn()} onClose={vi.fn()} />);
    expect(screen.getByDisplayValue("Test Song")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Test Artist")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Test Album")).toBeInTheDocument();
  });

  it("calls onSave with updated data", async () => {
    const onSave = vi.fn();
    const user = userEvent.setup();
    render(<EditSongDialog song={song} isPending={false} onSave={onSave} onClose={vi.fn()} />);
    await user.click(screen.getByRole("button", { name: /save/i }));
    expect(onSave).toHaveBeenCalledWith(song);
  });
});
