import { DndContext } from "@dnd-kit/core";
import { SortableContext, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { describe, expect, it, vi } from "vitest";
import { QueueRow } from "@/components/queue/queue-row";
import { render, screen } from "@/test/test-utils";

function renderWithDnd(ui: React.ReactElement) {
  return render(
    <DndContext>
      <SortableContext items={["q1"]} strategy={verticalListSortingStrategy}>
        {ui}
      </SortableContext>
    </DndContext>,
  );
}

const mockSong = {
  id: "q1",
  station_id: "s1",
  song_id: "s1",
  position: 0,
  title: "Test Song",
  artist: "Test Artist",
  album: "Test Album",
  duration: 200,
  has_cover: false,
  mime_type: "audio/mpeg",
  origin_playlist_id: null,
  playlist_name: null,
  is_auto_dj: false,
};

describe("QueueRow", () => {
  it("renders song title and artist", () => {
    renderWithDnd(<QueueRow song={mockSong} index={0} onDelete={vi.fn()} onMoveToTop={vi.fn()} />);
    expect(screen.getByText("Test Song")).toBeInTheDocument();
    expect(screen.getByText(/Test Artist/)).toBeInTheDocument();
  });

  it("shows index", () => {
    renderWithDnd(<QueueRow song={mockSong} index={2} onDelete={vi.fn()} onMoveToTop={vi.fn()} />);
    expect(screen.getByText("3")).toBeInTheDocument();
  });

  it("shows duration", () => {
    renderWithDnd(<QueueRow song={mockSong} index={0} onDelete={vi.fn()} onMoveToTop={vi.fn()} />);
    expect(screen.getByText("3:20")).toBeInTheDocument();
  });

  it("shows checkbox when onToggleSelect is provided", () => {
    renderWithDnd(
      <QueueRow song={mockSong} index={0} onDelete={vi.fn()} onMoveToTop={vi.fn()} onToggleSelect={vi.fn()} />,
    );
    expect(screen.getByRole("checkbox")).toBeInTheDocument();
  });

  it("shows menu button", () => {
    renderWithDnd(<QueueRow song={mockSong} index={0} onDelete={vi.fn()} onMoveToTop={vi.fn()} />);
    const moreVert = screen.queryByTestId("MoreVertIcon");
    expect(moreVert).toBeInTheDocument();
  });
});
