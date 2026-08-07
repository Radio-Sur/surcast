import { DndContext } from "@dnd-kit/core";
import { SortableContext, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { describe, expect, it, vi } from "vitest";
import { NestedSongRow } from "@/components/queue/nested-song-row";
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
  title: "Nested Song",
  artist: "Nested Artist",
  album: "Nested Album",
  duration: 250,
  has_cover: false,
  mime_type: "audio/mpeg",
  origin_playlist_id: "pl1",
  playlist_name: "Test Playlist",
  is_auto_dj: false,
};

describe("NestedSongRow", () => {
  it("renders song title and artist", () => {
    renderWithDnd(<NestedSongRow song={mockSong} index={0} />);
    expect(screen.getByText("Nested Song")).toBeInTheDocument();
    expect(screen.getByText(/Nested Artist/)).toBeInTheDocument();
  });

  it("shows checkbox when onToggleSelect is provided", () => {
    renderWithDnd(<NestedSongRow song={mockSong} index={0} onToggleSelect={vi.fn()} />);
    expect(screen.getByRole("checkbox")).toBeInTheDocument();
  });

  it("hides checkbox when onToggleSelect is not provided", () => {
    renderWithDnd(<NestedSongRow song={mockSong} index={0} />);
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("shows menu button when onDelete is provided", () => {
    renderWithDnd(<NestedSongRow song={mockSong} index={0} onDelete={vi.fn()} />);
    const moreVert = screen.queryByTestId("MoreVertIcon");
    expect(moreVert).toBeInTheDocument();
  });

  it("shows index", () => {
    renderWithDnd(<NestedSongRow song={mockSong} index={1} />);
    expect(screen.getByText("2")).toBeInTheDocument();
  });

  it("shows duration", () => {
    renderWithDnd(<NestedSongRow song={mockSong} index={0} />);
    expect(screen.getByText("4:10")).toBeInTheDocument();
  });
});
