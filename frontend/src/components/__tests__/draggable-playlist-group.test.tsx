import { DndContext } from "@dnd-kit/core";
import { SortableContext, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { describe, expect, it } from "vitest";
import { DraggablePlaylistGroup } from "@/components/queue/draggable-playlist-group";
import { render, screen } from "@/test/test-utils";

function renderWithDnd(ui: React.ReactElement) {
  return render(
    <DndContext>
      <SortableContext items={["pl1"]} strategy={verticalListSortingStrategy}>
        {ui}
      </SortableContext>
    </DndContext>,
  );
}

const baseGroup = {
  kind: "playlist_group" as const,
  playlist_id: "pl1",
  playlist_name: "Test Playlist",
  songs: [
    {
      id: "q1",
      station_id: "s1",
      song_id: "s1",
      position: 0,
      title: "S1",
      artist: "A",
      album: "B",
      duration: 200,
      has_cover: false,
      mime_type: "audio/mpeg",
      origin_playlist_id: "pl1",
      playlist_name: "Test Playlist",
      is_auto_dj: false,
    },
  ],
  total_duration: 200,
};

describe("DraggablePlaylistGroup", () => {
  it("renders playlist name", () => {
    renderWithDnd(<DraggablePlaylistGroup group={baseGroup} id="pl1" />);
    expect(screen.getByText("Test Playlist")).toBeInTheDocument();
  });
});
