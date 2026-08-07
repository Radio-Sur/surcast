import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import Box from "@mui/material/Box";
import type { PlaylistGroup } from "@/types";
import { PlaylistGroupCard } from "./playlist-group-card";

export function DraggablePlaylistGroup({
  group,
  id,
  selected,
  onToggleSelect,
  selectedSongIds,
  onToggleSelectSong,
  onDeleteSong,
  onReorderInGroup,
  onRemovePlaylist,
  onMoveToTop,
  playlistNumber,
  endsAt,
  renderActions,
}: {
  group: PlaylistGroup;
  id: string;
  selected?: boolean;
  onToggleSelect?: () => void;
  selectedSongIds?: Set<string>;
  onToggleSelectSong?: (id: string) => void;
  onDeleteSong?: (songId: string) => void;
  onReorderInGroup?: (songIds: string[]) => void;
  onRemovePlaylist?: () => void;
  onMoveToTop?: () => void;
  playlistNumber?: number;
  endsAt?: string | null;
  renderActions?: (closeMenu: () => void) => React.ReactNode;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    zIndex: isDragging ? 10 : undefined,
    position: "relative" as const,
  };

  return (
    <Box ref={setNodeRef} style={style}>
      <PlaylistGroupCard
        group={group}
        selected={selected}
        onToggleSelect={onToggleSelect}
        selectedSongIds={selectedSongIds}
        onToggleSelectSong={onToggleSelectSong}
        onDeleteSong={onDeleteSong}
        onReorderInGroup={onReorderInGroup}
        onRemovePlaylist={onRemovePlaylist}
        onMoveToTop={onMoveToTop}
        playlistNumber={playlistNumber}
        endsAt={endsAt}
        renderActions={renderActions}
        dragHandleProps={{ ...attributes, ...listeners }}
      />
    </Box>
  );
}
