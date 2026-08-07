import {
  closestCenter,
  DndContext,
  type DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import Box from "@mui/material/Box";
import Collapse from "@mui/material/Collapse";
import { useCallback, useMemo } from "react";
import type { PlaylistGroup } from "@/types";
import { NestedSongRow } from "./nested-song-row";
import { SimpleSongRow } from "./simple-song-row";

export function PlaylistGroupSongs({
  group,
  open,
  dimmed,
  selectedSongIds,
  onToggleSelectSong,
  onDeleteSong,
  onReorderInGroup,
  onReAddSong,
}: {
  group: PlaylistGroup;
  open: boolean;
  dimmed?: boolean;
  selectedSongIds?: Set<string>;
  onToggleSelectSong?: (id: string) => void;
  onDeleteSong?: (itemId: string) => void;
  onReorderInGroup?: (itemIds: string[]) => void;
  onReAddSong?: (itemId: string) => void;
}) {
  const nestedSensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  const nestedItemIds = useMemo(() => group.songs.map((s) => s.id), [group.songs]);

  const handleNestedDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event;
      if (!over || active.id === over.id || !onReorderInGroup) return;

      const oldIdx = nestedItemIds.indexOf(active.id as string);
      const newIdx = nestedItemIds.indexOf(over.id as string);
      if (oldIdx === -1 || newIdx === -1) return;

      const reordered = arrayMove(nestedItemIds, oldIdx, newIdx);
      onReorderInGroup(reordered);
    },
    [nestedItemIds, onReorderInGroup],
  );

  const songs = group.songs;

  if (onReorderInGroup) {
    return (
      <DndContext key="dnd" sensors={nestedSensors} collisionDetection={closestCenter} onDragEnd={handleNestedDragEnd}>
        <SortableContext items={nestedItemIds} strategy={verticalListSortingStrategy}>
          <Collapse in={open}>
            <Box sx={{ borderTop: 1, borderColor: "divider", bgcolor: "action.hover" }}>
              {songs.map((song, idx) => (
                <NestedSongRow
                  key={song.id}
                  song={song}
                  index={idx}
                  selected={selectedSongIds?.has(song.id)}
                  onToggleSelect={onToggleSelectSong ? () => onToggleSelectSong(song.id) : undefined}
                  onDelete={onDeleteSong}
                />
              ))}
            </Box>
          </Collapse>
        </SortableContext>
      </DndContext>
    );
  }

  return (
    <Collapse in={open}>
      <Box sx={{ borderTop: 1, borderColor: "divider", bgcolor: "action.hover" }}>
        {songs.map((song, idx) => (
          <SimpleSongRow
            key={song.id}
            song={song}
            index={idx}
            dimmed={dimmed}
            selected={!dimmed && selectedSongIds?.has(song.id)}
            onToggleSelect={!dimmed && onToggleSelectSong ? () => onToggleSelectSong(song.id) : undefined}
            onDelete={!dimmed ? onDeleteSong : undefined}
            onReAdd={dimmed ? onReAddSong : undefined}
          />
        ))}
      </Box>
    </Collapse>
  );
}
