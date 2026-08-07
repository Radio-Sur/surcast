import {
  closestCenter,
  DndContext,
  type DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import { SortableContext, sortableKeyboardCoordinates, verticalListSortingStrategy } from "@dnd-kit/sortable";
import Delete from "@mui/icons-material/Delete";
import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import Box from "@mui/material/Box";
import Collapse from "@mui/material/Collapse";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  computeGroupEndsAt,
  fmt,
  groupId,
  groupItems,
  isGroupId,
  playlistIdFromGroupId,
  QUEUE_END,
} from "@/components/queue";
import { DraggablePlaylistGroup } from "@/components/queue/draggable-playlist-group";
import { PlaylistGroupCard } from "@/components/queue/playlist-group-card";
import { QueueEndSentinel } from "@/components/queue/queue-end-sentinel";
import { QueueRow } from "@/components/queue/queue-row";
import { SongCover } from "@/components/song-cover";
import { isPlaylistGroup, type PlaylistGroup, type QueueItem } from "@/types";
import { getDropTargetIndex, UPCOMING_KEY } from "./queue-section-utils";

export function UpcomingQueue({
  stationId,
  queueSections,
  selectedIds,
  reorderQueue,
  removePlaylistFromQueue,
  handleRemoveFromQueue,
  handleMoveToTop,
  handleToggleSelect,
  showSnackbar,
}: {
  stationId: string;
  queueSections: { played: QueueItem[]; nowPlaying: QueueItem | null; upcoming: QueueItem[] };
  selectedIds: Set<string>;
  reorderQueue: {
    isPending: boolean;
    mutate: (songIds: string[], options?: { onError?: (err: unknown) => void }) => void;
  };
  removePlaylistFromQueue: {
    isPending: boolean;
    mutate: (playlistId: string, options?: { onError?: (err: unknown) => void }) => void;
  };
  handleRemoveFromQueue: (itemId: string) => void;
  handleMoveToTop: (itemId: string) => void;
  handleToggleSelect: (id: string) => void;
  showSnackbar: (msg: string, severity: "error" | "success") => void;
}) {
  const { t } = useTranslation();
  const [upcomingOpen, setUpcomingOpen] = useState(
    () => localStorage.getItem(`${UPCOMING_KEY}${stationId}`) !== "false",
  );

  const upcomingGroups = useMemo(() => groupItems(queueSections.upcoming), [queueSections.upcoming]);
  const upcomingStartPositions = useMemo(() => {
    const positions: number[] = [];
    let pos = 1;
    for (const _ of upcomingGroups) {
      positions.push(pos);
      pos++;
    }
    return positions;
  }, [upcomingGroups]);

  const dndItemIds = useMemo(() => {
    const ids: string[] = [];
    for (const item of queueSections.upcoming) {
      if (item.origin_playlist_id) {
        const playlistGroupId = groupId(item.origin_playlist_id);
        if (!ids.includes(playlistGroupId)) ids.push(playlistGroupId);
      } else {
        ids.push(item.id);
      }
    }
    ids.push(QUEUE_END);
    return ids;
  }, [queueSections.upcoming]);

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );

  const handleReorderInGroup = useCallback(
    (playlistId: string, newItemIds: string[]) => {
      const upcoming = queueSections.upcoming;
      const groupIndices = upcoming.map((_, i) => i).filter((i) => upcoming[i].origin_playlist_id === playlistId);
      if (groupIndices.length === 0) return;
      const newUpcoming = [...upcoming];
      for (let j = 0; j < newItemIds.length && j < groupIndices.length; j++) {
        const song = upcoming.find((s) => s.id === newItemIds[j]);
        if (song) newUpcoming[groupIndices[j]] = song;
      }
      const full = [
        ...queueSections.played,
        ...(queueSections.nowPlaying ? [queueSections.nowPlaying] : []),
        ...newUpcoming,
      ];
      reorderQueue.mutate(
        full.map((s) => s.id),
        {
          onError: (err) => {
            console.error("Failed to reorder queue", err);
            showSnackbar("Failed to reorder queue", "error");
          },
        },
      );
    },
    [queueSections, reorderQueue, showSnackbar],
  );

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event;
      if (!over || active.id === over.id) return;
      const activeId = active.id as string;
      const overId = over.id as string;
      if (activeId === QUEUE_END) return;
      const upcoming = queueSections.upcoming;
      const pointerY = (() => {
        const ae = event.activatorEvent;
        if (ae instanceof MouseEvent) return ae.clientY;
        if (ae instanceof TouchEvent) return ae.changedTouches[0]?.clientY ?? null;
        return null;
      })();

      if (isGroupId(activeId)) {
        const playlistGroupId = playlistIdFromGroupId(activeId);
        const groupSongs = upcoming.filter((s) => s.origin_playlist_id === playlistGroupId);
        if (groupSongs.length === 0) return;
        const firstIdx = upcoming.indexOf(groupSongs[0]);
        const lastIdx = upcoming.indexOf(groupSongs[groupSongs.length - 1]);
        const group = upcoming.slice(firstIdx, lastIdx + 1);
        const withoutGroup = [...upcoming.slice(0, firstIdx), ...upcoming.slice(lastIdx + 1)];
        const targetIdx = getDropTargetIndex(overId, withoutGroup, pointerY);
        if (targetIdx === -1) return;
        const reordered = [...withoutGroup.slice(0, targetIdx), ...group, ...withoutGroup.slice(targetIdx)];
        const full = [
          ...queueSections.played,
          ...(queueSections.nowPlaying ? [queueSections.nowPlaying] : []),
          ...reordered,
        ];
        reorderQueue.mutate(
          full.map((s) => s.id),
          {
            onError: (err) => {
              console.error("Failed to reorder queue", err);
              showSnackbar("Failed to reorder queue", "error");
            },
          },
        );
      } else {
        const oldIdx = upcoming.findIndex((s) => s.id === activeId);
        if (oldIdx === -1) return;
        const item = upcoming[oldIdx];
        const withoutItem = [...upcoming.slice(0, oldIdx), ...upcoming.slice(oldIdx + 1)];
        const targetIdx = getDropTargetIndex(overId, withoutItem, pointerY);
        if (targetIdx === -1) return;
        const reordered = [...withoutItem.slice(0, targetIdx), item, ...withoutItem.slice(targetIdx)];
        const full = [
          ...queueSections.played,
          ...(queueSections.nowPlaying ? [queueSections.nowPlaying] : []),
          ...reordered,
        ];
        reorderQueue.mutate(
          full.map((s) => s.id),
          {
            onError: (err) => {
              console.error("Failed to reorder queue", err);
              showSnackbar("Failed to reorder queue", "error");
            },
          },
        );
      }
    },
    [queueSections, reorderQueue, showSnackbar],
  );

  const renderItem = (g: PlaylistGroup | QueueItem, gi: number) => {
    if (isPlaylistGroup(g)) {
      const endsAt = computeGroupEndsAt(g, queueSections.nowPlaying, queueSections.upcoming, 0);
      return reorderQueue.isPending ? (
        <PlaylistGroupCard
          key={g.playlist_id}
          group={g}
          endsAt={endsAt}
          playlistNumber={upcomingStartPositions[gi]}
          selectedSongIds={selectedIds}
          onToggleSelectSong={handleToggleSelect}
          onDeleteSong={handleRemoveFromQueue}
          onRemovePlaylist={() =>
            removePlaylistFromQueue.mutate(g.playlist_id, {
              onError: (err) => {
                console.error("Failed to remove playlist from queue", err);
              },
            })
          }
        />
      ) : (
        <DraggablePlaylistGroup
          key={g.playlist_id}
          group={g}
          id={groupId(g.playlist_id)}
          playlistNumber={upcomingStartPositions[gi]}
          selected={selectedIds.has(groupId(g.playlist_id))}
          onToggleSelect={() => handleToggleSelect(groupId(g.playlist_id))}
          selectedSongIds={selectedIds}
          onToggleSelectSong={handleToggleSelect}
          onDeleteSong={handleRemoveFromQueue}
          onReorderInGroup={(newSongIds) => handleReorderInGroup(g.playlist_id, newSongIds)}
          onRemovePlaylist={() =>
            removePlaylistFromQueue.mutate(g.playlist_id, {
              onError: (err) => {
                console.error("Failed to remove playlist from queue", err);
              },
            })
          }
          onMoveToTop={() => handleMoveToTop(groupId(g.playlist_id))}
          endsAt={endsAt}
        />
      );
    }
    if (reorderQueue.isPending) {
      return (
        <Box key={g.id} sx={{ display: "flex", alignItems: "center", gap: 1, p: "6px 10px", borderRadius: 2 }}>
          <Typography variant="body2" sx={{ minWidth: 24, textAlign: "right", color: "text.secondary" }}>
            {upcomingStartPositions[gi]}
          </Typography>
          <Box sx={{ flex: 1, minWidth: 0, ml: 3, display: "flex", alignItems: "center", gap: 1.5 }}>
            <SongCover songId={g.song_id} hasCover={g.has_cover} size={32} autoDj={g.is_auto_dj} />
            <Box sx={{ flex: 1, minWidth: 0 }}>
              <Typography variant="body2" noWrap sx={{ fontWeight: 500 }}>
                {g.title}
              </Typography>
              <Typography variant="caption" noWrap color="text.secondary">
                {g.artist || "Unknown artist"}
                {g.album ? ` · ${g.album}` : ""}
              </Typography>
            </Box>
          </Box>
          <Typography variant="caption" color="text.secondary">
            {g.duration > 0 ? fmt(g.duration) : "--:--"}
          </Typography>
          <IconButton
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              handleRemoveFromQueue(g.id);
            }}
            sx={{ color: "error.main" }}
          >
            <Delete fontSize="small" />
          </IconButton>
        </Box>
      );
    }
    return (
      <QueueRow
        key={g.id}
        song={g}
        index={upcomingStartPositions[gi] - 1}
        selected={selectedIds.has(g.id)}
        onToggleSelect={() => handleToggleSelect(g.id)}
        onDelete={() => handleRemoveFromQueue(g.id)}
        onMoveToTop={() => handleMoveToTop(g.id)}
      />
    );
  };

  const content = reorderQueue.isPending ? (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>
      {upcomingGroups.map((g, gi) => renderItem(g, gi))}
    </Box>
  ) : (
    <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
      <SortableContext items={dndItemIds} strategy={verticalListSortingStrategy}>
        <Box sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>
          {upcomingGroups.map((g, gi) => renderItem(g, gi))}
          <QueueEndSentinel />
        </Box>
      </SortableContext>
    </DndContext>
  );
  return (
    <Box sx={{ mb: 2 }}>
      <Box
        onClick={() => setUpcomingOpen(!upcomingOpen)}
        sx={{ display: "flex", alignItems: "center", gap: 0.5, cursor: "pointer", px: 1, mb: 0.5 }}
      >
        {upcomingOpen ? <ExpandLess fontSize="small" /> : <ExpandMore fontSize="small" />}
        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 600 }}>
          {t("stations:queue_upcoming", {
            count: upcomingGroups.reduce((sum, g) => sum + (isPlaylistGroup(g) ? g.songs.length : 1), 0),
          })}
        </Typography>
      </Box>
      <Collapse in={upcomingOpen}>{content}</Collapse>
    </Box>
  );
}
