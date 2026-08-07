import Add from "@mui/icons-material/Add";
import Delete from "@mui/icons-material/Delete";
import PlaylistPlay from "@mui/icons-material/PlaylistPlay";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import Typography from "@mui/material/Typography";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { isGroupId, playlistIdFromGroupId } from "@/components/queue";
import { NowPlaying } from "@/components/queue/now-playing";
import { ScheduleInfoBanner } from "@/components/schedule/schedule-info-banner";
import { useSchedules } from "@/hooks/use-schedules";
import { useSnackbar } from "@/providers/snackbar-provider";
import type { LiveListeners, PlaylistGroup, QueueItem, StreamStatus } from "@/types";
import { PlayedQueue } from "./played-queue";
import { UpcomingQueue } from "./upcoming-queue";

interface QueueSectionProps {
  stationId: string;
  queueSections: { played: QueueItem[]; nowPlaying: QueueItem | null; upcoming: QueueItem[] };
  streamStatus: StreamStatus | null;
  connected: boolean;
  elapsed: number;
  listeners?: LiveListeners | null;
  httpSkip: { isPending: boolean; mutate: (...args: unknown[]) => void };
  reorderQueue: {
    isPending: boolean;
    mutate: (songIds: string[], options?: { onError?: (err: unknown) => void }) => void;
  };
  removePlaylistFromQueue: {
    isPending: boolean;
    mutate: (playlistId: string, options?: { onError?: (err: unknown) => void }) => void;
  };
  handleRemoveFromQueue: (itemId: string) => void;
  handleReAddToQueue: (songId: string) => void;
  setQueueAddOpen: (open: boolean) => void;
}

function useNowPlayingGroup(queueSections: {
  played: QueueItem[];
  nowPlaying: QueueItem | null;
  upcoming: QueueItem[];
}) {
  return useMemo(() => {
    const np = queueSections.nowPlaying;
    if (!np) return null;
    if (!np.origin_playlist_id) return np as QueueItem | PlaylistGroup;
    const allSongs = [
      ...queueSections.played,
      ...(queueSections.nowPlaying ? [queueSections.nowPlaying] : []),
      ...queueSections.upcoming,
    ];
    const npFlatIdx = allSongs.findIndex((s) => s.id === np.id);
    if (npFlatIdx < 0) return np;
    const groupSongs: QueueItem[] = [];
    let idx = npFlatIdx;
    while (idx >= 0 && allSongs[idx].origin_playlist_id === np.origin_playlist_id) {
      groupSongs.unshift(allSongs[idx]);
      idx--;
    }
    idx = npFlatIdx + 1;
    while (idx < allSongs.length && allSongs[idx].origin_playlist_id === np.origin_playlist_id) {
      groupSongs.push(allSongs[idx]);
      idx++;
    }
    const playingIdx = groupSongs.findIndex((s) => s.id === np.id);
    return {
      kind: "playlist_group" as const,
      playlist_id: np.origin_playlist_id,
      playlist_name: np.playlist_name || "Playlist",
      songs: groupSongs,
      total_duration: groupSongs.reduce((sum, s) => sum + s.duration, 0),
      current_song_index: playingIdx >= 0 ? playingIdx : undefined,
    };
  }, [queueSections]);
}

export function QueueSection(props: QueueSectionProps) {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const {
    stationId,
    queueSections,
    streamStatus,
    connected,
    elapsed,
    listeners,
    httpSkip,
    reorderQueue,
    removePlaylistFromQueue,
    handleRemoveFromQueue,
    handleReAddToQueue,
    setQueueAddOpen,
  } = props;

  const { data: schedules, isLoading: schedulesLoading, isError: schedulesError } = useSchedules(stationId);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false);

  const handleToggleSelect = useCallback((id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const handleBulkDelete = useCallback(async () => {
    let failed = 0;
    for (const id of selectedIds) {
      if (isGroupId(id)) {
        removePlaylistFromQueue.mutate(playlistIdFromGroupId(id), {
          onError: () => {
            failed++;
          },
        });
      } else {
        handleRemoveFromQueue(id);
      }
    }

    if (failed > 0) {
      console.error(`${failed} deletion(s) failed`);
      showSnackbar(`${failed} item(s) could not be deleted`, "error");
    }

    setSelectedIds(new Set());
    setBulkDeleteOpen(false);
  }, [selectedIds, removePlaylistFromQueue, handleRemoveFromQueue, showSnackbar]);

  const nowPlayingGroup = useNowPlayingGroup(queueSections);

  const handleMoveToTop = useCallback(
    (itemId: string) => {
      const upcoming = queueSections.upcoming;
      if (upcoming.length === 0) return;
      if (isGroupId(itemId)) {
        const playlistId = playlistIdFromGroupId(itemId);
        const groupSongs = upcoming.filter((s) => s.origin_playlist_id === playlistId);
        if (groupSongs.length === 0) return;
        const otherSongs = upcoming.filter((s) => s.origin_playlist_id !== playlistId);
        const full = [
          ...queueSections.played,
          ...(queueSections.nowPlaying ? [queueSections.nowPlaying] : []),
          ...groupSongs,
          ...otherSongs,
        ];
        reorderQueue.mutate(
          full.map((s) => s.id),
          {
            onError: (err: unknown) => {
              console.error("Failed to reorder queue", err);
              showSnackbar("Failed to reorder queue", "error");
            },
          },
        );
      } else {
        const songIndex = upcoming.findIndex((s) => s.id === itemId);
        if (songIndex === -1) return;
        const item = upcoming[songIndex];
        const otherSongs = [...upcoming];
        otherSongs.splice(songIndex, 1);
        otherSongs.splice(0, 0, item);
        const full = [
          ...queueSections.played,
          ...(queueSections.nowPlaying ? [queueSections.nowPlaying] : []),
          ...otherSongs,
        ];
        reorderQueue.mutate(
          full.map((s) => s.id),
          {
            onError: (err: unknown) => {
              console.error("Failed to reorder queue", err);
              showSnackbar("Failed to reorder queue", "error");
            },
          },
        );
      }
      setSelectedIds(new Set());
    },
    [queueSections, reorderQueue, showSnackbar],
  );

  return (
    <Card variant="outlined" sx={{ borderRadius: 3 }}>
      <CardContent sx={{ p: 4, "&:last-child": { pb: 4 } }}>
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 2 }}>
          <Typography variant="h6">{t("stations:queue_title")}</Typography>
          <Button size="small" startIcon={<Add />} variant="contained" onClick={() => setQueueAddOpen(true)}>
            {t("stations:queue_add_library")}
          </Button>
        </Box>

        {queueSections.nowPlaying || queueSections.upcoming.length > 0 || queueSections.played.length > 0 ? (
          <>
            {schedulesError ? (
              <Alert severity="error" sx={{ mb: 2 }}>
                Failed to load schedule data
              </Alert>
            ) : (
              <ScheduleInfoBanner
                schedules={schedules}
                isLoading={schedulesLoading}
                upcoming={queueSections.upcoming}
                nowPlaying={queueSections.nowPlaying}
                elapsed={elapsed}
              />
            )}

            {queueSections.nowPlaying && nowPlayingGroup && (
              <NowPlaying
                item={nowPlayingGroup}
                streamStatus={streamStatus}
                connected={connected}
                elapsed={elapsed}
                listeners={listeners}
                onSkip={() =>
                  httpSkip.mutate(undefined, {
                    onError: (err: unknown) => {
                      console.error("Failed to skip track", err);
                    },
                  })
                }
                isSkipping={httpSkip.isPending}
              />
            )}

            <PlayedQueue stationId={stationId} played={queueSections.played} onReAddToQueue={handleReAddToQueue} />

            <UpcomingQueue
              stationId={stationId}
              queueSections={queueSections}
              selectedIds={selectedIds}
              reorderQueue={reorderQueue}
              removePlaylistFromQueue={removePlaylistFromQueue}
              handleRemoveFromQueue={handleRemoveFromQueue}
              handleMoveToTop={handleMoveToTop}
              handleToggleSelect={handleToggleSelect}
              showSnackbar={showSnackbar}
            />

            {selectedIds.size > 0 && (
              <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 2, px: 1 }}>
                <Typography variant="body2" color="text.secondary" sx={{ mr: 1 }}>
                  {t("stations:queue_selected", { count: selectedIds.size })}
                </Typography>
                <Button
                  size="small"
                  variant="outlined"
                  color="error"
                  startIcon={<Delete />}
                  onClick={() => setBulkDeleteOpen(true)}
                >
                  {t("stations:queue_delete_selected")}
                </Button>
                <Button size="small" onClick={() => setSelectedIds(new Set())}>
                  {t("common:clear")}
                </Button>
                <Dialog open={bulkDeleteOpen} onClose={() => setBulkDeleteOpen(false)}>
                  <DialogTitle>{t("stations:queue_delete_title", { count: selectedIds.size })}</DialogTitle>
                  <DialogContent>
                    <DialogContentText>
                      {t("stations:queue_delete_message", { count: selectedIds.size })}
                    </DialogContentText>
                  </DialogContent>
                  <DialogActions>
                    <Button onClick={() => setBulkDeleteOpen(false)}>{t("common:cancel")}</Button>
                    <Button onClick={handleBulkDelete} color="error">
                      {t("common:delete")}
                    </Button>
                  </DialogActions>
                </Dialog>
              </Box>
            )}
          </>
        ) : (
          <Box sx={{ py: 6, textAlign: "center", color: "text.secondary" }}>
            <PlaylistPlay sx={{ fontSize: 48, mb: 1, opacity: 0.3 }} />
            <Typography>{t("stations:queue_empty")}</Typography>
            <Typography variant="body2">{t("stations:queue_empty_hint")}</Typography>
          </Box>
        )}
      </CardContent>
    </Card>
  );
}
