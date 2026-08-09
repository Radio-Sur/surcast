import { useMemo, useState } from "react";
import { useAppConfig } from "@/hooks/use-config";
import { useElapsedTimer } from "@/hooks/use-elapsed-timer";
import { usePlaylists } from "@/hooks/use-playlists";
import { useRemoveStationSong, useStationSongs, useStationSongsAll } from "@/hooks/use-station-library";
import {
  useAddToQueue,
  useRemoveFromQueue,
  useRemovePlaylistFromQueue,
  useReorderQueue,
} from "@/hooks/use-station-queue";
import { useStation, useUpdateStation } from "@/hooks/use-stations";
import { useStreamPause, useStreamPlay, useStreamRestart, useStreamSkip } from "@/hooks/use-stream";
import { isHttpError } from "@/lib/is-http-error";
import { useLiveStation } from "@/providers/live-provider";
import { useSnackbar } from "@/providers/snackbar-provider";

export function useStationDetail(id: string | undefined) {
  const { showSnackbar } = useSnackbar();
  const safeId = id ?? "";

  const {
    data: station,
    isLoading: stationLoading,
    isError: stationError,
    error: stationLoadError,
  } = useStation(safeId);
  const [libraryPage, setLibraryPage] = useState(0);
  const [libraryPerPage, setLibraryPerPage] = useState(50);

  const {
    data: librarySongsData,
    isLoading: libraryLoading,
    isError: libraryError,
    error: libraryLoadError,
  } = useStationSongs(safeId, libraryPage + 1, libraryPerPage);
  const librarySongs = librarySongsData?.songs;
  const librarySongTotal = librarySongsData?.total ?? 0;
  const { data: libraryAllData } = useStationSongsAll(safeId);
  const librarySongsAll = libraryAllData?.songs;
  const { data: playlists } = usePlaylists();

  const removeStationSong = useRemoveStationSong(safeId);
  const addToQueue = useAddToQueue(safeId);
  const removeFromQueue = useRemoveFromQueue(safeId);
  const reorderQueue = useReorderQueue(safeId);
  const removePlaylistFromQueue = useRemovePlaylistFromQueue(safeId);
  const streamPlay = useStreamPlay(safeId);
  const streamPause = useStreamPause(safeId);
  const streamRestart = useStreamRestart(safeId);
  const httpSkip = useStreamSkip(safeId);
  const updateStation = useUpdateStation();

  const [tab, setTab] = useState(0);
  const [queueAddOpen, setQueueAddOpen] = useState(false);
  const [confirmAction, setConfirmAction] = useState<"pause" | "restart" | null>(null);

  const { status: streamStatus, queue: wsQueue, connected, listeners: liveListeners } = useLiveStation(station?.id);

  const icecastHost =
    useAppConfig().data?.icecast_public_url || import.meta.env.VITE_ICECAST_HOST || "http://localhost:8000";
  const mount = station?.stream_url || (station ? `${station.name}.mp3` : "");
  const streamUrl = station
    ? `${icecastHost}/${encodeURIComponent(mount.endsWith(".mp3") ? mount : `${mount}.mp3`)}`
    : "";

  const elapsed = useElapsedTimer(streamStatus);

  const liveQueue = wsQueue;

  const queueSections = useMemo(() => {
    if (!liveQueue) return { played: [], nowPlaying: null, upcoming: [] };
    if (!streamStatus || liveQueue.length === 0) {
      return { played: [], nowPlaying: null, upcoming: liveQueue };
    }
    const pos = streamStatus.song_index % liveQueue.length;
    return {
      played: liveQueue.slice(0, pos),
      nowPlaying: liveQueue[pos],
      upcoming: liveQueue.slice(pos + 1),
    };
  }, [liveQueue, streamStatus]);

  const queueEndEstimate = useMemo(() => {
    if (!queueSections) return null;
    const upcomingTotal = queueSections.upcoming.reduce((sum: number, s: { duration: number }) => sum + s.duration, 0);
    const currentRemaining = queueSections.nowPlaying ? queueSections.nowPlaying.duration : 0;
    const totalSec = upcomingTotal + currentRemaining;
    if (totalSec <= 0) return null;
    const now = new Date();
    now.setSeconds(now.getSeconds() + totalSec);
    const h = now.getHours().toString().padStart(2, "0");
    const m = now.getMinutes().toString().padStart(2, "0");
    return `${h}:${m}`;
  }, [queueSections]);

  const handleMutationError = (err: unknown, label: string) => {
    console.error(`Failed to ${label}:`, err);
    showSnackbar(isHttpError(err).message || `Failed to ${label}`, "error");
  };

  const handleAddToQueue = async (songIds: string[]) => {
    if (songIds.length === 0) return;
    try {
      await addToQueue.mutateAsync(songIds);
      setQueueAddOpen(false);
    } catch (err) {
      handleMutationError(err, "add songs to queue");
      throw err;
    }
  };

  const handleRemoveFromStation = (songId: string) => {
    removeStationSong.mutate(songId, {
      onError: (err) => handleMutationError(err, "remove song from station"),
    });
  };

  const handleRemoveFromQueue = (songId: string) => {
    removeFromQueue.mutate(songId, {
      onError: (err) => handleMutationError(err, "remove from queue"),
    });
  };

  const handleReAddToQueue = (songId: string) => {
    addToQueue.mutate([songId], {
      onError: (err) => handleMutationError(err, "add to queue"),
    });
  };

  return {
    id,
    station,
    stationLoading,
    stationError,
    stationLoadError,
    librarySongs,
    librarySongsAll,
    librarySongTotal,
    libraryPage,
    libraryPerPage,
    setLibraryPage,
    setLibraryPerPage,
    libraryLoading,
    libraryError,
    libraryLoadError,
    playlists,
    addToQueue,
    reorderQueue,
    removePlaylistFromQueue,
    streamPlay,
    streamPause,
    streamRestart,
    httpSkip,
    updateStation,
    tab,
    setTab,
    queueAddOpen,
    setQueueAddOpen,
    confirmAction,
    setConfirmAction,
    streamStatus,
    connected,
    liveListeners,
    streamUrl,
    elapsed,
    queueSections,
    queueEndEstimate,
    handleAddToQueue,
    handleRemoveFromStation,
    handleRemoveFromQueue,
    handleReAddToQueue,
    handleMutationError,
  };
}
