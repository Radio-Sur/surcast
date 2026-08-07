import { useEffect, useRef, useState } from "react";
import {
  useAddAutoFillPlaylist,
  useAutoFill,
  useDeleteAutoFillPlaylist,
  useTriggerAutoFill,
  useUpdateAutoFill,
  useUpdateAutoFillPlaylist,
} from "@/hooks/use-auto-fill";
import { useSnackbar } from "@/providers/snackbar-provider";
import type { Playlist, ScheduleSourceType } from "@/types";

export function useAutoDjConfig(stationId: string, playlists: Playlist[]) {
  const { showSnackbar } = useSnackbar();
  const { data: config, isLoading, isError, error } = useAutoFill(stationId);
  const updateAutoFill = useUpdateAutoFill(stationId);
  const addAutoFillPlaylist = useAddAutoFillPlaylist(stationId);
  const updateAutoFillPlaylist = useUpdateAutoFillPlaylist(stationId);
  const deleteAutoFillPlaylist = useDeleteAutoFillPlaylist(stationId);
  const triggerAutoFill = useTriggerAutoFill(stationId);

  const [localEnabled, setLocalEnabled] = useState(true);
  const [localMode, setLocalMode] = useState("random");
  const [localSourceType, setLocalSourceType] = useState<ScheduleSourceType>("station_library");
  const [localPlaylistId, setLocalPlaylistId] = useState<string | null>(null);
  const [localAvoidRepeat, setLocalAvoidRepeat] = useState(true);
  const [localMinGap, setLocalMinGap] = useState(3);
  const [localSongsAhead, setLocalSongsAhead] = useState(5);
  const [saving, setSaving] = useState(false);

  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (config) {
      setLocalEnabled(config.enabled);
      setLocalMode(config.mode);
      setLocalSourceType(config.source_type);
      setLocalPlaylistId(config.source_playlist_id);
      setLocalAvoidRepeat(config.avoid_artist_repeat);
      setLocalMinGap(config.min_song_gap);
      setLocalSongsAhead(config.songs_ahead);
    }
  }, [config]);

  const autoSave = (overrides: Record<string, unknown> = {}) => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      setSaving(true);
      updateAutoFill.mutate(
        {
          enabled: (overrides.enabled as boolean | undefined) ?? localEnabled,
          mode: (overrides.mode as string | undefined) ?? localMode,
          source_type: (overrides.source_type as ScheduleSourceType | undefined) ?? localSourceType,
          source_playlist_id:
            localSourceType === "playlist"
              ? ((overrides.source_playlist_id as string | null | undefined) ?? localPlaylistId)
              : null,
          avoid_artist_repeat: (overrides.avoid_artist_repeat as boolean | undefined) ?? localAvoidRepeat,
          min_song_gap: (overrides.min_song_gap as number | undefined) ?? localMinGap,
          songs_ahead: (overrides.songs_ahead as number | undefined) ?? localSongsAhead,
        },
        {
          onSettled: () => setSaving(false),
          onError: (err) => {
            console.error("Failed to save auto-DJ config", err);
            showSnackbar("Failed to save auto-DJ configuration", "error");
          },
        },
      );
    }, 400);
  };

  const setField = (field: string, value: unknown) => {
    const overrides: Record<string, unknown> = {};
    switch (field) {
      case "enabled":
        setLocalEnabled(value as boolean);
        overrides.enabled = value;
        break;
      case "mode":
        setLocalMode(value as string);
        overrides.mode = value;
        break;
      case "source_type":
        setLocalSourceType(value as ScheduleSourceType);
        overrides.source_type = value;
        break;
      case "playlist_id":
        setLocalPlaylistId(value as string | null);
        overrides.source_playlist_id = value;
        break;
      case "avoid_repeat":
        setLocalAvoidRepeat(value as boolean);
        overrides.avoid_artist_repeat = value;
        break;
      case "min_gap":
        setLocalMinGap(value as number);
        overrides.min_song_gap = value;
        break;
      case "songs_ahead":
        setLocalSongsAhead(value as number);
        overrides.songs_ahead = value;
        break;
    }
    autoSave(overrides);
  };

  return {
    config,
    isLoading,
    isError,
    error,
    saving,
    localEnabled,
    localMode,
    localSourceType,
    localPlaylistId,
    localAvoidRepeat,
    localMinGap,
    localSongsAhead,
    setField,
    addAutoFillPlaylist,
    updateAutoFillPlaylist,
    deleteAutoFillPlaylist,
    triggerAutoFill,
    playlists,
  };
}
