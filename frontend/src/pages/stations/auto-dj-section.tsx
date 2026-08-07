import {
  Alert,
  Box,
  Button,
  CircularProgress,
  FormControl,
  FormControlLabel,
  InputLabel,
  MenuItem,
  Select,
  Slider,
  Switch,
  Typography,
} from "@mui/material";
import { useTranslation } from "react-i18next";
import { WeightedPlaylistsEditor } from "@/components/weighted-playlists-editor";
import type { Playlist } from "@/types";
import { useAutoDjConfig } from "./use-auto-dj-config";

export function AutoDJSection({ stationId, playlists }: { stationId: string; playlists: Playlist[] }) {
  const { t } = useTranslation();
  const {
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
  } = useAutoDjConfig(stationId, playlists);

  if (isLoading) {
    return (
      <Typography variant="body2" color="text.secondary">
        {t("common:loading")}
      </Typography>
    );
  }

  if (isError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">
          {error instanceof Error ? error.message : "Failed to load auto-DJ configuration"}
        </Alert>
      </Box>
    );
  }

  return (
    <Box>
      <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 2 }}>
        <Typography variant="h6">{t("stations:auto_dj_title")}</Typography>
        {saving && <CircularProgress size={14} sx={{ color: "text.disabled" }} />}
      </Box>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        {t("stations:auto_dj_description")}
      </Typography>

      <FormControlLabel
        control={<Switch checked={localEnabled} onChange={(e) => setField("enabled", e.target.checked)} />}
        label={t("stations:auto_dj_enable")}
      />

      {localEnabled && (
        <>
          <FormControl fullWidth size="small" sx={{ mt: 2, mb: 2 }}>
            <InputLabel>{t("stations:auto_dj_mode")}</InputLabel>
            <Select
              value={localMode}
              label={t("stations:auto_dj_mode")}
              onChange={(e) => setField("mode", e.target.value)}
            >
              <MenuItem value="random">{t("stations:auto_dj_mode_random")}</MenuItem>
              <MenuItem value="sequential">{t("stations:auto_dj_mode_sequential")}</MenuItem>
              <MenuItem value="reverse">{t("stations:auto_dj_mode_reverse")}</MenuItem>
            </Select>
          </FormControl>

          <FormControl fullWidth size="small" sx={{ mb: 2 }}>
            <InputLabel>{t("stations:auto_dj_source")}</InputLabel>
            <Select
              value={localSourceType}
              label={t("stations:auto_dj_source")}
              onChange={(e) => setField("source_type", e.target.value as string)}
            >
              <MenuItem value="station_library">{t("stations:auto_dj_source_station")}</MenuItem>
              <MenuItem value="global_library">{t("stations:auto_dj_source_global")}</MenuItem>
              <MenuItem value="weighted_playlists">{t("stations:auto_dj_source_weighted")}</MenuItem>
              <MenuItem value="playlist">{t("stations:auto_dj_source_playlist")}</MenuItem>
            </Select>
          </FormControl>

          {localSourceType === "playlist" && (
            <FormControl fullWidth size="small" sx={{ mb: 2 }}>
              <InputLabel>{t("stations:auto_dj_playlist")}</InputLabel>
              <Select
                value={localPlaylistId ?? ""}
                label={t("stations:auto_dj_playlist")}
                onChange={(e) => setField("playlist_id", e.target.value || null)}
              >
                {playlists.map((p) => (
                  <MenuItem key={p.id} value={p.id}>
                    {p.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}

          {localSourceType === "station_library" && (
            <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: "block" }}>
              {t("stations:auto_dj_station_hint")}
            </Typography>
          )}

          {localSourceType === "global_library" && (
            <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: "block" }}>
              {t("stations:auto_dj_global_hint")}
            </Typography>
          )}

          <FormControlLabel
            control={<Switch checked={localAvoidRepeat} onChange={(e) => setField("avoid_repeat", e.target.checked)} />}
            label={t("stations:auto_dj_avoid_repeat")}
          />

          {localAvoidRepeat && (
            <Box sx={{ mt: 1, mb: 2 }}>
              <Typography gutterBottom>{t("stations:auto_dj_min_gap", { count: localMinGap })}</Typography>
              <Slider
                value={localMinGap}
                onChange={(_, v) => setField("min_gap", v as number)}
                min={1}
                max={20}
                step={1}
                valueLabelDisplay="auto"
              />
            </Box>
          )}

          <Box sx={{ mb: 2 }}>
            <Typography gutterBottom>{t("stations:auto_dj_songs_ahead", { count: localSongsAhead })}</Typography>
            <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: "block" }}>
              {t("stations:auto_dj_songs_ahead_hint")}
            </Typography>
            <Slider
              value={localSongsAhead}
              onChange={(_, v) => setField("songs_ahead", v as number)}
              min={1}
              max={50}
              step={1}
              valueLabelDisplay="auto"
            />
          </Box>

          {localSourceType === "weighted_playlists" && (
            <WeightedPlaylistsEditor
              playlists={playlists}
              entries={config?.weighted_playlists ?? []}
              isAdding={addAutoFillPlaylist.isPending}
              onAdd={(playlistId, weight) =>
                addAutoFillPlaylist.mutate(
                  { playlist_id: playlistId, weight },
                  {
                    onError: (err) => {
                      console.error("Failed to add playlist to auto-fill", err);
                    },
                  },
                )
              }
              onUpdateWeight={(id, weight) =>
                updateAutoFillPlaylist.mutate(
                  { id, data: { weight } },
                  {
                    onError: (err) => {
                      console.error("Failed to update playlist weight", err);
                    },
                  },
                )
              }
              onRemove={(id) =>
                deleteAutoFillPlaylist.mutate(id, {
                  onError: (err) => {
                    console.error("Failed to remove playlist from auto-fill", err);
                  },
                })
              }
            />
          )}
        </>
      )}

      <Box sx={{ mt: 2 }}>
        <Button
          size="small"
          variant="outlined"
          onClick={() =>
            triggerAutoFill.mutate(undefined, {
              onError: (err) => {
                console.error("Failed to trigger auto-fill", err);
              },
            })
          }
          disabled={triggerAutoFill.isPending}
        >
          {triggerAutoFill.isPending ? t("common:filling") : t("stations:auto_dj_trigger")}
        </Button>
      </Box>
    </Box>
  );
}
