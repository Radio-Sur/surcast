import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import LinearProgress from "@mui/material/LinearProgress";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import type { LiveListeners, PlaylistGroup, QueueItem, StreamStatus } from "@/types";
import { isPlaylistGroup } from "@/types";
import { fmt } from "./";
import { LiveListenersBadge } from "./live-listeners-badge";
import { NowPlayingPlaylistGroup } from "./now-playing-playlist-group";
import { NowPlayingSong } from "./now-playing-song";

export function NowPlaying({
  item,
  streamStatus,
  connected,
  elapsed,
  onSkip,
  isSkipping,
  listeners,
  playing,
}: {
  item: QueueItem | PlaylistGroup;
  streamStatus: StreamStatus | null;
  connected: boolean;
  elapsed: number;
  onSkip?: () => void;
  isSkipping?: boolean;
  listeners?: LiveListeners | null;
  /** Display override: false forces the stopped look even if the last
   * known status still says playing (e.g. pause acknowledged locally
   * before the backend status catches up). Defaults to the status flag. */
  playing?: boolean;
}) {
  const { t } = useTranslation();
  const isPlaying = playing ?? streamStatus?.playing ?? false;

  return (
    <Box
      sx={{
        mb: 2,
        borderRadius: 2,
        bgcolor: isPlaying ? "primary.main" : "action.disabledBackground",
        color: isPlaying ? "primary.contrastText" : "text.secondary",
        overflow: "hidden",
      }}
    >
      <Box sx={{ p: 2, pb: isPlaylistGroup(item) ? 0 : 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="caption" sx={{ opacity: 0.7, fontWeight: 600, letterSpacing: 1 }}>
              {isPlaylistGroup(item) ? t("stations:queue_now_playing_playlist") : t("stations:queue_now_playing")}
            </Typography>
            {!isPlaying && (
              <Chip label={t("stations:stream_status_stopped")} size="small" variant="outlined" sx={{ height: 20 }} />
            )}
          </Box>
          {listeners && (
            <Box sx={{ opacity: 0.9 }}>
              <LiveListenersBadge listeners={listeners} />
            </Box>
          )}
        </Box>

        {isPlaylistGroup(item) ? (
          <NowPlayingPlaylistGroup
            group={item}
            currentSong={item.current_song_index !== undefined ? item.songs[item.current_song_index] : null}
            connected={connected}
            onSkip={onSkip}
            isSkipping={isSkipping}
          />
        ) : (
          <NowPlayingSong song={item} connected={connected} onSkip={onSkip} isSkipping={isSkipping} />
        )}
      </Box>

      {streamStatus && isPlaying && (
        <Box sx={{ px: 2, pb: 2 }}>
          <LinearProgress
            variant={isSkipping ? "indeterminate" : streamStatus.duration > 0 ? "determinate" : "indeterminate"}
            value={
              !isSkipping && streamStatus.duration > 0
                ? Math.min((elapsed / streamStatus.duration) * 100, 100)
                : undefined
            }
            sx={{
              height: 4,
              borderRadius: 2,
              bgcolor: "rgba(255,255,255,0.2)",
              "& .MuiLinearProgress-bar": { bgcolor: "rgba(255,255,255,0.8)" },
            }}
          />
          <Box sx={{ display: "flex", justifyContent: "space-between", mt: 0.5 }}>
            <Typography variant="caption" sx={{ opacity: 0.6 }}>
              {streamStatus.duration > 0 ? fmt(elapsed) : t("common:duration_unknown")}
            </Typography>
            <Typography variant="caption" sx={{ opacity: 0.6 }}>
              {streamStatus.duration > 0 ? fmt(streamStatus.duration) : t("common:duration_unknown")}
            </Typography>
          </Box>
        </Box>
      )}
    </Box>
  );
}
