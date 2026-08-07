import Box from "@mui/material/Box";
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
}: {
  item: QueueItem | PlaylistGroup;
  streamStatus: StreamStatus | null;
  connected: boolean;
  elapsed: number;
  onSkip?: () => void;
  isSkipping?: boolean;
  listeners?: LiveListeners | null;
}) {
  const { t } = useTranslation();

  return (
    <Box
      sx={{
        mb: 2,
        borderRadius: 2,
        bgcolor: "primary.main",
        color: "primary.contrastText",
        overflow: "hidden",
      }}
    >
      <Box sx={{ p: 2, pb: isPlaylistGroup(item) ? 0 : 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <Typography variant="caption" sx={{ opacity: 0.7, fontWeight: 600, letterSpacing: 1 }}>
            {isPlaylistGroup(item) ? t("stations:queue_now_playing_playlist") : t("stations:queue_now_playing")}
          </Typography>
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

      {streamStatus && (
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
