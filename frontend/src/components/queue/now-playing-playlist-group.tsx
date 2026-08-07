import PlaylistPlay from "@mui/icons-material/PlaylistPlay";
import SkipNext from "@mui/icons-material/SkipNext";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import type { PlaylistGroup } from "@/types";
import { fmt } from "./";

export function NowPlayingPlaylistGroup({
  group,
  currentSong,
  connected,
  onSkip,
  isSkipping,
}: {
  group: PlaylistGroup;
  currentSong: { title: string; artist?: string | null; album?: string | null } | null;
  connected: boolean;
  onSkip?: () => void;
  isSkipping?: boolean;
}) {
  const { t } = useTranslation();

  return (
    <>
      <Box
        sx={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          mt: 1.5,
        }}
      >
        <Box>
          <Typography variant="h6" sx={{ fontWeight: 700, lineHeight: 1.2 }}>
            {group.playlist_name}
          </Typography>
          <Typography variant="caption" sx={{ opacity: 0.7 }}>
            {t("stations:playlist_group_total", {
              count: group.songs.length,
              duration: fmt(group.total_duration),
            })}
          </Typography>
        </Box>
        <PlaylistPlay sx={{ fontSize: 32, opacity: 0.4 }} />
      </Box>

      {currentSong && (
        <Box
          sx={{
            mt: 2,
            pt: 1.5,
            borderTop: "1px solid",
            borderColor: "rgba(255,255,255,0.2)",
          }}
        >
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
            }}
          >
            <Box sx={{ flex: 1, minWidth: 0 }}>
              <Typography variant="caption" sx={{ opacity: 0.6, fontSize: 11 }}>
                {t("stations:queue_current_track")}
              </Typography>
              <Typography variant="body1" sx={{ fontWeight: 600, mt: 0.25 }}>
                {currentSong.title}
              </Typography>
              <Typography variant="body2" sx={{ opacity: 0.8 }}>
                {currentSong.artist || t("common:unknown_artist")}
                {currentSong.album ? ` · ${currentSong.album}` : ""}
              </Typography>
              <Typography variant="caption" sx={{ opacity: 0.6, mt: 0.5, mb: 0.25, display: "block" }}>
                {t("stations:queue_song_of", {
                  current: (group.current_song_index ?? 0) + 1,
                  total: group.songs.length,
                })}
              </Typography>
            </Box>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, ml: 1 }}>
              <Box
                sx={{
                  width: 8,
                  height: 8,
                  borderRadius: "50%",
                  bgcolor: connected ? "success.light" : "error.light",
                }}
              />
              <IconButton size="small" onClick={() => onSkip?.()} disabled={isSkipping} sx={{ color: "inherit" }}>
                {isSkipping ? <CircularProgress size={16} sx={{ color: "inherit" }} /> : <SkipNext />}
              </IconButton>
            </Box>
          </Box>
        </Box>
      )}
    </>
  );
}
