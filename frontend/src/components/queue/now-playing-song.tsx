import SkipNext from "@mui/icons-material/SkipNext";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import type { QueueItem } from "@/types";

export function NowPlayingSong({
  song,
  connected,
  onSkip,
  isSkipping,
}: {
  song: QueueItem;
  connected: boolean;
  onSkip?: () => void;
  isSkipping?: boolean;
}) {
  const { t } = useTranslation();

  return (
    <Box sx={{ mt: 1.5 }}>
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography variant="body1" sx={{ fontWeight: 600 }}>
            {song.title}
          </Typography>
          <Typography variant="body2" sx={{ opacity: 0.8 }}>
            {song.artist || t("common:unknown_artist")}
            {song.album ? ` · ${song.album}` : ""}
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
  );
}
