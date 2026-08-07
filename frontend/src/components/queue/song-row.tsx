import PlaylistAdd from "@mui/icons-material/PlaylistAdd";
import Box from "@mui/material/Box";
import Checkbox from "@mui/material/Checkbox";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import { SongCover } from "@/components/song-cover";
import type { QueueItem } from "@/types";
import { fmt } from "./";

export function SongRow({
  song,
  index,
  dimmed,
  selected,
  onToggleSelect,
  onReAdd,
}: {
  song: QueueItem;
  index: number;
  dimmed?: boolean;
  selected?: boolean;
  onToggleSelect?: () => void;
  onReAdd?: () => void;
}) {
  const { t } = useTranslation();
  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        gap: 1,
        p: "6px 10px",
        borderRadius: 2,
        opacity: dimmed ? 0.5 : 1,
        "&:hover": { bgcolor: dimmed ? "action.hover" : undefined },
      }}
    >
      {!dimmed && (
        <Checkbox
          size="small"
          checked={!!selected}
          onChange={() => onToggleSelect?.()}
          onClick={(e) => e.stopPropagation()}
        />
      )}
      <Typography variant="body2" sx={{ minWidth: 24, textAlign: "right", color: "text.secondary" }}>
        {index + 1}
      </Typography>
      <Box
        sx={{
          flex: 1,
          minWidth: 0,
          ml: 3,
          display: "flex",
          alignItems: "center",
          gap: 1.5,
        }}
      >
        <SongCover songId={song.song_id} hasCover={song.has_cover} size={32} autoDj={song.is_auto_dj} />
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography variant="body2" noWrap sx={{ fontWeight: 500 }}>
            {song.title}
          </Typography>
          <Typography variant="caption" noWrap color="text.secondary">
            {song.artist || t("common:unknown_artist")}
            {song.album ? ` · ${song.album}` : ""}
          </Typography>
        </Box>
      </Box>
      <Typography variant="caption" color="text.secondary">
        {song.duration > 0 ? fmt(song.duration) : t("common:duration_unknown")}
      </Typography>
      {onReAdd && (
        <IconButton
          size="small"
          onClick={(e) => {
            e.stopPropagation();
            onReAdd();
          }}
          sx={{
            color: "primary.main",
            bgcolor: "action.selected",
            "&:hover": { bgcolor: "primary.light", color: "common.white" },
          }}
          title={t("stations:queue_song_re_add")}
        >
          <PlaylistAdd fontSize="small" />
        </IconButton>
      )}
    </Box>
  );
}
