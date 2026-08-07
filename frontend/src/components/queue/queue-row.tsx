import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import Delete from "@mui/icons-material/Delete";
import MoreVert from "@mui/icons-material/MoreVert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import Typography from "@mui/material/Typography";
import type React from "react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { SongCover } from "@/components/song-cover";
import type { QueueItem } from "@/types";
import { fmt, GripDots } from "./";

export function QueueRow({
  song,
  index,
  selected,
  onToggleSelect,
  onDelete,
  onMoveToTop,
  renderActions,
}: {
  song: QueueItem;
  index: number;
  selected?: boolean;
  onToggleSelect?: () => void;
  onDelete: () => void;
  onMoveToTop: () => void;
  renderActions?: (closeMenu: () => void) => React.ReactNode;
}) {
  const { t } = useTranslation();
  const [menuAnchor, setMenuAnchor] = useState<HTMLElement | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);

  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: song.id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    zIndex: isDragging ? 10 : undefined,
    position: "relative" as const,
  };

  return (
    <Box
      ref={setNodeRef}
      style={style}
      sx={{
        display: "flex",
        alignItems: "center",
        gap: 1,
        pl: "10px",
        pr: "8px",
        py: "6px",
        borderRadius: 2,
        "&:hover": { bgcolor: "action.hover" },
      }}
    >
      <Checkbox
        size="small"
        checked={!!selected}
        onChange={() => onToggleSelect?.()}
        onClick={(e) => e.stopPropagation()}
      />
      <IconButton
        size="small"
        {...attributes}
        {...listeners}
        sx={{ cursor: "grab", touchAction: "none", width: 32, height: 32 }}
      >
        <Box sx={{ display: "flex", transform: "translateX(-1px)" }}>
          <GripDots />
        </Box>
      </IconButton>
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
      <IconButton
        size="small"
        onClick={(e) => {
          e.stopPropagation();
          setMenuAnchor(e.currentTarget);
        }}
        sx={{ color: "text.secondary" }}
      >
        <MoreVert fontSize="small" />
      </IconButton>
      <Menu anchorEl={menuAnchor} open={!!menuAnchor} onClose={() => setMenuAnchor(null)}>
        {renderActions?.(() => setMenuAnchor(null))}
        <MenuItem
          onClick={() => {
            setMenuAnchor(null);
            setDeleteOpen(true);
          }}
        >
          <Delete fontSize="small" sx={{ mr: 1 }} /> {t("common:delete")}
        </MenuItem>
        <MenuItem
          onClick={() => {
            setMenuAnchor(null);
            onMoveToTop();
          }}
        >
          {t("stations:move_to_top")}
        </MenuItem>
      </Menu>
      <Dialog open={deleteOpen} onClose={() => setDeleteOpen(false)}>
        <DialogTitle>{t("stations:queue_delete_song_title")}</DialogTitle>
        <DialogContent>
          <DialogContentText>{t("stations:queue_delete_song_message", { title: song.title })}</DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteOpen(false)}>{t("common:cancel")}</Button>
          <Button
            onClick={() => {
              setDeleteOpen(false);
              onDelete();
            }}
            color="error"
          >
            {t("common:delete")}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
