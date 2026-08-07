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
import { useState } from "react";
import { SongCover } from "@/components/song-cover";
import type { QueueItem } from "@/types";
import { fmt, GripDots } from "./";

export function NestedSongRow({
  song,
  index,
  selected,
  onToggleSelect,
  onDelete,
}: {
  song: QueueItem;
  index: number;
  selected?: boolean;
  onToggleSelect?: () => void;
  onDelete?: (songId: string) => void;
}) {
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
        "&:hover": { bgcolor: "action.focus" },
      }}
    >
      {onToggleSelect && (
        <Checkbox size="small" checked={!!selected} onChange={onToggleSelect} onClick={(e) => e.stopPropagation()} />
      )}
      <Box
        {...attributes}
        {...listeners}
        sx={{
          cursor: "grab",
          touchAction: "none",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          width: 32,
          color: "text.secondary",
          "&:hover": { color: "text.primary" },
        }}
      >
        <GripDots />
      </Box>
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
          <Typography variant="body2" noWrap>
            {song.title}
          </Typography>
          <Typography variant="caption" noWrap color="text.secondary">
            {song.artist || "Unknown artist"}
            {song.album ? ` · ${song.album}` : ""}
          </Typography>
        </Box>
      </Box>
      <Typography variant="caption" color="text.secondary">
        {song.duration > 0 ? fmt(song.duration) : "--:--"}
      </Typography>
      {onDelete && (
        <>
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
            <MenuItem
              onClick={(e) => {
                e.stopPropagation();
                setMenuAnchor(null);
                setDeleteOpen(true);
              }}
            >
              <Delete fontSize="small" sx={{ mr: 1 }} /> Delete
            </MenuItem>
          </Menu>
          <Dialog open={deleteOpen} onClose={() => setDeleteOpen(false)}>
            <DialogTitle>Delete song?</DialogTitle>
            <DialogContent>
              <DialogContentText>Remove &ldquo;{song.title}&rdquo; from the queue?</DialogContentText>
            </DialogContent>
            <DialogActions>
              <Button
                onClick={(e) => {
                  e.stopPropagation();
                  setDeleteOpen(false);
                }}
              >
                Cancel
              </Button>
              <Button
                onClick={(e) => {
                  e.stopPropagation();
                  setDeleteOpen(false);
                  onDelete(song.id);
                }}
                color="error"
              >
                Delete
              </Button>
            </DialogActions>
          </Dialog>
        </>
      )}
    </Box>
  );
}
