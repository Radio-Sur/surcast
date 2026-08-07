import Delete from "@mui/icons-material/Delete";
import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import MoreVert from "@mui/icons-material/MoreVert";
import PlaylistPlay from "@mui/icons-material/PlaylistPlay";
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
import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import type { PlaylistGroup } from "@/types";
import { fmt, GripDots } from "./";
import { PlaylistGroupSongs } from "./playlist-group-songs";

const _playlistOpenCache = new Map<string, boolean>();

export function PlaylistGroupCard({
  group,
  dimmed,
  selected,
  onToggleSelect,
  selectedSongIds,
  onToggleSelectSong,
  onDeleteSong,
  dragHandleProps,
  onReorderInGroup,
  onRemovePlaylist,
  onMoveToTop,
  playlistNumber,
  onReAddSong,
  endsAt,
  renderActions,
}: {
  group: PlaylistGroup;
  dimmed?: boolean;
  selected?: boolean;
  onToggleSelect?: () => void;
  selectedSongIds?: Set<string>;
  onToggleSelectSong?: (id: string) => void;
  onDeleteSong?: (itemId: string) => void;
  dragHandleProps?: Record<string, unknown>;
  onReorderInGroup?: (itemIds: string[]) => void;
  onRemovePlaylist?: () => void;
  onMoveToTop?: () => void;
  playlistNumber?: number;
  onReAddSong?: (itemId: string) => void;
  endsAt?: string | null;
  renderActions?: (closeMenu: () => void) => React.ReactNode;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(() => _playlistOpenCache.get(group.playlist_id) ?? false);
  const toggleOpen = useCallback(() => {
    setOpen((prev) => {
      const next = !prev;
      _playlistOpenCache.set(group.playlist_id, next);
      return next;
    });
  }, [group.playlist_id]);
  const [menuAnchor, setMenuAnchor] = useState<HTMLElement | null>(null);
  const [deleteOpen, setDeleteOpen] = useState(false);

  return (
    <Box
      data-group-id={group.playlist_id}
      sx={{
        border: 1,
        borderColor: "divider",
        borderRadius: 2,
        opacity: dimmed ? 0.5 : 1,
        overflow: "hidden",
      }}
    >
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          gap: 1,
          p: "6px 10px",
          "&:hover": { bgcolor: "action.hover" },
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
        {dragHandleProps ? (
          <Box
            {...dragHandleProps}
            sx={{
              cursor: "grab",
              touchAction: "none",
              display: "flex",
              justifyContent: "center",
              width: 32,
              color: "text.secondary",
              "&:hover": { color: "text.primary" },
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <GripDots />
          </Box>
        ) : null}
        {playlistNumber !== undefined && (
          <Typography
            variant="body2"
            sx={{
              minWidth: 24,
              textAlign: "right",
              color: "text.secondary",
            }}
          >
            {playlistNumber}
          </Typography>
        )}
        <Box
          sx={{
            flex: 1,
            minWidth: 0,
            ml: 3,
            display: "flex",
            alignItems: "center",
            gap: 1.5,
            cursor: "pointer",
          }}
          onClick={(e) => {
            e.stopPropagation();
            toggleOpen();
          }}
        >
          <Box
            sx={{
              width: 32,
              display: "flex",
              justifyContent: "center",
              flexShrink: 0,
              ml: 0.5,
            }}
          >
            <PlaylistPlay fontSize="small" sx={{ color: "primary.main" }} />
          </Box>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="body2" noWrap sx={{ fontWeight: 600 }}>
              {group.playlist_name}
            </Typography>
            <Typography variant="caption" noWrap color="text.secondary">
              {t("stations:playlist_group_info", { count: group.songs.length, duration: fmt(group.total_duration) })}
              {endsAt ? ` · ${t("stations:playlist_group_ends_at", { time: endsAt })}` : ""}
            </Typography>
          </Box>
        </Box>
        <IconButton
          size="small"
          onClick={(e) => {
            e.stopPropagation();
            toggleOpen();
          }}
          sx={{ color: "text.secondary", mr: "-4px" }}
        >
          {open ? <ExpandLess fontSize="small" /> : <ExpandMore fontSize="small" />}
        </IconButton>
        {!dimmed && (
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
              {renderActions?.(() => setMenuAnchor(null))}
              <MenuItem
                onClick={(e) => {
                  e.stopPropagation();
                  setMenuAnchor(null);
                  setDeleteOpen(true);
                }}
              >
                <Delete fontSize="small" sx={{ mr: 1 }} /> {t("common:delete")}
              </MenuItem>
              {onMoveToTop && (
                <MenuItem
                  onClick={(e) => {
                    e.stopPropagation();
                    setMenuAnchor(null);
                    onMoveToTop();
                  }}
                >
                  {t("stations:move_to_top")}
                </MenuItem>
              )}
            </Menu>
            <Dialog open={deleteOpen} onClose={() => setDeleteOpen(false)}>
              <DialogTitle>{t("stations:queue_playlist_group_delete_title")}</DialogTitle>
              <DialogContent>
                <DialogContentText>
                  {t("stations:queue_playlist_group_delete_message", { name: group.playlist_name })}
                </DialogContentText>
              </DialogContent>
              <DialogActions>
                <Button onClick={() => setDeleteOpen(false)}>{t("common:cancel")}</Button>
                <Button
                  onClick={() => {
                    setDeleteOpen(false);
                    onRemovePlaylist?.();
                  }}
                  color="error"
                >
                  {t("common:delete")}
                </Button>
              </DialogActions>
            </Dialog>
          </>
        )}
      </Box>

      <PlaylistGroupSongs
        group={group}
        open={open}
        dimmed={dimmed}
        selectedSongIds={selectedSongIds}
        onToggleSelectSong={onToggleSelectSong}
        onDeleteSong={onDeleteSong}
        onReorderInGroup={onReorderInGroup}
        onReAddSong={onReAddSong}
      />
    </Box>
  );
}
