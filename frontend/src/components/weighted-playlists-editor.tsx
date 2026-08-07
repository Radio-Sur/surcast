import { Add, Delete } from "@mui/icons-material";
import {
  Box,
  Button,
  Chip,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { AutoFillPlaylistEntry, Playlist } from "@/types";

export function WeightedPlaylistsEditor({
  playlists,
  entries,
  onAdd,
  onUpdateWeight,
  onRemove,
  isAdding,
}: {
  playlists: Playlist[];
  entries: AutoFillPlaylistEntry[];
  onAdd: (playlistId: string, weight: number) => void;
  onUpdateWeight: (id: string, weight: number) => void;
  onRemove: (id: string) => void;
  isAdding: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [newPlaylistId, setNewPlaylistId] = useState("");
  const [newWeight, setNewWeight] = useState(1);
  const { t } = useTranslation();
  const [editingWeight, setEditingWeight] = useState<{ id: string; value: number } | null>(null);

  const handleAdd = () => {
    if (!newPlaylistId) return;
    onAdd(newPlaylistId, newWeight);
    setOpen(false);
    setNewPlaylistId("");
    setNewWeight(1);
  };

  return (
    <>
      <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", mb: 1 }}>
        <Typography variant="subtitle2">{t("stations:weighted_title")}</Typography>
        <Button size="small" startIcon={<Add />} onClick={() => setOpen(!open)}>
          {open ? t("common:cancel") : t("common:add")}
        </Button>
      </Box>

      {open && (
        <Box sx={{ display: "flex", gap: 1, alignItems: "center", mb: 2 }}>
          <FormControl size="small" sx={{ flex: 1 }}>
            <InputLabel>{t("stations:weighted_playlist")}</InputLabel>
            <Select
              value={newPlaylistId}
              label={t("stations:weighted_playlist")}
              onChange={(e) => setNewPlaylistId(e.target.value)}
            >
              {playlists.map((p) => (
                <MenuItem key={p.id} value={p.id}>
                  {p.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            size="small"
            type="number"
            label={t("stations:weighted_weight")}
            value={newWeight}
            onChange={(e) => setNewWeight(Number(e.target.value))}
            sx={{ width: 100 }}
            slotProps={{ htmlInput: { min: 1 } }}
          />
          <Button size="small" variant="contained" onClick={handleAdd} disabled={!newPlaylistId || isAdding}>
            {t("common:add")}
          </Button>
        </Box>
      )}

      {entries.length > 0 ? (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1, mb: 2 }}>
          {entries.map((wp) => (
            <Box
              key={wp.id}
              sx={{
                display: "flex",
                alignItems: "center",
                gap: 1,
                p: 1,
                borderRadius: 1,
                bgcolor: "action.hover",
              }}
            >
              <Chip label={wp.playlist_name} size="small" color="primary" variant="outlined" />
              {editingWeight?.id === wp.id ? (
                <TextField
                  size="small"
                  type="number"
                  value={editingWeight.value}
                  onChange={(e) => setEditingWeight({ id: wp.id, value: Number(e.target.value) })}
                  onBlur={() => {
                    onUpdateWeight(wp.id, editingWeight.value);
                    setEditingWeight(null);
                  }}
                  sx={{ width: 80 }}
                  slotProps={{ htmlInput: { min: 1, style: { textAlign: "center" } } }}
                />
              ) : (
                <Typography
                  variant="body2"
                  sx={{ cursor: "pointer", minWidth: 40, textAlign: "center" }}
                  onClick={() => setEditingWeight({ id: wp.id, value: wp.weight })}
                >
                  {t("stations:weighted_display", { weight: wp.weight })}
                </Typography>
              )}
              <Box sx={{ flex: 1 }} />
              <Tooltip title={t("stations:weighted_remove")}>
                <IconButton size="small" color="error" onClick={() => onRemove(wp.id)}>
                  <Delete fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
          ))}
        </Box>
      ) : (
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          {t("stations:weighted_empty")}
        </Typography>
      )}
    </>
  );
}
