import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormGroup from "@mui/material/FormGroup";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import type { StationSong } from "@/types";

export function QueueAddDialog({
  open,
  librarySongs,
  selectedSongIds,
  isPending,
  onToggleSelect,
  onAdd,
  onClose,
}: {
  open: boolean;
  librarySongs: StationSong[] | undefined;
  selectedSongIds: Set<string>;
  isPending: boolean;
  onToggleSelect: (id: string) => void;
  onAdd: () => void;
  onClose: () => void;
}) {
  const { t } = useTranslation();

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{t("stations:queue_add_dialog_title")}</DialogTitle>
      <DialogContent>
        {!librarySongs || librarySongs.length === 0 ? (
          <Typography variant="body2" color="text.secondary" sx={{ py: 2, textAlign: "center" }}>
            {t("stations:queue_add_dialog_empty")}
          </Typography>
        ) : (
          <FormGroup>
            {librarySongs.map((song) => (
              <FormControlLabel
                key={song.song_id}
                control={
                  <Checkbox
                    size="small"
                    checked={selectedSongIds.has(song.song_id)}
                    onChange={() => onToggleSelect(song.song_id)}
                  />
                }
                label={
                  <Box sx={{ display: "flex", gap: 1, alignItems: "center" }}>
                    <Typography variant="body2">{song.title}</Typography>
                    {song.artist && (
                      <Typography variant="caption" color="text.secondary">
                        — {song.artist}
                      </Typography>
                    )}
                  </Box>
                }
              />
            ))}
          </FormGroup>
        )}
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={onClose}>{t("common:cancel")}</Button>
        <Button onClick={onAdd} variant="contained" disabled={selectedSongIds.size === 0 || isPending}>
          {isPending ? t("common:adding") : t("stations:queue_add_dialog_button", { count: selectedSongIds.size })}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
