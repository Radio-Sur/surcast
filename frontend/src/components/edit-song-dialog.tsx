import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

interface EditSongData {
  id: string;
  title: string;
  artist: string;
  album: string;
}

export function EditSongDialog({
  song,
  isPending,
  onSave,
  onClose,
}: {
  song: EditSongData | null;
  isPending: boolean;
  onSave: (data: EditSongData) => void;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const [editData, setEditData] = useState<EditSongData | null>(null);

  useEffect(() => {
    setEditData(song);
  }, [song]);

  return (
    <Dialog open={!!song} onClose={onClose} maxWidth="sm" fullWidth slotProps={{ paper: { sx: { borderRadius: 3 } } }}>
      <DialogTitle sx={{ px: 3, pt: 3 }}>{t("songs:edit_title")}</DialogTitle>
      <DialogContent
        sx={{
          px: 3,
          pt: 4,
          pb: 3,
          display: "flex",
          flexDirection: "column",
          gap: 3,
        }}
      >
        <TextField
          label={t("songs:title_label")}
          value={editData?.title ?? ""}
          sx={{ mt: 1 }}
          onChange={(e) => setEditData((s) => (s ? { ...s, title: e.target.value } : null))}
          fullWidth
        />
        <TextField
          label={t("songs:artist_label")}
          value={editData?.artist ?? ""}
          onChange={(e) => setEditData((s) => (s ? { ...s, artist: e.target.value } : null))}
          fullWidth
        />
        <TextField
          label={t("songs:album_label")}
          value={editData?.album ?? ""}
          onChange={(e) => setEditData((s) => (s ? { ...s, album: e.target.value } : null))}
          fullWidth
        />
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button onClick={onClose}>{t("common:cancel")}</Button>
        <Button
          onClick={() => {
            if (editData) {
              onSave(editData);
              onClose();
            }
          }}
          variant="contained"
          disabled={isPending}
        >
          {isPending ? t("common:saving") : t("common:save")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
