import Alert from "@mui/material/Alert";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useCreateStation } from "@/hooks/use-stations";
import { isHttpError } from "@/lib/is-http-error";

export function CreateStationDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { t } = useTranslation();
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [streamUrl, setStreamUrl] = useState("");
  const [error, setError] = useState("");
  const createStation = useCreateStation();

  const resetForm = () => {
    setName("");
    setDescription("");
    setStreamUrl("");
    setError("");
  };

  const handleClose = () => {
    resetForm();
    onClose();
  };

  const handleSubmit = async () => {
    setError("");

    try {
      await createStation.mutateAsync({
        name,
        description: description || undefined,
        stream_url: streamUrl || undefined,
      });
      handleClose();
    } catch (err: unknown) {
      setError(isHttpError(err)?.message || t("errors:station_create"));
    }
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
      slotProps={{ paper: { sx: { borderRadius: 3 } } }}
    >
      <DialogTitle sx={{ px: 3, pt: 3, pb: 0 }}>{t("stations:create_title")}</DialogTitle>
      <DialogContent
        sx={{
          px: 3,
          pt: 3,
          pb: 2,
          display: "flex",
          flexDirection: "column",
          gap: 2.5,
        }}
      >
        {error && <Alert severity="error">{error}</Alert>}

        <TextField
          label={t("stations:name_label")}
          value={name}
          sx={{ mt: 2 }}
          onChange={(e) => setName(e.target.value)}
          placeholder={t("stations:name_placeholder")}
          required
          fullWidth
        />
        <TextField
          label={t("stations:description_label")}
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          placeholder={t("stations:description_placeholder")}
          fullWidth
          multiline
          rows={3}
        />
        <TextField
          label={t("stations:mount_label")}
          value={streamUrl}
          onChange={(e) => setStreamUrl(e.target.value)}
          placeholder={t("stations:mount_placeholder")}
          helperText={t("stations:mount_helper")}
          required
          fullWidth
        />
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button onClick={handleClose}>{t("common:cancel")}</Button>
        <Button onClick={handleSubmit} variant="contained" disabled={createStation.isPending}>
          {createStation.isPending ? t("common:creating") : t("stations:create_button")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
