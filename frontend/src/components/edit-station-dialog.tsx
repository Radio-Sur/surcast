import Alert from "@mui/material/Alert";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { ErrorDetails } from "@/components/error-details";
import { useAuth } from "@/hooks/use-auth";
import { useUpdateStation } from "@/hooks/use-stations";
import { type HttpErrorInfo, isHttpError } from "@/lib/is-http-error";
import type { Station } from "@/types";

export function EditStationDialog({
  open,
  station,
  onClose,
}: {
  open: boolean;
  station: Station | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const isDemo = useAuth().user?.role === "demo";
  const updateStation = useUpdateStation();
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [streamUrl, setStreamUrl] = useState("");
  const [error, setError] = useState<HttpErrorInfo | null>(null);
  const wasOpen = useRef(false);

  useEffect(() => {
    // Populate only on the closed -> open transition. A background refetch of
    // the station must not overwrite edits in progress.
    if (open && !wasOpen.current && station) {
      setName(station.name);
      setDescription(station.description);
      setStreamUrl(station.stream_url ?? "");
      setError(null);
    }
    wasOpen.current = open;
  }, [open, station]);

  const handleClose = () => {
    setError(null);
    onClose();
  };

  const handleSubmit = async () => {
    if (!station || isDemo) return;
    setError(null);

    try {
      await updateStation.mutateAsync({
        id: station.id,
        data: {
          name,
          description: description || undefined,
          stream_url: streamUrl || undefined,
        },
      });
      handleClose();
    } catch (err: unknown) {
      const info = isHttpError(err);
      setError({ ...info, message: info.message || t("errors:station_update") });
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
      <DialogTitle sx={{ px: 3, pt: 3, pb: 0 }}>{t("stations:edit_title")}</DialogTitle>
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
        {isDemo && <Alert severity="info">{t("errors:codes.DEMO_STATION_READONLY")}</Alert>}
        {error && <ErrorDetails title={error.message} details={error} onClose={() => setError(null)} />}

        <TextField
          label={t("stations:name_label")}
          value={name}
          sx={{ mt: 2 }}
          onChange={(e) => setName(e.target.value)}
          disabled={isDemo}
          required
          fullWidth
        />
        <TextField
          label={t("stations:description_label")}
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          disabled={isDemo}
          fullWidth
          multiline
          rows={3}
        />
        <TextField
          label={t("stations:mount_label")}
          value={streamUrl}
          onChange={(e) => setStreamUrl(e.target.value)}
          placeholder={t("stations:mount_placeholder_edit")}
          helperText={t("stations:mount_helper_edit")}
          disabled={isDemo}
          fullWidth
        />
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button onClick={handleClose}>{t("common:cancel")}</Button>
        <Button onClick={handleSubmit} variant="contained" disabled={isDemo || !station || updateStation.isPending}>
          {updateStation.isPending ? t("common:saving") : t("stations:save_changes")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
