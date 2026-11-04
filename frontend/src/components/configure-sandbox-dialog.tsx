import Alert from "@mui/material/Alert";
import Autocomplete from "@mui/material/Autocomplete";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { usePlaylists } from "@/hooks/use-playlists";
import { useStations } from "@/hooks/use-stations";
import { httpClient } from "@/lib/api";
import { reportHttpError } from "@/lib/report-error";
import { useSnackbar } from "@/providers/snackbar-provider";

interface SandboxStatus {
  ready: boolean;
  station?: string;
  songs?: number;
  playlists?: number;
}

export function ConfigureSandboxDialog({ userId, onClose }: { userId: string; onClose: () => void }) {
  const { t } = useTranslation();
  const { showError } = useSnackbar();
  const { data: stations } = useStations();
  const { data: playlists } = usePlaylists();
  const [station, setStation] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [confirmReset, setConfirmReset] = useState(false);
  const status = useQuery({
    queryKey: ["sandbox", userId],
    queryFn: () => httpClient.get<SandboxStatus>(`/users/${userId}/sandbox`),
  });
  const save = useMutation({
    mutationFn: () => httpClient.post(`/users/${userId}/sandbox`, { station_id: station, playlist_ids: selected }),
    onSuccess: () => {
      status.refetch();
      setConfirmReset(false);
    },
    onError: (error) => reportHttpError(showError, error, t("users:sandbox_failed")),
  });
  const reset = useMutation({
    mutationFn: () => httpClient.post(`/users/${userId}/sandbox/reset`),
    onSuccess: () => {
      status.refetch();
      setConfirmReset(false);
    },
    onError: (error) => reportHttpError(showError, error, t("users:sandbox_failed")),
  });
  const busy = save.isPending || reset.isPending;
  return (
    <Dialog
      open
      onClose={busy ? undefined : onClose}
      maxWidth="sm"
      fullWidth
      slotProps={{ paper: { sx: { borderRadius: 3 } } }}
    >
      <DialogTitle sx={{ px: 3, pt: 3 }}>{t("users:sandbox_title")}</DialogTitle>
      <DialogContent sx={{ display: "flex", flexDirection: "column", gap: 2.5 }}>
        <Typography variant="body2" color="text.secondary">
          {t("users:sandbox_help")}
        </Typography>
        {status.isError && <Alert severity="error">{t("users:sandbox_failed")}</Alert>}
        {status.data?.ready && (
          <Alert severity="success">
            {t("users:sandbox_ready", { station: status.data.station, count: status.data.songs })}
          </Alert>
        )}
        {reset.isSuccess && <Alert severity="success">{t("users:sandbox_saved")}</Alert>}
        <TextField
          select
          label={t("users:sandbox_source")}
          value={station}
          onChange={(event) => setStation(event.target.value)}
          disabled={busy}
          slotProps={{ select: { native: true }, inputLabel: { shrink: true } }}
          sx={{ mt: 1 }}
        >
          <option value="">{t("users:sandbox_choose")}</option>
          {stations?.map((item) => (
            <option key={item.id} value={item.id}>
              {item.name}
            </option>
          ))}
        </TextField>
        <Autocomplete
          multiple
          disableCloseOnSelect
          filterSelectedOptions
          disabled={busy}
          options={playlists ?? []}
          value={(playlists ?? []).filter((playlist) => selected.includes(playlist.id))}
          getOptionLabel={(playlist) => playlist.name}
          getOptionKey={(playlist) => playlist.id}
          isOptionEqualToValue={(option, value) => option.id === value.id}
          onChange={(_, values) => setSelected(values.map((playlist) => playlist.id))}
          renderInput={(params) => <TextField {...params} label={t("users:sandbox_playlists")} />}
        />
        {status.data?.ready && <Alert severity="warning">{t("users:sandbox_replace_warning")}</Alert>}
        {status.data?.ready && (
          <Box>
            {confirmReset ? (
              <Alert
                severity="warning"
                action={
                  <Button disabled={busy} onClick={() => reset.mutate()} color="inherit">
                    {t("users:sandbox_confirm")}
                  </Button>
                }
              >
                {t("users:sandbox_reset_confirm")}
              </Alert>
            ) : (
              <Button disabled={busy} color="warning" onClick={() => setConfirmReset(true)}>
                {t("users:sandbox_reset")}
              </Button>
            )}
          </Box>
        )}
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button disabled={busy} onClick={onClose}>
          {t("common:close")}
        </Button>
        <Button variant="contained" disabled={!station || busy} loading={save.isPending} onClick={() => save.mutate()}>
          {t("users:sandbox_prepare")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
