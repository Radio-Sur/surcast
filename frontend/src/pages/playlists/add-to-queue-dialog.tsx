import QueueMusic from "@mui/icons-material/QueueMusic";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import type { Station } from "@/types";

export function AddPlaylistToQueueDialog({
  open,
  stations,
  isPending,
  onAddToQueue,
  onClose,
}: {
  open: boolean;
  stations: Station[] | undefined;
  isPending: boolean;
  onAddToQueue: (stationId: string) => void;
  onClose: () => void;
}) {
  const { t } = useTranslation();

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{t("playlists:add_to_queue_title")}</DialogTitle>
      <DialogContent>
        <Typography sx={{ mb: 2 }}>{t("playlists:add_to_queue_prompt")}</Typography>
        {!stations || stations.length === 0 ? (
          <Typography color="text.secondary">{t("playlists:no_stations")}</Typography>
        ) : (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
            {stations.map((s) => (
              <Button
                key={s.id}
                variant="outlined"
                fullWidth
                sx={{ justifyContent: "flex-start", py: 1.5, px: 2, borderRadius: 2 }}
                onClick={() => onAddToQueue(s.id)}
                disabled={isPending}
              >
                <QueueMusic sx={{ mr: 1.5 }} />
                <Box sx={{ textAlign: "left" }}>
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>
                    {s.name}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {s.description || t("common:no_description")}
                  </Typography>
                </Box>
              </Button>
            ))}
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>{t("common:cancel")}</Button>
      </DialogActions>
    </Dialog>
  );
}
