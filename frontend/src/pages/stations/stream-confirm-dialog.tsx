import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import { useTranslation } from "react-i18next";

export function StreamConfirmDialog({
  action,
  isPending,
  onConfirm,
  onClose,
}: {
  action: "stop" | "restart" | null;
  isPending: boolean;
  onConfirm: () => void;
  onClose: () => void;
}) {
  const { t } = useTranslation();

  return (
    <Dialog open={action !== null} onClose={isPending ? undefined : onClose}>
      <DialogTitle>
        {action === "stop" ? t("stations:stream_stop_title") : t("stations:stream_restart_title")}
      </DialogTitle>
      <DialogContent>
        <DialogContentText>
          {action === "stop" ? t("stations:stream_stop_message") : t("stations:stream_restart_message")}
        </DialogContentText>
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={onClose} disabled={isPending}>
          {t("common:cancel")}
        </Button>
        <Button
          variant="contained"
          color={action === "stop" ? "error" : "primary"}
          onClick={onConfirm}
          disabled={isPending}
          loading={isPending}
        >
          {isPending
            ? t(`stations:stream_${action === "stop" ? "stopping" : "restarting"}`)
            : action === "stop"
              ? t("common:stop")
              : t("common:restart")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
