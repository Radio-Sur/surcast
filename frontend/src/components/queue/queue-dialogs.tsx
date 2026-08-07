import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import { useTranslation } from "react-i18next";

interface QueueDialogsProps {
  bulkDeleteOpen: boolean;
  selectedCount: number;
  onClose: () => void;
  onConfirmDelete: () => void;
}

export function QueueDialogs({ bulkDeleteOpen, selectedCount, onClose, onConfirmDelete }: QueueDialogsProps) {
  const { t } = useTranslation();

  return (
    <Dialog open={bulkDeleteOpen} onClose={onClose}>
      <DialogTitle>{t("stations:queue_delete_title", { count: selectedCount })}</DialogTitle>
      <DialogContent>
        <DialogContentText>{t("stations:queue_delete_message", { count: selectedCount })}</DialogContentText>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>{t("common:cancel")}</Button>
        <Button onClick={onConfirmDelete} color="error">
          {t("common:delete")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
