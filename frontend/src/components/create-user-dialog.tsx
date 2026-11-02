import Alert from "@mui/material/Alert";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useCreateUser } from "@/hooks/use-users";
import { reportHttpError } from "@/lib/report-error";
import { useSnackbar } from "@/providers/snackbar-provider";

export function CreateUserDialog({ onClose }: { onClose: () => void }) {
  const { t } = useTranslation();
  const create = useCreateUser();
  const { showError } = useSnackbar();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<"admin" | "demo">("admin");
  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (create.isPending) return;
    try {
      await create.mutateAsync({ username, password, role });
      onClose();
    } catch (error) {
      reportHttpError(showError, error, t("users:create_failed"));
    }
  };
  return (
    <Dialog
      open
      onClose={create.isPending ? undefined : onClose}
      maxWidth="sm"
      fullWidth
      slotProps={{ paper: { sx: { borderRadius: 3 } } }}
    >
      <form onSubmit={submit}>
        {/* Left padding matches the outlined inputs' text inset (24px content padding + 14px input padding). */}
        <DialogTitle sx={{ pl: "38px", pr: 3, pt: 3, pb: 0, mb: 1 }}>{t("users:add")}</DialogTitle>
        <DialogContent sx={{ px: 3, pt: 0, pb: 2, display: "flex", flexDirection: "column", gap: 2.5 }}>
          <TextField
            required
            label={t("users:table_username")}
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="off"
            sx={{ mt: 2 }}
          />
          <TextField
            required
            label={t("users:password")}
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="new-password"
            slotProps={{ htmlInput: { minLength: 8, maxLength: 72 } }}
          />
          <TextField
            select
            label={t("users:role_label")}
            value={role}
            onChange={(e) => setRole(e.target.value as "admin" | "demo")}
            slotProps={{ select: { native: true } }}
          >
            <option value="admin">{t("users:role_admin")}</option>
            <option value="demo">{t("users:role_demo")}</option>
          </TextField>
          {role === "demo" && <Alert severity="warning">{t("users:demo_not_ready")}</Alert>}
        </DialogContent>
        <DialogActions sx={{ px: 3, pt: 2, pb: 3 }}>
          <Button disabled={create.isPending} onClick={onClose}>
            {t("common:cancel")}
          </Button>
          <Button type="submit" variant="contained" loading={create.isPending}>
            {t("common:create")}
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
}
