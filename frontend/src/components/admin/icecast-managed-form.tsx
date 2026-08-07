import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import { useTranslation } from "react-i18next";
import { PasswordField } from "./password-field";

export function IcecastManagedForm({
  port,
  sourcePassword,
  adminUser,
  adminPassword,
  onPortChange,
  onSourcePasswordChange,
  onAdminUserChange,
  onAdminPasswordChange,
}: {
  port: number;
  sourcePassword: string;
  adminUser: string;
  adminPassword: string;
  onPortChange: (v: number) => void;
  onSourcePasswordChange: (v: string) => void;
  onAdminUserChange: (v: string) => void;
  onAdminPasswordChange: (v: string) => void;
}) {
  const { t } = useTranslation("settings");
  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 2, maxWidth: 400 }}>
      <TextField
        label={t("icecast_port")}
        type="number"
        value={port}
        onChange={(e) => onPortChange(Number(e.target.value))}
        size="small"
      />
      <PasswordField label={t("icecast_source_password")} value={sourcePassword} onChange={onSourcePasswordChange} />
      <TextField
        label={t("icecast_admin_user")}
        value={adminUser}
        onChange={(e) => onAdminUserChange(e.target.value)}
        size="small"
      />
      <PasswordField label={t("icecast_admin_password")} value={adminPassword} onChange={onAdminPasswordChange} />
    </Box>
  );
}
