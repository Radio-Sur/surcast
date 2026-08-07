import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import { useTranslation } from "react-i18next";
import { PasswordField } from "./password-field";

export function IcecastExternalForm({
  externalUrl,
  sourcePassword,
  adminPassword,
  onExternalUrlChange,
  onSourcePasswordChange,
  onAdminPasswordChange,
}: {
  externalUrl: string;
  sourcePassword: string;
  adminPassword: string;
  onExternalUrlChange: (v: string) => void;
  onSourcePasswordChange: (v: string) => void;
  onAdminPasswordChange: (v: string) => void;
}) {
  const { t } = useTranslation("settings");
  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 2, maxWidth: 400 }}>
      <TextField
        label={t("icecast_external_url")}
        placeholder={t("external_url_placeholder")}
        value={externalUrl}
        onChange={(e) => onExternalUrlChange(e.target.value)}
        size="small"
      />
      <PasswordField label={t("icecast_source_password")} value={sourcePassword} onChange={onSourcePasswordChange} />
      <PasswordField label={t("icecast_admin_password")} value={adminPassword} onChange={onAdminPasswordChange} />
    </Box>
  );
}
