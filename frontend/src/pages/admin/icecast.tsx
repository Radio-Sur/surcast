import PlayArrow from "@mui/icons-material/PlayArrow";
import Refresh from "@mui/icons-material/Refresh";
import Save from "@mui/icons-material/Save";
import Stop from "@mui/icons-material/Stop";
import WifiTethering from "@mui/icons-material/WifiTethering";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import Skeleton from "@mui/material/Skeleton";
import Switch from "@mui/material/Switch";
import Typography from "@mui/material/Typography";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { IcecastExternalForm } from "@/components/admin/icecast-external-form";
import { IcecastManagedForm } from "@/components/admin/icecast-managed-form";
import {
  useIcecastStatus,
  useStartIcecast,
  useStopIcecast,
  useTestIcecast,
  useUpdateIcecast,
} from "@/hooks/use-icecast";
import { useSnackbar } from "@/providers/snackbar-provider";

export function AdminIcecastPage() {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const { data, isLoading, isError, error, refetch } = useIcecastStatus();
  const updateMutation = useUpdateIcecast();
  const startMutation = useStartIcecast();
  const stopMutation = useStopIcecast();
  const testMutation = useTestIcecast();

  const [mode, setMode] = useState("managed");
  const [enabled, setEnabled] = useState(false);
  const [port, setPort] = useState(8000);
  const [sourcePassword, setSourcePassword] = useState("");
  const [adminUser, setAdminUser] = useState("admin");
  const [adminPassword, setAdminPassword] = useState("");
  const [externalUrl, setExternalUrl] = useState("");
  const [externalSourcePw, setExternalSourcePw] = useState("");
  const [externalAdminPw, setExternalAdminPw] = useState("");

  useEffect(() => {
    if (data) {
      setMode(data.settings.mode);
      setEnabled(data.settings.enabled);
      setPort(data.settings.port);
      setSourcePassword(data.settings.source_password);
      setAdminUser(data.settings.admin_user);
      setAdminPassword(data.settings.admin_password);
      setExternalUrl(data.settings.external_url ?? "");
      setExternalSourcePw(data.settings.external_source_pw ?? "");
      setExternalAdminPw(data.settings.external_admin_pw ?? "");
    }
  }, [data]);

  const handleSave = () => {
    updateMutation.mutate(
      {
        mode,
        enabled,
        port,
        source_password: sourcePassword,
        admin_user: adminUser,
        admin_password: adminPassword,
        external_url: externalUrl || null,
        external_source_pw: externalSourcePw || null,
        external_admin_pw: externalAdminPw || null,
      },
      {
        onError: (err) => {
          console.error("Failed to save Icecast settings", err);
          showSnackbar("Failed to save Icecast settings", "error");
        },
      },
    );
  };

  if (isLoading) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Skeleton variant="text" width={200} height={40} />
        <Skeleton variant="rounded" height={400} />
      </Box>
    );
  }

  if (isError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">{error instanceof Error ? error.message : "Failed to load Icecast status"}</Alert>
      </Box>
    );
  }

  const pending = updateMutation.isPending || startMutation.isPending || stopMutation.isPending;

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <Box>
        <Typography variant="h4">{t("settings:icecast_title")}</Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
          {t("settings:icecast_subtitle")}
        </Typography>
      </Box>

      {data && (
        <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
          <Chip
            label={data.running ? t("settings:icecast_running") : t("settings:icecast_stopped")}
            color={data.running ? "success" : "error"}
            variant="outlined"
          />
          <Button size="small" variant="outlined" startIcon={<Refresh />} onClick={() => refetch()}>
            {t("common:refresh")}
          </Button>
        </Box>
      )}

      {updateMutation.isSuccess && (
        <Alert severity="success" onClose={updateMutation.reset}>
          {t("settings:icecast_settings_saved")}
        </Alert>
      )}
      {updateMutation.isError && (
        <Alert severity="error" onClose={updateMutation.reset}>
          {String(updateMutation.error)}
        </Alert>
      )}
      {startMutation.isSuccess && (
        <Alert severity="success" onClose={startMutation.reset}>
          {startMutation.data?.message || t("settings:icecast_started")}
        </Alert>
      )}
      {stopMutation.isSuccess && (
        <Alert severity="info" onClose={stopMutation.reset}>
          {stopMutation.data?.message || t("settings:icecast_stopped_msg")}
        </Alert>
      )}
      {testMutation.isSuccess && (
        <Alert severity="success" onClose={testMutation.reset}>
          {testMutation.data?.message || t("settings:icecast_connection_ok")}
        </Alert>
      )}
      {testMutation.isError && (
        <Alert severity="error" onClose={testMutation.reset}>
          {String(testMutation.error)}
        </Alert>
      )}

      <Card variant="outlined" sx={{ borderRadius: 3 }}>
        <CardContent
          sx={{
            display: "flex",
            flexDirection: "column",
            gap: 3,
            p: 4,
            "&:last-child": { pb: 4 },
          }}
        >
          <FormControl size="small" sx={{ maxWidth: 300 }}>
            <InputLabel>{t("settings:icecast_mode")}</InputLabel>
            <Select value={mode} label={t("settings:icecast_mode")} onChange={(e) => setMode(e.target.value)}>
              <MenuItem value="managed">{t("settings:icecast_mode_managed")}</MenuItem>
              <MenuItem value="external">{t("settings:icecast_mode_external")}</MenuItem>
            </Select>
          </FormControl>

          <FormControlLabel
            control={<Switch checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />}
            label={t("settings:icecast_auto_start")}
          />

          {mode === "managed" ? (
            <IcecastManagedForm
              port={port}
              sourcePassword={sourcePassword}
              adminUser={adminUser}
              adminPassword={adminPassword}
              onPortChange={setPort}
              onSourcePasswordChange={setSourcePassword}
              onAdminUserChange={setAdminUser}
              onAdminPasswordChange={setAdminPassword}
            />
          ) : (
            <IcecastExternalForm
              externalUrl={externalUrl}
              sourcePassword={externalSourcePw}
              adminPassword={externalAdminPw}
              onExternalUrlChange={setExternalUrl}
              onSourcePasswordChange={setExternalSourcePw}
              onAdminPasswordChange={setExternalAdminPw}
            />
          )}
        </CardContent>
      </Card>

      <Box sx={{ display: "flex", gap: 2 }}>
        <Button variant="contained" startIcon={<Save />} onClick={handleSave} disabled={pending}>
          {pending ? t("common:saving") : t("common:save")}
        </Button>
        {mode === "managed" &&
          (data?.running ? (
            <Button
              variant="outlined"
              color="error"
              startIcon={<Stop />}
              onClick={() =>
                stopMutation.mutate(undefined, {
                  onError: (err) => {
                    console.error("Failed to stop Icecast", err);
                    showSnackbar("Failed to stop Icecast", "error");
                  },
                })
              }
              disabled={pending}
            >
              {t("common:stop")}
            </Button>
          ) : (
            <Button
              variant="outlined"
              color="success"
              startIcon={<PlayArrow />}
              onClick={() =>
                startMutation.mutate(undefined, {
                  onError: (err) => {
                    console.error("Failed to start Icecast", err);
                    showSnackbar("Failed to start Icecast", "error");
                  },
                })
              }
              disabled={pending}
            >
              {t("common:start")}
            </Button>
          ))}
        {mode === "external" && (
          <Button
            variant="outlined"
            startIcon={<WifiTethering />}
            onClick={() =>
              testMutation.mutate(undefined, {
                onError: (err) => {
                  console.error("Failed to test Icecast connection", err);
                  showSnackbar("Failed to test Icecast connection", "error");
                },
              })
            }
            disabled={testMutation.isPending}
          >
            {testMutation.isPending ? t("common:testing") : t("settings:icecast_test_connection")}
          </Button>
        )}
      </Box>
    </Box>
  );
}
