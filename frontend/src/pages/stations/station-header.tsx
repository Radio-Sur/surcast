import ArrowBack from "@mui/icons-material/ArrowBack";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import { useAuth } from "@/hooks/use-auth";
import type { Station } from "@/types";

export function StationHeader({
  station,
  playing,
  operation,
  onBack,
  onToggle,
  onRestart,
  onEdit,
}: {
  station: Station;
  playing: boolean;
  operation?: "starting" | "stopping" | "restarting" | null;
  onBack: () => void;
  onToggle: () => void;
  onRestart: () => void;
  onEdit: () => void;
}) {
  const { t } = useTranslation();
  const busy = !!operation;
  const isDemo = useAuth().user?.role === "demo";

  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, flexWrap: "wrap" }}>
      <Button onClick={onBack} aria-label={t("common:go_back")} sx={{ minWidth: 40, p: 1 }} disabled={busy}>
        <ArrowBack />
      </Button>
      <Box sx={{ flex: { xs: "1 1 calc(100% - 80px)", md: 1 }, minWidth: 0 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, flexWrap: "wrap" }}>
          <Typography variant="h4" sx={{ lineHeight: 1.2, overflowWrap: "anywhere" }}>
            {station.name}
          </Typography>
          <Chip
            label={
              operation
                ? t(`stations:stream_${operation}`)
                : playing
                  ? t("stations:stream_status_live")
                  : t("stations:stream_status_stopped")
            }
            size="small"
            variant={playing ? "filled" : "outlined"}
            sx={
              playing
                ? { bgcolor: "success.main", color: "success.contrastText", fontWeight: 700, alignSelf: "center" }
                : { alignSelf: "center" }
            }
          />
        </Box>
        <Typography variant="body2" color="text.secondary" sx={{ mt: 0.25 }}>
          {station.description || t("common:no_description")}
        </Typography>
      </Box>
      <Button
        variant="outlined"
        size="small"
        color={playing ? "error" : "success"}
        onClick={onToggle}
        disabled={busy}
        loading={operation === "starting" || operation === "stopping"}
      >
        {operation && operation !== "restarting"
          ? t(`stations:stream_${operation}`)
          : playing
            ? t("common:stop")
            : t("common:start")}
      </Button>
      <Button variant="outlined" size="small" onClick={onRestart} disabled={busy} loading={operation === "restarting"}>
        {operation === "restarting" ? t("stations:stream_restarting") : t("common:restart")}
      </Button>
      <Button variant="outlined" size="small" onClick={onEdit} disabled={busy || isDemo}>
        {t("common:edit")}
      </Button>
    </Box>
  );
}
