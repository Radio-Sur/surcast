import ArrowBack from "@mui/icons-material/ArrowBack";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import type { Station } from "@/types";

export function StationHeader({
  station,
  playing,
  onBack,
  onToggle,
  onRestart,
  onEdit,
}: {
  station: Station;
  playing: boolean;
  onBack: () => void;
  onToggle: () => void;
  onRestart: () => void;
  onEdit: () => void;
}) {
  const { t } = useTranslation();

  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
      <Button onClick={onBack} sx={{ minWidth: 40, p: 1 }}>
        <ArrowBack />
      </Button>
      <Box sx={{ flex: 1 }}>
        <Typography variant="h4">{station.name}</Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mt: 0.25 }}>
          {station.description || t("common:no_description")}
        </Typography>
      </Box>
      <Button variant="outlined" size="small" color={playing ? "error" : "success"} onClick={onToggle}>
        {playing ? t("common:stop") : t("common:start")}
      </Button>
      <Button variant="outlined" size="small" onClick={onRestart}>
        {t("common:restart")}
      </Button>
      <Button variant="outlined" size="small" onClick={onEdit}>
        {t("common:edit")}
      </Button>
    </Box>
  );
}
