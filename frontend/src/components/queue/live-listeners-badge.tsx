import Headphones from "@mui/icons-material/Headphones";
import Box from "@mui/material/Box";
import Skeleton from "@mui/material/Skeleton";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import type { LiveListeners } from "@/types";

export function LiveListenersBadge({ listeners }: { listeners: LiveListeners | null }) {
  const { t } = useTranslation();
  const count = listeners?.online ? listeners.listeners : 0;

  return (
    <Box sx={{ display: "inline-flex", alignItems: "center", gap: 0.75 }}>
      <Headphones sx={{ fontSize: 16, opacity: listeners ? 1 : 0.4 }} />
      <Typography variant="body2" sx={{ fontWeight: 600, lineHeight: 1 }}>
        {listeners ? count : <Skeleton width={18} height={16} />}
      </Typography>
      <Typography variant="caption" sx={{ opacity: 0.7 }}>
        {listeners ? t("stations:listeners_count", { count }) : ""}
      </Typography>
    </Box>
  );
}
