import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";

export function BufferingInfoTable({ prebufferBytes }: { prebufferBytes: number }) {
  const { t } = useTranslation();

  return (
    <Box sx={{ mt: 3, p: 2, bgcolor: "action.hover", borderRadius: 2 }}>
      <Typography variant="subtitle2" gutterBottom>
        {t("stations:buffering_title")}
      </Typography>
      {[64000, 128000, 192000, 320000].map((bps) => {
        const secs = (prebufferBytes * 8) / bps;
        return (
          <Box key={bps} sx={{ display: "flex", justifyContent: "space-between", py: 0.25 }}>
            <Typography variant="body2" color="text.secondary">
              {t("stations:bitrate_label", { rate: (bps / 1000).toFixed(0) })}
            </Typography>
            <Typography variant="body2">
              {secs < 1
                ? t("stations:buffering_ms", { time: (secs * 1000).toFixed(0) })
                : t("stations:buffering_s", { time: secs.toFixed(1) })}
            </Typography>
          </Box>
        );
      })}
    </Box>
  );
}
