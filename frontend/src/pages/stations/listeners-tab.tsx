import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import CircularProgress from "@mui/material/CircularProgress";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Typography from "@mui/material/Typography";
import { LineChart } from "@mui/x-charts/LineChart";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { LiveListenersBadge } from "@/components/queue/live-listeners-badge";
import { useStationListenersHistory } from "@/hooks/use-listeners";
import { useLiveStation } from "@/providers/live-provider";
import type { ListenerRange } from "@/types";

const RANGES: ListenerRange[] = ["24h", "7d", "30d"];

function formatTime(value: number | Date) {
  return new Date(value).toLocaleString([], {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function ListenersTab({ stationId }: { stationId: string }) {
  const { t } = useTranslation();
  const [range, setRange] = useState<ListenerRange>("7d");
  const { listeners } = useLiveStation(stationId);
  const { data, isLoading, isError } = useStationListenersHistory(stationId, range);

  const points = data?.points ?? [];
  const dataset = points.map((p) => ({ time: new Date(p.time).getTime(), listeners: p.listeners }));

  return (
    <Card variant="outlined" sx={{ borderRadius: 3 }}>
      <CardContent sx={{ p: 4, "&:last-child": { pb: 4 } }}>
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 3 }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 3 }}>
            <Typography variant="h6">{t("stations:listeners_title")}</Typography>
            <LiveListenersBadge listeners={listeners} />
          </Box>
          <ToggleButtonGroup
            size="small"
            exclusive
            value={range}
            onChange={(_, value) => {
              if (value) setRange(value as ListenerRange);
            }}
          >
            {RANGES.map((r) => (
              <ToggleButton key={r} value={r}>
                {t(`stations:listeners_range_${r}`)}
              </ToggleButton>
            ))}
          </ToggleButtonGroup>
        </Box>

        {isLoading ? (
          <Box sx={{ display: "flex", justifyContent: "center", py: 8 }}>
            <CircularProgress />
          </Box>
        ) : isError ? (
          <Typography color="error">{t("stations:listeners_load_error")}</Typography>
        ) : dataset.length === 0 ? (
          <Typography color="text.secondary">{t("stations:listeners_empty")}</Typography>
        ) : (
          <LineChart
            height={320}
            dataset={dataset}
            xAxis={[
              {
                dataKey: "time",
                scaleType: "time",
                valueFormatter: (v) => formatTime(v),
              },
            ]}
            yAxis={[{ min: 0 }]}
            series={[{ dataKey: "listeners", area: true, showMark: false, label: t("stations:listeners") }]}
            margin={{ top: 10, right: 20, bottom: 30, left: 45 }}
          />
        )}
      </CardContent>
    </Card>
  );
}
