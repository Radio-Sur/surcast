import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import CircularProgress from "@mui/material/CircularProgress";
import Divider from "@mui/material/Divider";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Typography from "@mui/material/Typography";
import { BarChart } from "@mui/x-charts/BarChart";
import { LineChart } from "@mui/x-charts/LineChart";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useListenersOverview } from "@/hooks/use-listeners";
import type { ListenerRange } from "@/types";

const RANGES: ListenerRange[] = ["24h", "7d", "30d"];
const WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"];

function formatTime(value: number | Date) {
  return new Date(value).toLocaleString([], {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function ListenersOverviewSection() {
  const { t } = useTranslation();
  const [range, setRange] = useState<ListenerRange>("7d");
  const { data, isLoading, isError } = useListenersOverview(range);

  const series = (data?.series ?? []).map((p) => ({
    time: new Date(p.time).getTime(),
    listeners: p.listeners,
  }));

  const hours = (data?.by_hour ?? []).map((h) => ({
    hour: `${String(h.hour).padStart(2, "0")}:00`,
    avg_listeners: h.avg_listeners,
  }));

  const weekdays = (data?.by_weekday ?? []).map((w) => ({
    weekday: t(`common:weekday_${WEEKDAYS[w.weekday - 1] ?? ""}`),
    avg_listeners: w.avg_listeners,
  }));

  return (
    <Card sx={{ borderRadius: 3 }}>
      <CardContent sx={{ p: 3 }}>
        <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 2 }}>
          <Typography variant="h6">{t("dashboard:listeners_title")}</Typography>
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
          <Box sx={{ display: "flex", justifyContent: "center", py: 6 }}>
            <CircularProgress />
          </Box>
        ) : isError ? (
          <Typography color="error">{t("dashboard:listeners_load_error")}</Typography>
        ) : (
          <>
            <Box sx={{ mb: 2 }}>
              <Typography variant="body2" color="text.secondary" sx={{ fontWeight: 500 }}>
                {t("dashboard:listeners_now")}
              </Typography>
              <Typography variant="h4">{data?.total_now ?? 0}</Typography>
            </Box>

            {series.length > 0 ? (
              <LineChart
                height={240}
                dataset={series}
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
            ) : (
              <Typography color="text.secondary" sx={{ py: 4 }}>
                {t("stations:listeners_empty")}
              </Typography>
            )}

            <Box sx={{ display: "grid", gridTemplateColumns: { xs: "1fr", md: "1fr 1fr" }, gap: 3, mt: 3 }}>
              <Box>
                <Typography variant="body2" color="text.secondary" sx={{ fontWeight: 500, mb: 1 }}>
                  {t("dashboard:listeners_by_hour")}
                </Typography>
                {hours.length > 0 ? (
                  <BarChart
                    height={200}
                    dataset={hours}
                    xAxis={[{ dataKey: "hour", scaleType: "band", tickLabelStyle: { fontSize: 10 } }]}
                    series={[
                      {
                        dataKey: "avg_listeners",
                        label: t("stations:listeners"),
                        valueFormatter: (v) => `${Math.round((v as number) ?? 0)}`,
                      },
                    ]}
                    margin={{ top: 10, right: 10, bottom: 30, left: 40 }}
                  />
                ) : (
                  <Typography color="text.secondary">{t("stations:listeners_empty")}</Typography>
                )}
              </Box>

              <Box>
                <Typography variant="body2" color="text.secondary" sx={{ fontWeight: 500, mb: 1 }}>
                  {t("dashboard:listeners_by_weekday")}
                </Typography>
                {weekdays.length > 0 ? (
                  <BarChart
                    height={200}
                    dataset={weekdays}
                    xAxis={[{ dataKey: "weekday", scaleType: "band", tickLabelStyle: { fontSize: 10 } }]}
                    series={[
                      {
                        dataKey: "avg_listeners",
                        label: t("stations:listeners"),
                        valueFormatter: (v) => `${Math.round((v as number) ?? 0)}`,
                      },
                    ]}
                    margin={{ top: 10, right: 10, bottom: 30, left: 40 }}
                  />
                ) : (
                  <Typography color="text.secondary">{t("stations:listeners_empty")}</Typography>
                )}
              </Box>
            </Box>

            {data && data.stations.length > 0 && (
              <>
                <Divider sx={{ my: 3 }} />
                <Typography variant="body2" color="text.secondary" sx={{ fontWeight: 500, mb: 1 }}>
                  {t("dashboard:listeners_per_station")}
                </Typography>
                <Box sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>
                  {data.stations.map((station) => (
                    <Box
                      key={station.station_id}
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        p: 1,
                        borderRadius: 1,
                        "&:hover": { bgcolor: "action.hover" },
                      }}
                    >
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                        {station.name}
                      </Typography>
                      <Typography variant="body2" color={station.online ? "text.primary" : "text.disabled"}>
                        {station.online ? station.listeners : "—"}
                      </Typography>
                    </Box>
                  ))}
                </Box>
              </>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
