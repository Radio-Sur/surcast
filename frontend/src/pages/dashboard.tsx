import People from "@mui/icons-material/People";
import Radio from "@mui/icons-material/Radio";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Grid from "@mui/material/Grid";
import Skeleton from "@mui/material/Skeleton";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import { ListenersOverviewSection } from "@/components/listeners/listeners-overview-section";
import { useAuth } from "@/hooks/use-auth";
import { useStations } from "@/hooks/use-stations";

export function DashboardPage() {
  const { t } = useTranslation();
  const { user } = useAuth();
  const { data: stations, isLoading, isError, error } = useStations();

  const stats = [
    {
      title: t("dashboard:total_stations"),
      value: isLoading ? t("dashboard:fallback") : (stations?.length ?? 0),
      icon: Radio,
    },
    {
      title: t("dashboard:user_role"),
      value: user?.role ?? t("dashboard:fallback"),
      icon: People,
    },
  ];

  if (isError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">{error instanceof Error ? error.message : "Failed to load stations"}</Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <Box>
        <Typography variant="h4">{t("dashboard:welcome", { name: user?.name })}</Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
          {t("dashboard:subtitle")}
        </Typography>
      </Box>

      <Grid container spacing={2}>
        {stats.map((stat) => {
          const Icon = stat.icon;
          return (
            <Grid size={{ xs: 12, sm: 4 }} key={stat.title}>
              <Card sx={{ borderRadius: 3 }}>
                <CardContent sx={{ p: 3 }}>
                  <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 1 }}>
                    <Typography variant="body2" color="text.secondary" sx={{ fontWeight: 500 }}>
                      {stat.title}
                    </Typography>
                    <Icon sx={{ fontSize: 20, color: "text.secondary" }} />
                  </Box>
                  <Typography variant="h4">{isLoading ? <Skeleton width={60} /> : stat.value}</Typography>
                </CardContent>
              </Card>
            </Grid>
          );
        })}
      </Grid>

      <ListenersOverviewSection />

      {isLoading ? (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
          <Skeleton variant="text" width={200} height={32} />
          <Skeleton variant="rounded" height={160} />
        </Box>
      ) : stations && stations.length > 0 ? (
        <Card sx={{ borderRadius: 3 }}>
          <CardContent sx={{ p: 3 }}>
            <Typography variant="h6" sx={{ mb: 2 }}>
              {t("dashboard:recent_stations")}
            </Typography>
            <Box sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>
              {stations.slice(0, 5).map((station) => (
                <Box
                  key={station.id}
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    p: 1.5,
                    borderRadius: 2,
                    "&:hover": { bgcolor: "action.hover" },
                  }}
                >
                  <Box>
                    <Typography variant="body2" sx={{ fontWeight: 600 }}>
                      {station.name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {station.description || t("common:no_description")}
                    </Typography>
                  </Box>
                  <Box
                    sx={{
                      width: 8,
                      height: 8,
                      borderRadius: "50%",
                      bgcolor: "success.main",
                    }}
                  />
                </Box>
              ))}
            </Box>
          </CardContent>
        </Card>
      ) : (
        <Card sx={{ borderRadius: 3 }}>
          <CardContent sx={{ p: 3 }}>
            <Typography variant="h6" sx={{ mb: 1 }}>
              {t("dashboard:get_started_title")}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {t("dashboard:get_started_text")}
            </Typography>
          </CardContent>
        </Card>
      )}
    </Box>
  );
}
