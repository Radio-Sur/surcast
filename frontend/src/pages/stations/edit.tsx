import ArrowBack from "@mui/icons-material/ArrowBack";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Skeleton from "@mui/material/Skeleton";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate, useParams } from "react-router-dom";
import { useStation, useUpdateStation } from "@/hooks/use-stations";
import { isHttpError } from "@/lib/is-http-error";

export function EditStationPage() {
  const { t } = useTranslation();
  const { id } = useParams<{ id: string }>();
  const { data: station, isLoading } = useStation(id ?? "");
  const updateStation = useUpdateStation();
  const navigate = useNavigate();

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [streamUrl, setStreamUrl] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    if (station) {
      setName(station.name);
      setDescription(station.description);
      setStreamUrl(station.stream_url ?? "");
    }
  }, [station]);

  if (!id) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Typography variant="h4">{t("stations:not_found")}</Typography>
        <Button onClick={() => navigate("/stations")}>{t("common:go_back")}</Button>
      </Box>
    );
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    try {
      await updateStation.mutateAsync({
        id: id,
        data: {
          name,
          description: description || undefined,
          stream_url: streamUrl || undefined,
        },
      });
      navigate("/stations");
    } catch (err: unknown) {
      setError(isHttpError(err)?.message || t("errors:station_update"));
    }
  };

  if (isLoading) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2, maxWidth: 640 }}>
        <Skeleton variant="text" width={200} height={40} />
        <Skeleton variant="rounded" height={300} />
      </Box>
    );
  }

  if (!station) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Typography variant="h4">{t("stations:not_found")}</Typography>
        <Button onClick={() => navigate("/stations")}>{t("common:go_back")}</Button>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3, maxWidth: 640 }}>
      <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
        <Button onClick={() => navigate("/stations")} sx={{ minWidth: 40, p: 1 }}>
          <ArrowBack />
        </Button>
        <Box>
          <Typography variant="h4">{t("stations:edit_title")}</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.25 }}>
            {t("stations:edit_subtitle")}
          </Typography>
        </Box>
      </Box>

      <Card>
        <CardContent sx={{ p: 3 }}>
          <Typography variant="h6" sx={{ mb: 0.5 }}>
            {t("stations:card_title")}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
            {t("stations:edit_subtitle_named", { name: station.name })}
          </Typography>

          <Box component="form" onSubmit={handleSubmit} sx={{ display: "flex", flexDirection: "column", gap: 2.5 }}>
            {error && <Alert severity="error">{error}</Alert>}

            <TextField
              label={t("stations:name_label")}
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              fullWidth
            />
            <TextField
              label={t("stations:description_label")}
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              fullWidth
              multiline
              rows={3}
            />
            <TextField
              label={t("stations:mount_label")}
              value={streamUrl}
              onChange={(e) => setStreamUrl(e.target.value)}
              placeholder={t("stations:mount_placeholder_edit")}
              helperText={t("stations:mount_helper_edit")}
              fullWidth
            />
            <Box sx={{ display: "flex", gap: 1.5, pt: 1 }}>
              <Button variant="outlined" onClick={() => navigate("/stations")}>
                {t("common:cancel")}
              </Button>
              <Button type="submit" variant="contained" disabled={updateStation.isPending}>
                {updateStation.isPending ? t("common:saving") : t("stations:save_changes")}
              </Button>
            </Box>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
