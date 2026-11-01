import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import Divider from "@mui/material/Divider";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import { fmt } from "@/components/queue";
import { SongCover } from "@/components/song-cover";
import { useSongDetails } from "@/hooks/use-songs";
import { formatBytes } from "@/lib/format-bytes";

function formatDate(value: string | null | undefined) {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <Box sx={{ display: "flex", gap: 2, alignItems: "baseline" }}>
      <Typography variant="body2" color="text.secondary" sx={{ minWidth: 150, flexShrink: 0 }}>
        {label}
      </Typography>
      <Typography variant="body2" sx={{ wordBreak: "break-all" }}>
        {value}
      </Typography>
    </Box>
  );
}

export function SongDetailsDialog({ songId, onClose }: { songId: string | null; onClose: () => void }) {
  const { t } = useTranslation();
  const { data, isLoading, isError } = useSongDetails(songId ?? "", { enabled: !!songId });

  return (
    <Dialog
      open={!!songId}
      onClose={onClose}
      maxWidth="sm"
      fullWidth
      slotProps={{ paper: { sx: { borderRadius: 3 } } }}
    >
      <DialogTitle sx={{ px: 3, pt: 3, pb: 1 }}>{t("songs:details_title")}</DialogTitle>
      <DialogContent sx={{ px: 3, pt: 2, pb: 3 }}>
        {isLoading || !data ? (
          isError ? (
            <Typography color="text.secondary" sx={{ py: 4 }}>
              {t("songs:details_no_metadata")}
            </Typography>
          ) : (
            <Skeleton variant="rounded" height={320} />
          )
        ) : (
          <Stack spacing={2.5}>
            <Box sx={{ display: "flex", gap: 2, alignItems: "center" }}>
              <SongCover songId={data.id} hasCover={data.has_cover} size={72} />
              <Box sx={{ minWidth: 0 }}>
                <Typography variant="h6" noWrap>
                  {data.title || "—"}
                </Typography>
                <Typography variant="body2" color="text.secondary" noWrap>
                  {data.artist || t("common:unknown_artist")}
                  {data.album ? ` — ${data.album}` : ""}
                </Typography>
                <Box sx={{ display: "flex", gap: 0.5, mt: 0.5, flexWrap: "wrap" }}>
                  {data.missing_since && (
                    <Chip size="small" color="error" variant="outlined" label={t("songs:missing_file")} />
                  )}
                  <Chip size="small" variant="outlined" label={formatBytes(data.file_size)} />
                  <Chip
                    size="small"
                    variant="outlined"
                    label={`${t("songs:table_duration")}: ${data.duration > 0 ? fmt(data.duration) : "—"}`}
                  />
                </Box>
              </Box>
            </Box>

            <Box>
              <InfoRow label={t("songs:details_path")} value={data.file_path || "—"} />
              <InfoRow
                label={t("songs:details_origin")}
                value={
                  data.origin === "directory" ? t("songs:details_origin_directory") : t("songs:details_origin_api")
                }
              />
              <InfoRow label={t("songs:details_type")} value={data.extension ?? "—"} />
              <InfoRow label={t("songs:details_mime")} value={data.mime_type || "—"} />
              <InfoRow label={t("songs:details_added")} value={formatDate(data.created_at)} />
              <InfoRow label={t("songs:details_updated")} value={formatDate(data.updated_at)} />
              {data.missing_since && (
                <InfoRow label={t("songs:details_missing_since")} value={formatDate(data.missing_since)} />
              )}
            </Box>

            <Box>
              <Typography variant="subtitle2" sx={{ mb: 1 }}>
                {t("songs:details_track")}
              </Typography>
              {data.track ? (
                <Box>
                  <InfoRow label={t("songs:details_container")} value={data.track.container ?? "—"} />
                  <InfoRow
                    label={t("songs:table_duration")}
                    value={data.track.duration_ms ? fmt(Math.round(data.track.duration_ms / 1000)) : "—"}
                  />
                  <InfoRow
                    label={t("songs:details_bitrate")}
                    value={data.track.bitrate_kbps ? `${data.track.bitrate_kbps} kbps` : "—"}
                  />
                  <InfoRow
                    label={t("songs:details_sample_rate")}
                    value={data.track.sample_rate_hz ? `${data.track.sample_rate_hz} Hz` : "—"}
                  />
                  <InfoRow label={t("songs:details_channels")} value={data.track.channels?.toString() ?? "—"} />
                  <InfoRow
                    label={t("songs:details_bit_depth")}
                    value={data.track.bit_depth ? `${data.track.bit_depth} bit` : "—"}
                  />
                </Box>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  {t("songs:details_no_metadata")}
                </Typography>
              )}
            </Box>

            {data.track && data.track.tags.length > 0 && (
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                  {t("songs:details_tags")}
                </Typography>
                <Box sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>
                  {data.track.tags.map((tag) => (
                    <InfoRow key={`${tag.key}:${tag.value}`} label={tag.key} value={tag.value} />
                  ))}
                </Box>
              </Box>
            )}

            <Divider />
            <Typography variant="caption" color="text.secondary">
              {data.track?.has_cover || data.has_cover ? t("songs:details_cover") : t("songs:details_no_cover")}
            </Typography>
          </Stack>
        )}
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button onClick={onClose}>{t("common:close")}</Button>
      </DialogActions>
    </Dialog>
  );
}
