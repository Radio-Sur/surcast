import LibraryMusic from "@mui/icons-material/LibraryMusic";
import PlaylistPlay from "@mui/icons-material/PlaylistPlay";
import Podcasts from "@mui/icons-material/Podcasts";
import ScheduleIcon from "@mui/icons-material/Schedule";
import SettingsIcon from "@mui/icons-material/Settings";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Skeleton from "@mui/material/Skeleton";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import { useNavigate, useParams } from "react-router-dom";
import { ScheduleSection } from "@/components/schedule/schedule-section";
import { useStationDetail } from "@/hooks/use-station-detail";
import { AutoDJSection } from "./auto-dj-section";
import { LibraryTab } from "./library-tab";
import { ListenersTab } from "./listeners-tab";
import { QueueAddDialog } from "./queue-add-dialog";
import { QueueSection } from "./queue-section";
import { SettingsTab } from "./settings-tab";
import { StationHeader } from "./station-header";
import { StreamConfirmDialog } from "./stream-confirm-dialog";

export function StationDetailPage() {
  const { t } = useTranslation();
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();

  const d = useStationDetail(id);

  if (!id) {
    return (
      <Box>
        <Typography variant="h4">{t("stations:not_found")}</Typography>
        <Button onClick={() => navigate("/stations")}>{t("common:go_back")}</Button>
      </Box>
    );
  }

  if (d.stationLoading) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Skeleton variant="text" width={200} height={40} />
        <Skeleton variant="rounded" height={300} />
      </Box>
    );
  }

  if (d.stationError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">
          {d.stationLoadError instanceof Error ? d.stationLoadError.message : "Failed to load station details"}
        </Alert>
      </Box>
    );
  }

  if (!d.station) {
    return (
      <Box>
        <Typography variant="h4">{t("stations:not_found")}</Typography>
        <Button onClick={() => navigate("/stations")}>{t("common:go_back")}</Button>
      </Box>
    );
  }

  const station = d.station;

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <StationHeader
        station={station}
        playing={!!d.streamStatus?.playing}
        onBack={() => navigate("/stations")}
        onToggle={() => {
          if (d.streamStatus?.playing) {
            d.setConfirmAction("pause");
          } else {
            d.streamPlay.mutate(undefined, {
              onError: (err) => d.handleMutationError(err, "start stream"),
            });
          }
        }}
        onRestart={() => d.setConfirmAction("restart")}
        onEdit={() => navigate(`/stations/${station.slug || id}/edit`)}
      />

      <Tabs value={d.tab} onChange={(_, v) => d.setTab(v)} sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tab
          icon={<LibraryMusic />}
          iconPosition="start"
          label={t("stations:detail_tab_library", { count: d.librarySongTotal ?? d.librarySongs?.length ?? 0 })}
        />
        <Tab
          icon={<PlaylistPlay />}
          iconPosition="start"
          label={t("stations:detail_tab_queue", { count: d.queueSections.upcoming.length })}
        />
        <Tab icon={<ScheduleIcon />} iconPosition="start" label={t("stations:detail_tab_schedule")} />
        <Tab icon={<Podcasts />} iconPosition="start" label={t("stations:detail_tab_listeners")} />
        <Tab icon={<SettingsIcon />} iconPosition="start" label={t("stations:detail_tab_settings")} />
      </Tabs>

      {d.tab === 0 &&
        (d.libraryError ? (
          <Alert severity="error">
            {d.libraryLoadError instanceof Error ? d.libraryLoadError.message : "Failed to load station library"}
          </Alert>
        ) : (
          <LibraryTab
            librarySongs={d.librarySongs}
            librarySongTotal={d.librarySongTotal}
            libraryPage={d.libraryPage}
            libraryPerPage={d.libraryPerPage}
            onLibraryPageChange={d.setLibraryPage}
            onLibraryPerPageChange={d.setLibraryPerPage}
            libraryLoading={d.libraryLoading}
            onRemove={d.handleRemoveFromStation}
            stationId={station.id}
          />
        ))}

      {d.tab === 1 && (
        <QueueSection
          stationId={station.id}
          queueSections={d.queueSections}
          streamStatus={d.streamStatus}
          connected={d.connected}
          elapsed={d.elapsed}
          listeners={d.liveListeners}
          httpSkip={d.httpSkip as { isPending: boolean; mutate: (...args: unknown[]) => void }}
          reorderQueue={
            d.reorderQueue as {
              isPending: boolean;
              mutate: (songIds: string[], options?: { onError?: (err: unknown) => void }) => void;
            }
          }
          removePlaylistFromQueue={
            d.removePlaylistFromQueue as {
              isPending: boolean;
              mutate: (playlistId: string, options?: { onError?: (err: unknown) => void }) => void;
            }
          }
          handleRemoveFromQueue={d.handleRemoveFromQueue}
          handleReAddToQueue={d.handleReAddToQueue}
          setQueueAddOpen={d.setQueueAddOpen}
        />
      )}

      {d.tab === 3 && <ListenersTab stationId={station.id} />}

      {d.tab === 4 && (
        <SettingsTab
          prebufferBytes={station.prebuffer_bytes}
          playedLimit={station.played_limit}
          defaultFadeMs={station.default_fade_ms}
          transitionMode={station.transition_mode}
          autocueFadeMaxMs={station.autocue_fade_max_ms}
          streamUrl={d.streamUrl}
          updateStation={(data) =>
            d.updateStation.mutate(
              { id: station.id, data },
              { onError: (err) => d.handleMutationError(err, "update station") },
            )
          }
          updatePending={d.updateStation.isPending}
        />
      )}

      {d.tab === 2 && (
        <Card variant="outlined" sx={{ borderRadius: 3 }}>
          <CardContent sx={{ p: 4, "&:last-child": { pb: 4 } }}>
            <ScheduleSection
              stationId={station.id}
              playlists={d.playlists || []}
              queueEndEstimate={d.queueEndEstimate}
            />
            <Box sx={{ mt: 4, pt: 4, borderTop: 1, borderColor: "divider" }}>
              <AutoDJSection stationId={station.id} playlists={d.playlists || []} />
            </Box>
          </CardContent>
        </Card>
      )}

      <QueueAddDialog
        open={d.queueAddOpen}
        librarySongs={d.librarySongsAll}
        selectedSongIds={d.selectedSongIds}
        isPending={d.addToQueue.isPending}
        onToggleSelect={d.toggleSelectSong}
        onAdd={d.handleAddToQueue}
        onClose={() => {
          d.setQueueAddOpen(false);
          d.setSelectedSongIds(new Set());
        }}
      />

      <StreamConfirmDialog
        action={d.confirmAction}
        isPending={false}
        onConfirm={() => {
          if (d.confirmAction === "pause") {
            d.streamPause.mutate(undefined, {
              onError: (err) => d.handleMutationError(err, "pause stream"),
            });
          } else {
            d.streamRestart.mutate(undefined, {
              onError: (err) => d.handleMutationError(err, "restart stream"),
            });
          }
          d.setConfirmAction(null);
        }}
        onClose={() => d.setConfirmAction(null)}
      />
    </Box>
  );
}
