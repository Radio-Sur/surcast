import { Warning } from "@mui/icons-material";
import Lock from "@mui/icons-material/Lock";
import {
  Alert,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  TextField,
} from "@mui/material";
import { useTranslation } from "react-i18next";
import { AutoDJFormFields } from "@/components/schedule/auto-dj-form-fields";
import { RecurrencePicker } from "@/pages/stations/recurrence-picker";
import type { Playlist, ScheduleEvent, ScheduleSourceType } from "@/types";
import { fmtDuration, minutesToTime } from "./schedule-utils";
import { useScheduleEventForm } from "./use-schedule-event-form";

export function ScheduleEventDialog({
  open,
  editingEvent,
  stationId,
  playlists,
  events,
  queueEndEstimate,
  defaultDate,
  defaultStartTime,
  onClose,
  onDelete,
}: {
  open: boolean;
  editingEvent: ScheduleEvent | null;
  stationId: string;
  playlists: Playlist[];
  events: ScheduleEvent[] | undefined;
  queueEndEstimate?: string | null;
  defaultDate?: string;
  defaultStartTime?: string;
  onClose: () => void;
  onDelete: (event: ScheduleEvent) => void;
}) {
  const { t } = useTranslation();

  const {
    form,
    setForm,
    error,
    setError,
    selectedPlaylist,
    endTimeAutoCalc,
    formOvernight,
    isPastEvent,
    isActiveEvent,
    isReadOnly,
    isAddingToPast,
    queueEndWarning,
    conflictInfo,
    handleSave,
    createPending,
    updatePending,
  } = useScheduleEventForm({
    open,
    editingEvent,
    stationId,
    playlists,
    events,
    queueEndEstimate,
    defaultDate,
    defaultStartTime,
    onClose,
    onDelete,
  });

  const handleSaveWrapper = async () => {
    await handleSave();
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{editingEvent ? t("schedule:dialog_edit_title") : t("schedule:dialog_new_title")}</DialogTitle>
      <DialogContent>
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2, mt: 1 }}>
          {isPastEvent && (
            <Alert severity="info" icon={<Lock fontSize="small" />}>
              {t("schedule:dialog_past_alert")}
            </Alert>
          )}
          {isActiveEvent && <Alert severity="info">{t("schedule:dialog_active_alert")}</Alert>}

          {endTimeAutoCalc && selectedPlaylist && (
            <Alert severity="warning">{t("schedule:dialog_auto_calc_alert")}</Alert>
          )}

          {queueEndWarning && !isReadOnly && (
            <Alert severity="info" icon={<Warning fontSize="small" />}>
              {t("schedule:dialog_queue_end", { time: queueEndEstimate, adjusted: minutesToTime(queueEndWarning.adj) })}
            </Alert>
          )}
          {conflictInfo && !isReadOnly && (
            <Alert severity="info" icon={<Warning fontSize="small" />}>
              {conflictInfo.adjStart
                ? t("schedule:dialog_conflict_adjust", {
                    label:
                      conflictInfo.conflict.title ||
                      conflictInfo.conflict.playlist_name ||
                      t(`schedule:source_type_${conflictInfo.conflict.source_type}`),
                    start: conflictInfo.conflict.start_time,
                    end: conflictInfo.conflict.end_time,
                    adjStart: conflictInfo.adjStart,
                    adjEnd: conflictInfo.adjEnd,
                  })
                : t("schedule:dialog_conflict_blocked", {
                    label:
                      conflictInfo.conflict.title ||
                      conflictInfo.conflict.playlist_name ||
                      t(`schedule:source_type_${conflictInfo.conflict.source_type}`),
                  })}
            </Alert>
          )}

          <TextField
            label={t("schedule:dialog_title_optional")}
            size="small"
            value={form.title || ""}
            onChange={(e) => setForm({ ...form, title: e.target.value || null })}
            disabled={isReadOnly}
          />
          <TextField
            label={t("schedule:dialog_date")}
            type="date"
            size="small"
            value={form.start_date}
            onChange={(e) => setForm({ ...form, start_date: e.target.value })}
            slotProps={{ inputLabel: { shrink: true } }}
            disabled={isReadOnly}
          />
          <Box sx={{ display: "flex", gap: 2 }}>
            <TextField
              label={t("schedule:dialog_start")}
              type="time"
              size="small"
              value={form.start_time}
              onChange={(e) => setForm({ ...form, start_time: e.target.value })}
              slotProps={{ inputLabel: { shrink: true } }}
              sx={{ flex: 1 }}
              disabled={isReadOnly}
            />
            <TextField
              label={t("schedule:dialog_end")}
              type="time"
              size="small"
              value={form.end_time}
              onChange={(e) => setForm({ ...form, end_time: e.target.value })}
              slotProps={{
                inputLabel: { shrink: true },
                input: endTimeAutoCalc
                  ? {
                      readOnly: true,
                      startAdornment: <Lock sx={{ fontSize: 14, mr: 0.5, color: "text.disabled" }} />,
                    }
                  : undefined,
              }}
              sx={{ flex: 1 }}
              disabled={isReadOnly || endTimeAutoCalc}
              helperText={
                endTimeAutoCalc && selectedPlaylist
                  ? t("schedule:dialog_auto_calc_helper", {
                      duration: fmtDuration(selectedPlaylist.total_duration_seconds),
                    })
                  : undefined
              }
            />
          </Box>
          {formOvernight && !endTimeAutoCalc && (
            <Chip
              label={t("schedule:dialog_overnight_chip")}
              size="small"
              color="warning"
              sx={{ alignSelf: "flex-start" }}
            />
          )}

          <FormControl size="small">
            <InputLabel>{t("schedule:dialog_source_type")}</InputLabel>
            <Select
              value={form.source_type}
              label={t("schedule:dialog_source_type")}
              onChange={(e) => setForm({ ...form, source_type: e.target.value as ScheduleSourceType })}
              disabled={isReadOnly}
            >
              {(["playlist", "station_library", "global_library", "weighted_playlists"] as const).map((k) => (
                <MenuItem key={k} value={k}>
                  {t(`schedule:source_type_${k}`)}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          {form.source_type === "playlist" && (
            <FormControl size="small">
              <InputLabel>{t("schedule:dialog_playlist")}</InputLabel>
              <Select
                value={form.playlist_id || ""}
                label={t("schedule:dialog_playlist")}
                onChange={(e) => setForm({ ...form, playlist_id: e.target.value || null })}
                disabled={isReadOnly}
              >
                {playlists.map((p) => (
                  <MenuItem key={p.id} value={p.id}>
                    {p.name}
                    {p.total_duration_seconds > 0 ? ` (${fmtDuration(p.total_duration_seconds)})` : ""}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}

          {form.source_type === "playlist" && (
            <AutoDJFormFields
              autoDjMode={form.auto_dj_mode}
              autoDjAvoidRepeat={form.auto_dj_avoid_repeat}
              autoDjMinGap={form.auto_dj_min_gap}
              autoDjSongsAhead={form.auto_dj_songs_ahead}
              readOnly={isReadOnly}
              onChange={(field, val) => setForm({ ...form, [field]: val })}
            />
          )}

          <RecurrencePicker
            value={form.recurrence_type}
            interval={form.recurrence_interval}
            days={form.recurrence_days}
            endDate={form.recurrence_end_date}
            count={form.recurrence_count}
            onChange={(field, val) => setForm({ ...form, [field]: val })}
          />

          {isAddingToPast && (
            <Alert severity="warning" icon={<Warning fontSize="small" />}>
              {t("schedule:dialog_past_add_warning")}
            </Alert>
          )}

          {error && (
            <Alert severity="error" onClose={() => setError(null)}>
              {error}
            </Alert>
          )}
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>{t("common:cancel")}</Button>
        {editingEvent && (
          <Button color="error" onClick={() => onDelete(editingEvent)}>
            {t("common:delete")}
          </Button>
        )}
        <Button
          variant="contained"
          onClick={handleSaveWrapper}
          disabled={isReadOnly || isAddingToPast || createPending || updatePending}
        >
          {createPending || updatePending
            ? t("common:saving")
            : editingEvent
              ? t("schedule:dialog_update")
              : t("schedule:dialog_create")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
