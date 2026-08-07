import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
} from "@mui/material";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useDeleteScheduleEvent, useScheduleEvents } from "@/hooks/use-schedule-events";
import { useSnackbar } from "@/providers/snackbar-provider";
import type { Playlist, ScheduleEvent } from "@/types";
import { ScheduleCalendar } from "./schedule-calendar";
import { ScheduleEventDialog } from "./schedule-event-dialog";
import { ScheduleEventList } from "./schedule-event-list";
import { formatDate, getWeekStart, isDateInRecurrence, timeToMinutes } from "./schedule-utils";
import { WeekNavigation } from "./week-navigation";

export function ScheduleSection({
  stationId,
  playlists,
  queueEndEstimate,
}: {
  stationId: string;
  playlists: Playlist[];
  queueEndEstimate?: string | null;
}) {
  const [weekStart, setWeekStart] = useState(() => getWeekStart(new Date()));
  const weekStartStr = formatDate(weekStart);
  const weekEndDate = new Date(weekStart);
  weekEndDate.setDate(weekEndDate.getDate() + 6);
  const weekEndStr = formatDate(weekEndDate);

  const { data: events, isLoading, isError, error } = useScheduleEvents(stationId, weekStartStr, weekEndStr);

  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingEvent, setEditingEvent] = useState<ScheduleEvent | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState<ScheduleEvent | null>(null);
  const [defaultDate, setDefaultDate] = useState<string>("");
  const [defaultStartTime, setDefaultStartTime] = useState<string>("09:00");
  const [dialogKey, setDialogKey] = useState(0);
  const { showSnackbar } = useSnackbar();
  const { t, i18n } = useTranslation();

  const deleteEvent = useDeleteScheduleEvent(stationId);

  const weekDates = useMemo(() => {
    return Array.from({ length: 7 }, (_, i) => {
      const d = new Date(weekStart);
      d.setDate(d.getDate() + i);
      return d;
    });
  }, [weekStart]);

  const queueEndMinutes = useMemo(() => {
    if (!queueEndEstimate) return null;
    const now = new Date();
    const currentMinutes = now.getHours() * 60 + now.getMinutes();
    const queueEndMinutesRaw = timeToMinutes(queueEndEstimate);
    return queueEndMinutesRaw < currentMinutes ? queueEndMinutesRaw + 24 * 60 : queueEndMinutesRaw;
  }, [queueEndEstimate]);

  const openCreateDialog = useCallback(
    (dayIdx?: number, hour?: number) => {
      setEditingEvent(null);
      const date = new Date(weekStart);
      if (dayIdx !== undefined) date.setDate(date.getDate() + dayIdx);
      setDefaultDate(formatDate(date));
      setDefaultStartTime(hour !== undefined ? `${hour.toString().padStart(2, "0")}:00` : "09:00");
      setDialogKey((k) => k + 1);
      setDialogOpen(true);
    },
    [weekStart],
  );

  const openEditDialog = useCallback((event: ScheduleEvent) => {
    setEditingEvent(event);
    setDefaultDate(event.start_date);
    setDefaultStartTime(event.start_time);
    setDialogKey((k) => k + 1);
    setDialogOpen(true);
  }, []);

  const handleDelete = useCallback(async () => {
    if (!deleteConfirm) return;
    try {
      await deleteEvent.mutateAsync(deleteConfirm.id);
      setDeleteConfirm(null);
    } catch (err) {
      console.error("Failed to delete schedule event:", err);
      showSnackbar("Failed to delete event", "error");
    }
  }, [deleteConfirm, deleteEvent, showSnackbar]);

  if (isError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">{error instanceof Error ? error.message : "Failed to load schedule events"}</Alert>
      </Box>
    );
  }

  return (
    <Box>
      <WeekNavigation
        weekDates={weekDates}
        onPrevWeek={() => {
          const d = new Date(weekStart);
          d.setDate(d.getDate() - 7);
          setWeekStart(d);
        }}
        onNextWeek={() => {
          const d = new Date(weekStart);
          d.setDate(d.getDate() + 7);
          setWeekStart(d);
        }}
        onToday={() => setWeekStart(getWeekStart(new Date()))}
        onAddEvent={() => openCreateDialog()}
        t={t}
        i18n={i18n}
      />

      <ScheduleCalendar
        weekDates={weekDates}
        events={events}
        queueEndEstimate={queueEndEstimate}
        queueEndMinutes={queueEndMinutes}
        onCreateEvent={openCreateDialog}
        onEditEvent={openEditDialog}
        isEventOnDate={isDateInRecurrence}
      />

      <ScheduleEventList
        events={events}
        isLoading={isLoading}
        onEditEvent={openEditDialog}
        onDeleteEvent={(ev) => setDeleteConfirm(ev)}
      />

      <ScheduleEventDialog
        key={dialogKey}
        open={dialogOpen}
        editingEvent={editingEvent}
        stationId={stationId}
        playlists={playlists}
        events={events}
        queueEndEstimate={queueEndEstimate}
        defaultDate={editingEvent ? editingEvent.start_date : defaultDate}
        defaultStartTime={editingEvent ? editingEvent.start_time : defaultStartTime}
        onClose={() => {
          setDialogOpen(false);
          setEditingEvent(null);
        }}
        onDelete={(event) => {
          setDialogOpen(false);
          setDeleteConfirm(event);
        }}
      />

      <Dialog open={!!deleteConfirm} onClose={() => setDeleteConfirm(null)}>
        <DialogTitle>{t("schedule:delete_title")}</DialogTitle>
        <DialogContent>
          <DialogContentText>
            {t("schedule:delete_message", {
              label:
                deleteConfirm?.title ||
                deleteConfirm?.playlist_name ||
                t(`schedule:source_type_${deleteConfirm?.source_type || "playlist"}`),
            })}
            {deleteConfirm?.recurrence_type !== "none" && t("schedule:delete_recurring_note")}
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteConfirm(null)}>{t("common:cancel")}</Button>
          <Button onClick={handleDelete} color="error" disabled={deleteEvent.isPending}>
            {t("common:delete")}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
