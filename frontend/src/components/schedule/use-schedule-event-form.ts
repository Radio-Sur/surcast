import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useCreateScheduleEvent, useUpdateScheduleEvent } from "@/hooks/use-schedule-events";
import { isHttpError } from "@/lib/is-http-error";
import type { Playlist, RecurrenceType, ScheduleEvent, ScheduleSourceType } from "@/types";
import {
  checkTimeOverlap,
  computeConflictInfo,
  isActiveEvent as computeIsActiveEvent,
  isAddingToPast as computeIsAddingToPast,
  isPastEvent as computeIsPastEvent,
  computeQueueEndWarning,
  getEventsForDate,
  minutesToTime,
  timeToMinutes,
} from "./schedule-utils";

interface FormState {
  start_date: string;
  start_time: string;
  end_time: string;
  source_type: ScheduleSourceType;
  playlist_id: string | null;
  auto_dj_mode: string | null;
  auto_dj_avoid_repeat: boolean | null;
  auto_dj_min_gap: number | null;
  auto_dj_songs_ahead: number | null;
  recurrence_type: RecurrenceType;
  recurrence_interval: number | null;
  recurrence_days: number[] | null;
  recurrence_end_date: string | null;
  recurrence_count: number | null;
  title: string | null;
}

const defaultFormState: FormState = {
  start_date: "",
  start_time: "09:00",
  end_time: "10:00",
  source_type: "playlist",
  playlist_id: null,
  auto_dj_mode: null,
  auto_dj_avoid_repeat: null,
  auto_dj_min_gap: null,
  auto_dj_songs_ahead: null,
  recurrence_type: "none",
  recurrence_interval: null,
  recurrence_days: null,
  recurrence_end_date: null,
  recurrence_count: null,
  title: null,
};

interface UseScheduleEventFormProps {
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
}

export function useScheduleEventForm({
  open,
  editingEvent,
  stationId,
  playlists,
  events,
  queueEndEstimate,
  defaultDate,
  defaultStartTime,
  onClose,
}: UseScheduleEventFormProps) {
  const [form, setForm] = useState<FormState>({ ...defaultFormState });
  const [error, setError] = useState<string | null>(null);
  const { t } = useTranslation();
  const createEvent = useCreateScheduleEvent(stationId);
  const updateEvent = useUpdateScheduleEvent(stationId);

  useEffect(() => {
    if (!open) return;
    if (editingEvent) {
      setForm({
        start_date: editingEvent.start_date,
        start_time: editingEvent.start_time,
        end_time: editingEvent.end_time,
        source_type: editingEvent.source_type,
        playlist_id: editingEvent.playlist_id,
        auto_dj_mode: editingEvent.auto_dj_mode,
        auto_dj_avoid_repeat: editingEvent.auto_dj_avoid_repeat,
        auto_dj_min_gap: editingEvent.auto_dj_min_gap,
        auto_dj_songs_ahead: editingEvent.auto_dj_songs_ahead,
        recurrence_type: editingEvent.recurrence_type,
        recurrence_interval: editingEvent.recurrence_interval,
        recurrence_days: editingEvent.recurrence_days,
        recurrence_end_date: editingEvent.recurrence_end_date,
        recurrence_count: editingEvent.recurrence_count,
        title: editingEvent.title,
      });
    } else {
      setForm({
        ...defaultFormState,
        start_date: defaultDate || "",
        start_time: defaultStartTime || "09:00",
        end_time: (() => {
          if (!defaultStartTime) return "10:00";
          const [h] = defaultStartTime.split(":").map(Number);
          return `${((h + 1) % 24).toString().padStart(2, "0")}:00`;
        })(),
      });
    }
    setError(null);
  }, [open, editingEvent, defaultDate, defaultStartTime]);

  const selectedPlaylist = useMemo(
    () => playlists.find((p) => p.id === form.playlist_id),
    [playlists, form.playlist_id],
  );

  const isPlaylistSource = form.source_type === "playlist";
  const endTimeAutoCalc = isPlaylistSource && !form.auto_dj_mode;
  const formOvernight = timeToMinutes(form.end_time) <= timeToMinutes(form.start_time);

  useEffect(() => {
    if (endTimeAutoCalc && selectedPlaylist) {
      const startMinutes = timeToMinutes(form.start_time);
      const durMinutes = Math.ceil(selectedPlaylist.total_duration_seconds / 60);
      setForm((prev) => ({ ...prev, end_time: minutesToTime(startMinutes + durMinutes) }));
    }
  }, [endTimeAutoCalc, selectedPlaylist, form.start_time]);

  const isPastEvent = editingEvent ? computeIsPastEvent(editingEvent) : false;
  const isActiveEvent = editingEvent ? computeIsActiveEvent(editingEvent) : false;
  const isReadOnly = isPastEvent || isActiveEvent;
  const isAddingToPast = computeIsAddingToPast(form.start_date, form.start_time, editingEvent);

  const queueEndWarning = computeQueueEndWarning(queueEndEstimate, form.start_date, form.start_time, isReadOnly);
  const conflictInfo = computeConflictInfo(
    events,
    form.start_date,
    form.start_time,
    form.end_time,
    editingEvent,
    isReadOnly,
  );

  const handleSave = useCallback(async () => {
    setError(null);
    if (!form.start_date || !form.start_time || !form.end_time) {
      setError(t("schedule:dialog_validation_required"));
      return;
    }

    if (form.end_time === form.start_time) {
      setError(t("schedule:dialog_validation_end_time"));
      return;
    }

    if (form.source_type === "playlist" && !form.playlist_id) {
      setError(t("schedule:dialog_validation_playlist"));
      return;
    }

    let adjustedStart = form.start_time;
    let adjustedEnd = form.end_time;
    let needsAdjustment = false;
    const formDate = form.start_date ? new Date(`${form.start_date}T00:00:00`) : null;

    if (formDate && events) {
      const dayEvents = getEventsForDate(events, formDate).filter((e) => {
        if (editingEvent && e.id === editingEvent.id) return false;
        return checkTimeOverlap(adjustedStart, adjustedEnd, e.start_time, e.end_time);
      });

      if (dayEvents.length > 0) {
        dayEvents.sort((a, b) => timeToMinutes(a.start_time) - timeToMinutes(b.start_time));
        const lastConflict = dayEvents[dayEvents.length - 1];
        const conflictEnd = timeToMinutes(lastConflict.end_time);
        const endAdjustedForOvernight =
          lastConflict.end_time <= lastConflict.start_time ? conflictEnd + 1440 : conflictEnd;
        const durationMin = timeToMinutes(form.end_time) - timeToMinutes(form.start_time);
        if (durationMin <= 0) {
          setError(
            t("schedule:dialog_validation_conflict", {
              label:
                lastConflict.title ||
                lastConflict.playlist_name ||
                t(`schedule:source_type_${lastConflict.source_type}`),
            }),
          );
          return;
        }
        const newStartMin = endAdjustedForOvernight;
        const newEndMin = newStartMin + durationMin;
        adjustedStart = `${Math.floor(newStartMin / 60) % 24}:${(newStartMin % 60).toString().padStart(2, "0")}`;
        adjustedEnd = `${Math.floor(newEndMin / 60) % 24}:${(newEndMin % 60).toString().padStart(2, "0")}`;

        const recheck = getEventsForDate(events, formDate).filter((e) => {
          if (editingEvent && e.id === editingEvent.id) return false;
          return checkTimeOverlap(adjustedStart, adjustedEnd, e.start_time, e.end_time);
        });
        if (recheck.length > 0) {
          setError(t("schedule:dialog_validation_no_slot"));
          return;
        }
        needsAdjustment = true;
      }
    }

    const payload = {
      start_date: form.start_date,
      start_time: needsAdjustment ? adjustedStart : form.start_time,
      end_time: needsAdjustment ? adjustedEnd : form.end_time,
      source_type: form.source_type,
      playlist_id: form.playlist_id,
      auto_dj_mode: form.auto_dj_mode,
      auto_dj_avoid_repeat: form.auto_dj_avoid_repeat,
      auto_dj_min_gap: form.auto_dj_min_gap,
      auto_dj_songs_ahead: form.auto_dj_songs_ahead,
      recurrence_type: form.recurrence_type,
      recurrence_interval: form.recurrence_interval,
      recurrence_days: form.recurrence_days,
      recurrence_end_date: form.recurrence_end_date,
      recurrence_count: form.recurrence_count,
      title: form.title || null,
    };

    try {
      if (editingEvent) {
        await updateEvent.mutateAsync({ id: editingEvent.id, data: payload });
      } else {
        await createEvent.mutateAsync(payload);
      }
      onClose();
    } catch (err: unknown) {
      const httpErr = isHttpError(err);
      const msg = httpErr?.message || t("schedule:dialog_validation_save_failed");
      if (msg.includes("overlap") || msg.includes("Conflict")) {
        setError(t("schedule:dialog_validation_overlap", { message: msg }));
      } else {
        setError(msg);
      }
    }
  }, [form, editingEvent, events, t, updateEvent, createEvent, onClose]);

  return {
    form,
    setForm,
    error,
    setError,
    selectedPlaylist,
    isPlaylistSource,
    endTimeAutoCalc,
    formOvernight,
    isPastEvent,
    isActiveEvent,
    isReadOnly,
    isAddingToPast,
    queueEndWarning,
    conflictInfo,
    handleSave,
    createPending: createEvent.isPending,
    updatePending: updateEvent.isPending,
  };
}
