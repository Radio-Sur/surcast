import { Delete, Edit } from "@mui/icons-material";
import { Box, IconButton, Paper, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";
import type { ScheduleEvent } from "@/types";
import { timeToMinutes } from "./schedule-utils";

export function ScheduleEventList({
  events,
  isLoading,
  onEditEvent,
  onDeleteEvent,
}: {
  events?: ScheduleEvent[];
  isLoading: boolean;
  onEditEvent: (ev: ScheduleEvent) => void;
  onDeleteEvent: (ev: ScheduleEvent) => void;
}) {
  const { t } = useTranslation();

  return (
    <Box sx={{ mb: 3 }}>
      <Typography variant="h6" sx={{ mb: 1 }}>
        {t("schedule:scheduled_events")}
      </Typography>
      {isLoading ? (
        <Typography variant="body2" color="text.secondary">
          {t("common:loading")}
        </Typography>
      ) : events && events.length > 0 ? (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
          {events.map((ev) => {
            const label = ev.title || ev.playlist_name || t(`schedule:source_type_${ev.source_type}`);
            const isOvernight = timeToMinutes(ev.end_time) <= timeToMinutes(ev.start_time);
            return (
              <Paper
                key={ev.id}
                variant="outlined"
                sx={{ p: 1.5, borderRadius: 2, display: "flex", alignItems: "center", gap: 2 }}
              >
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>
                    {label}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    {t("schedule:event_info", { date: ev.start_date, start: ev.start_time, end: ev.end_time })}
                    {ev.recurrence_type !== "none" && ` · ${t(`schedule:recurrence_${ev.recurrence_type}`)}`}
                    {isOvernight && ` · ${t("common:continues_next_day")}`}
                  </Typography>
                </Box>
                <IconButton size="small" onClick={() => onEditEvent(ev)}>
                  <Edit fontSize="small" />
                </IconButton>
                <IconButton size="small" onClick={() => onDeleteEvent(ev)} color="error">
                  <Delete fontSize="small" />
                </IconButton>
              </Paper>
            );
          })}
        </Box>
      ) : (
        <Typography variant="body2" color="text.secondary">
          {t("schedule:empty_week")}
        </Typography>
      )}
    </Box>
  );
}
