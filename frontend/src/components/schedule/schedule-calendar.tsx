import { Box, Paper, Tooltip, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";
import type { ScheduleEvent } from "@/types";
import { COLUMN_HEIGHT, HOUR_HEIGHT, HOURS, timeToMinutes } from "./schedule-utils";

function EventBlock({ event, onClick }: { event: ScheduleEvent; onClick: (event: ScheduleEvent) => void }) {
  const { t } = useTranslation();
  const startMinutes = timeToMinutes(event.start_time);
  const rawEndMinutes = timeToMinutes(event.end_time);
  const endMinutes = rawEndMinutes <= startMinutes ? rawEndMinutes + 1440 : rawEndMinutes;
  const top = (startMinutes / 60) * HOUR_HEIGHT;
  const height = Math.max(((endMinutes - startMinutes) / 60) * HOUR_HEIGHT, 12);
  const label = event.title || event.playlist_name || t(`schedule:source_type_${event.source_type}`);

  return (
    <Tooltip
      title={
        <Box>
          <Typography variant="body2">{label}</Typography>
          <Typography variant="body2">
            {event.start_time} – {event.end_time}
          </Typography>
          {event.recurrence_type !== "none" && (
            <Typography variant="body2">{t(`schedule:recurrence_${event.recurrence_type}`)}</Typography>
          )}
          {rawEndMinutes <= startMinutes && <Typography variant="body2">{t("common:continues_next_day")}</Typography>}
        </Box>
      }
      arrow
    >
      <Box
        onClick={(e) => {
          e.stopPropagation();
          onClick(event);
        }}
        sx={{
          position: "absolute",
          top,
          left: 1,
          right: 1,
          height,
          bgcolor: "primary.main",
          color: "primary.contrastText",
          borderRadius: 0.5,
          px: 0.5,
          py: 0.25,
          cursor: "pointer",
          fontSize: "0.65rem",
          lineHeight: 1.2,
          overflow: "hidden",
          "&:hover": { opacity: 0.85 },
          zIndex: 1,
        }}
      >
        {label}
        {rawEndMinutes <= startMinutes && (
          <Typography variant="caption" sx={{ fontSize: "0.55rem", opacity: 0.9, display: "block" }}>
            {t("common:continues_next_day")}
          </Typography>
        )}
        {event.recurrence_type !== "none" && (
          <Typography variant="caption" sx={{ fontSize: "0.55rem", opacity: 0.8, display: "block" }}>
            {t(`schedule:recurrence_${event.recurrence_type}`)}
          </Typography>
        )}
      </Box>
    </Tooltip>
  );
}

function TodayIndicators({
  queueEndEstimate,
  queueEndMinutes,
}: {
  queueEndEstimate: string | null | undefined;
  queueEndMinutes: number | null;
}) {
  const { t } = useTranslation();
  const now = new Date();
  const nowMinutes = now.getHours() * 60 + now.getMinutes();
  const nowTop = (nowMinutes / 60) * HOUR_HEIGHT;
  const queueEndMin = queueEndMinutes != null ? queueEndMinutes - (queueEndMinutes >= 1440 ? 1440 : 0) : null;
  const queueEndTop = queueEndMin != null ? (queueEndMin / 60) * HOUR_HEIGHT : null;

  return (
    <>
      <Box
        sx={{
          position: "absolute",
          top: nowTop,
          left: 0,
          right: 0,
          height: 2,
          bgcolor: "error.main",
          zIndex: 2,
          display: "flex",
          alignItems: "center",
        }}
      >
        <Typography
          variant="caption"
          sx={{
            fontSize: "0.55rem",
            color: "error.main",
            fontWeight: 700,
            bgcolor: "background.paper",
            px: 0.25,
            lineHeight: 1,
          }}
        >
          {t("schedule:calendar_now")}
        </Typography>
      </Box>
      {queueEndMin != null && queueEndMin > nowMinutes && (
        <Box
          sx={{
            position: "absolute",
            top: queueEndTop,
            left: 0,
            right: 0,
            height: 2,
            borderTop: "2px dashed",
            borderColor: "info.main",
            zIndex: 2,
            display: "flex",
            alignItems: "center",
          }}
        >
          <Typography
            variant="caption"
            sx={{
              fontSize: "0.55rem",
              color: "info.main",
              fontWeight: 700,
              bgcolor: "background.paper",
              px: 0.25,
              lineHeight: 1,
            }}
          >
            {t("schedule:calendar_queue_ends", { time: queueEndEstimate })}
          </Typography>
        </Box>
      )}
    </>
  );
}

const DAY_SHORT_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];

export function ScheduleCalendar({
  weekDates,
  events,
  queueEndEstimate,
  queueEndMinutes,
  onCreateEvent,
  onEditEvent,
  isEventOnDate,
}: {
  weekDates: Date[];
  events: ScheduleEvent[] | undefined;
  queueEndEstimate: string | null | undefined;
  queueEndMinutes: number | null;
  onCreateEvent: (dayIndex: number, hour: number) => void;
  onEditEvent: (event: ScheduleEvent) => void;
  isEventOnDate: (event: ScheduleEvent, date: Date) => boolean;
}) {
  const { t } = useTranslation();
  return (
    <Paper variant="outlined" sx={{ borderRadius: 2, overflow: "hidden", mb: 3 }}>
      <Box
        sx={{
          display: "grid",
          gridTemplateColumns: "65px repeat(7, 1fr)",
          borderBottom: "1px solid",
          borderColor: "divider",
        }}
      >
        <Box sx={{ borderRight: "1px solid", borderColor: "divider" }} />
        {weekDates.map((date, dayIndex) => {
          const today = date.toDateString() === new Date().toDateString();
          return (
            <Box
              key={dayIndex}
              sx={{
                textAlign: "center",
                py: 0.5,
                borderLeft: dayIndex > 0 ? "1px solid" : undefined,
                borderColor: "divider",
                bgcolor: today ? "primary.main" : undefined,
                color: today ? "primary.contrastText" : undefined,
              }}
            >
              <Typography variant="caption" sx={{ fontWeight: 600 }}>
                {t(`schedule:day_${DAY_SHORT_KEYS[dayIndex]}`)}
              </Typography>
              <Typography variant="caption" sx={{ display: "block", fontWeight: 700 }}>
                {date.getDate()}
              </Typography>
            </Box>
          );
        })}
      </Box>

      <Box sx={{ display: "grid", gridTemplateColumns: "65px repeat(7, 1fr)" }}>
        <Box sx={{ borderRight: "1px solid", borderColor: "divider" }}>
          {HOURS.map((hour) => (
            <Box
              key={hour}
              sx={{
                height: HOUR_HEIGHT,
                borderBottom: "1px solid",
                borderColor: "divider",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                pr: 0.5,
              }}
            >
              <Typography variant="caption" color="text.secondary" sx={{ fontSize: "0.7rem", lineHeight: 1 }}>
                {t("schedule:hour_label", { hour: hour.toString().padStart(2, "0") })}
              </Typography>
            </Box>
          ))}
        </Box>
        {weekDates.map((date, dayIndex) => {
          const today = date.toDateString() === new Date().toDateString();
          const dayEvents = events ? events.filter((e) => isEventOnDate(e, date)) : [];

          return (
            <Box
              key={dayIndex}
              sx={{
                borderLeft: dayIndex > 0 ? "1px solid" : undefined,
                borderColor: "divider",
                position: "relative",
                bgcolor: today ? "action.hover" : undefined,
              }}
            >
              <Box
                sx={{ position: "relative", height: COLUMN_HEIGHT, cursor: "pointer" }}
                onClick={(e) => {
                  const rect = e.currentTarget.getBoundingClientRect();
                  const y = e.clientY - rect.top;
                  const hour = Math.floor(y / HOUR_HEIGHT);
                  onCreateEvent(dayIndex, hour);
                }}
              >
                {HOURS.map((hour) => (
                  <Box
                    key={hour}
                    sx={{
                      height: HOUR_HEIGHT,
                      borderBottom: "1px solid",
                      borderColor: "divider",
                      opacity: 0.5,
                    }}
                  />
                ))}

                {today && <TodayIndicators queueEndEstimate={queueEndEstimate} queueEndMinutes={queueEndMinutes} />}
                {dayEvents.map((ev) => (
                  <EventBlock key={ev.id} event={ev} onClick={onEditEvent} />
                ))}
              </Box>
            </Box>
          );
        })}
      </Box>
    </Paper>
  );
}
