import { Box, Skeleton, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";
import { durationBetween } from "@/components/queue";
import { timeToMinutes } from "@/components/schedule/schedule-utils";
import type { QueueItem, ScheduleEntry } from "@/types";

const DAY_LONG_KEYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"];

export function ScheduleInfoBanner({
  schedules,
  isLoading,
  upcoming,
  nowPlaying,
  elapsed,
}: {
  schedules: ScheduleEntry[] | undefined;
  isLoading: boolean;
  upcoming: QueueItem[];
  nowPlaying: QueueItem | null;
  elapsed: number;
}) {
  const { t } = useTranslation();
  if (isLoading) return <Skeleton variant="rounded" height={80} sx={{ mb: 2 }} />;
  if (!schedules) return null;
  if (schedules.length === 0) {
    return (
      <Box
        sx={{ mb: 2, border: 1, borderColor: "divider", borderRadius: 2, bgcolor: "background.paper", px: 2, py: 1.25 }}
      >
        <Typography variant="caption" sx={{ fontWeight: 600, color: "text.secondary", letterSpacing: 0.5 }}>
          {t("schedule:banner_no_schedules")}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {t("schedule:banner_no_schedules_hint")}
        </Typography>
      </Box>
    );
  }

  const now = new Date();
  const dayIndex = (now.getDay() + 6) % 7;
  const currentMinutes = now.getHours() * 60 + now.getMinutes();

  const activeSchedule = schedules.find((s) => {
    if (s.day_of_week !== dayIndex) return false;
    const startMinutes = timeToMinutes(s.start_time);
    const endMinutes = timeToMinutes(s.end_time);
    if (endMinutes > startMinutes) return currentMinutes >= startMinutes && currentMinutes < endMinutes;
    return currentMinutes >= startMinutes || currentMinutes < endMinutes;
  });

  const upcomingSchedules = schedules
    .filter((s) => {
      if (activeSchedule && s.id === activeSchedule.id) return false;
      const dayDiff = (s.day_of_week - dayIndex + 7) % 7;
      if (dayDiff > 0) return true;
      if (dayDiff === 0) {
        const startMinutes = timeToMinutes(s.start_time);
        return startMinutes > currentMinutes;
      }
      return false;
    })
    .sort((a, b) => {
      const dayDiffA = (a.day_of_week - dayIndex + 7) % 7;
      const dayDiffB = (b.day_of_week - dayIndex + 7) % 7;
      if (dayDiffA !== dayDiffB) return dayDiffA - dayDiffB;
      return timeToMinutes(a.start_time) - timeToMinutes(b.start_time);
    });

  const nextSchedule = upcomingSchedules[0];
  const todaySchedules = schedules.filter((s) => s.day_of_week === dayIndex);

  const upcomingTotalSeconds = upcoming.reduce((sum, s) => sum + s.duration, 0);
  const remainingCurrent = nowPlaying ? Math.max(0, nowPlaying.duration - elapsed) : 0;
  const queueEndDate = new Date(Date.now() + (remainingCurrent + upcomingTotalSeconds) * 1000);
  const queueEndHour = queueEndDate.getHours().toString().padStart(2, "0");
  const queueEndMinute = queueEndDate.getMinutes().toString().padStart(2, "0");
  const queueEndMinutesTotal = queueEndDate.getHours() * 60 + queueEndDate.getMinutes();
  const queueEndWrapped = queueEndMinutesTotal < currentMinutes ? queueEndMinutesTotal + 24 * 60 : queueEndMinutesTotal;

  const nextDayName =
    nextSchedule && nextSchedule.day_of_week !== dayIndex
      ? t(`schedule:day_${DAY_LONG_KEYS[nextSchedule.day_of_week]}`)
      : null;
  const nextDayDiff = nextSchedule ? (nextSchedule.day_of_week - dayIndex + 7) % 7 : 0;

  const queueOverlapsNext =
    nextSchedule &&
    ((nextDayDiff === 0 && queueEndWrapped > timeToMinutes(nextSchedule.start_time)) ||
      (nextDayDiff === 1 && queueEndWrapped > 1440 && queueEndMinutesTotal > timeToMinutes(nextSchedule.start_time)));

  return (
    <Box
      sx={{
        mb: 2,
        border: 1,
        borderColor: activeSchedule ? "primary.main" : "divider",
        borderRadius: 2,
        bgcolor: activeSchedule ? "primary.main" : "background.paper",
        color: activeSchedule ? "primary.contrastText" : "text.primary",
        overflow: "hidden",
      }}
    >
      <Box sx={{ px: 2, py: 1.25 }}>
        {activeSchedule ? (
          <>
            <Box sx={{ display: "flex", alignItems: "center", gap: 0.5, mb: 0.25 }}>
              <Box
                sx={{
                  width: 6,
                  height: 6,
                  borderRadius: "50%",
                  bgcolor: "primary.contrastText",
                  flexShrink: 0,
                }}
              />
              <Typography variant="caption" sx={{ opacity: 0.8, fontWeight: 600 }}>
                {t("schedule:banner_now_playing")}
              </Typography>
            </Box>
            <Typography variant="body2" sx={{ fontWeight: 600 }}>
              {activeSchedule.playlist_name ||
                t(`schedule:source_type_${activeSchedule.source_type}`) ||
                activeSchedule.source_type}
              {" · "}
              {durationBetween(activeSchedule.start_time, activeSchedule.end_time)}
              {" · "}
              {t("schedule:banner_ends_at", { time: activeSchedule.end_time })}
            </Typography>
            <Typography variant="caption" sx={{ opacity: 0.75 }}>
              {t("schedule:banner_queue_ends", { hour: queueEndHour, minute: queueEndMinute })}
            </Typography>
          </>
        ) : nextSchedule ? (
          <>
            <Typography variant="caption" sx={{ fontWeight: 600, color: "text.secondary", letterSpacing: 0.5 }}>
              {t("schedule:banner_next_scheduled")}
            </Typography>
            <Typography variant="body2" sx={{ fontWeight: 600 }}>
              {nextSchedule.playlist_name ||
                t(`schedule:source_type_${nextSchedule.source_type}`) ||
                nextSchedule.source_type}
              {nextDayName ? ` · ${nextDayName}` : ""}
              {t("schedule:banner_at", { time: nextSchedule.start_time })}
              {" · "}
              {durationBetween(nextSchedule.start_time, nextSchedule.end_time)}
            </Typography>
            {queueOverlapsNext ? (
              <Typography variant="caption" color="warning.main" sx={{ display: "block", mt: 0.25 }}>
                {t("schedule:banner_overlap_warning", { hour: queueEndHour, minute: queueEndMinute })}
              </Typography>
            ) : (
              <Typography variant="caption" color="text.secondary">
                {t("schedule:banner_ends_at", { time: nextSchedule.end_time })}
              </Typography>
            )}
          </>
        ) : todaySchedules.length > 0 ? (
          <Typography variant="body2" color="text.secondary">
            {t("schedule:banner_all_ended")}
          </Typography>
        ) : (
          <Typography variant="body2" color="text.secondary">
            {t("schedule:banner_no_upcoming")}
          </Typography>
        )}
      </Box>
    </Box>
  );
}
