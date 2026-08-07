import { Add, ArrowBack, ArrowForward, Today } from "@mui/icons-material";
import { Box, Button, Typography } from "@mui/material";
import type { TFunction } from "i18next";

export function WeekNavigation({
  weekDates,
  onPrevWeek,
  onNextWeek,
  onToday,
  onAddEvent,
  t,
  i18n,
}: {
  weekDates: Date[];
  onPrevWeek: () => void;
  onNextWeek: () => void;
  onToday: () => void;
  onAddEvent: () => void;
  t: TFunction;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  i18n: any;
}) {
  return (
    <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", mb: 2 }}>
      <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
        <Button size="small" startIcon={<ArrowBack />} onClick={onPrevWeek}>
          {t("schedule:prev")}
        </Button>
        <Button size="small" startIcon={<ArrowForward />} onClick={onNextWeek}>
          {t("schedule:next")}
        </Button>
        <Button size="small" startIcon={<Today />} onClick={onToday}>
          {t("schedule:today")}
        </Button>
      </Box>
      <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
        {new Intl.DateTimeFormat(i18n.language, { day: "numeric", month: "short" }).format(weekDates[0])} –{" "}
        {new Intl.DateTimeFormat(i18n.language, { day: "numeric", month: "short", year: "numeric" }).format(
          weekDates[6],
        )}
      </Typography>
      <Button variant="contained" size="small" startIcon={<Add />} onClick={onAddEvent}>
        {t("schedule:add_event")}
      </Button>
    </Box>
  );
}
