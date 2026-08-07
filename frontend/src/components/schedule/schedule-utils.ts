import type { RecurrenceType, ScheduleEvent, ScheduleSourceType } from "@/types";

export const DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
export const HOURS = Array.from({ length: 24 }, (_, i) => i);
export const HOUR_HEIGHT = 36;
export const COLUMN_HEIGHT = HOUR_HEIGHT * 24;

export const SOURCE_TYPE_LABELS: Record<ScheduleSourceType, string> = {
  playlist: "Playlist",
  station_library: "Station library",
  global_library: "Global library",
  weighted_playlists: "Weighted playlists",
};

export const RECURRENCE_LABELS: Record<RecurrenceType, string> = {
  none: "Once",
  daily: "Daily",
  every_n_days: "Every N days",
  weekly: "Weekly",
  biweekly: "Biweekly",
  monthly: "Monthly",
  custom_days: "Custom days",
};

export function timeToMinutes(t: string) {
  const [h, m] = t.split(":").map(Number);
  return h * 60 + m;
}

export function minutesToTime(min: number) {
  const h = Math.floor(min / 60) % 24;
  const m = min % 60;
  return `${h.toString().padStart(2, "0")}:${m.toString().padStart(2, "0")}`;
}

export function fmtDuration(sec: number) {
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  return `${m} min`;
}

export function getWeekStart(date: Date): Date {
  const d = new Date(date);
  const day = d.getDay();
  const diff = d.getDate() - day + (day === 0 ? -6 : 1);
  d.setDate(diff);
  d.setHours(0, 0, 0, 0);
  return d;
}

export function formatDate(date: Date): string {
  const year = date.getFullYear();
  const month = (date.getMonth() + 1).toString().padStart(2, "0");
  const day = date.getDate().toString().padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export function isDateInRecurrence(event: ScheduleEvent, date: Date): boolean {
  const eventDate = new Date(`${event.start_date}T00:00:00`);
  if (date < eventDate) return false;
  if (event.recurrence_end_date) {
    const end = new Date(`${event.recurrence_end_date}T00:00:00`);
    if (date > end) return false;
  }
  const diffDays = Math.floor((date.getTime() - eventDate.getTime()) / (1000 * 60 * 60 * 24));
  switch (event.recurrence_type) {
    case "none":
      return date.toDateString() === eventDate.toDateString();
    case "daily":
      return true;
    case "every_n_days": {
      const interval = event.recurrence_interval || 1;
      return diffDays >= 0 && diffDays % interval === 0;
    }
    case "weekly":
      return diffDays >= 0 && diffDays % 7 === 0;
    case "biweekly":
      return diffDays >= 0 && diffDays % 14 === 0;
    case "monthly":
      return date.getDate() === eventDate.getDate();
    case "custom_days": {
      const dayIdx = (date.getDay() + 6) % 7;
      return event.recurrence_days?.includes(dayIdx) ?? false;
    }
    default:
      return false;
  }
}

export function getEventsForDate(events: ScheduleEvent[], date: Date): ScheduleEvent[] {
  return events.filter((e) => isDateInRecurrence(e, date));
}

export function checkTimeOverlap(aStart: string, aEnd: string, bStart: string, bEnd: string) {
  const aS = timeToMinutes(aStart);
  const aE = timeToMinutes(aEnd);
  const bS = timeToMinutes(bStart);
  const bE = timeToMinutes(bEnd);
  const aOvernight = aE <= aS;
  const bOvernight = bE <= bS;
  if (!aOvernight && !bOvernight) return aS < bE && aE > bS;
  if (aOvernight && !bOvernight) return bS < aE || bE > aS;
  if (!aOvernight && bOvernight) return aS < bE || aE > bS;
  return true;
}

export function isPastEvent(event: ScheduleEvent): boolean {
  const today = formatDate(new Date());
  if (event.start_date < today) return true;
  if (event.start_date === today) {
    const nowMinutes = new Date().getHours() * 60 + new Date().getMinutes();
    const endMinutes = timeToMinutes(event.end_time);
    return nowMinutes >= endMinutes;
  }
  return false;
}

export function isActiveEvent(event: ScheduleEvent): boolean {
  const today = formatDate(new Date());
  if (event.start_date !== today) return false;
  const nowMinutes = new Date().getHours() * 60 + new Date().getMinutes();
  const startMinutes = timeToMinutes(event.start_time);
  const endMinutes = timeToMinutes(event.end_time);
  const adjustedEnd = endMinutes <= startMinutes ? endMinutes + 1440 : endMinutes;
  return nowMinutes >= startMinutes && nowMinutes < adjustedEnd;
}

export function isAddingToPast(startDate: string, startTime: string, editingEvent: ScheduleEvent | null): boolean {
  if (editingEvent) return false;
  const today = formatDate(new Date());
  if (startDate !== today) return false;
  const currMin = new Date().getHours() * 60 + new Date().getMinutes();
  return timeToMinutes(startTime) <= currMin;
}

export function computeQueueEndWarning(
  queueEndEstimate: string | null | undefined,
  startDate: string,
  startTime: string,
  isReadOnly: boolean,
): { adj: number } | null {
  const queueEndMinutes = queueEndEstimate
    ? (() => {
        const now = new Date();
        const currMin = now.getHours() * 60 + now.getMinutes();
        const raw = timeToMinutes(queueEndEstimate);
        return raw < currMin ? raw + 24 * 60 : raw;
      })()
    : null;
  if (queueEndMinutes == null || isReadOnly) return null;
  if (!startDate) return null;
  const today = formatDate(new Date());
  if (startDate !== today) return null;
  const startMin = timeToMinutes(startTime);
  if (startMin < queueEndMinutes) {
    return { adj: queueEndMinutes };
  }
  return null;
}

interface ConflictInfo {
  conflict: ScheduleEvent;
  adjStart: string | null;
  adjEnd: string | null;
}

export function computeConflictInfo(
  events: ScheduleEvent[] | undefined,
  startDate: string,
  startTime: string,
  endTime: string,
  editingEvent: ScheduleEvent | null,
  isReadOnly: boolean,
): ConflictInfo | null {
  if (!events || !startDate || !startTime || !endTime) return null;
  if (isReadOnly) return null;
  const formDate = new Date(`${startDate}T00:00:00`);
  const dayEvents = getEventsForDate(events, formDate).filter((e) => {
    if (editingEvent && e.id === editingEvent.id) return false;
    return checkTimeOverlap(startTime, endTime, e.start_time, e.end_time);
  });
  if (dayEvents.length === 0) return null;
  dayEvents.sort((a, b) => timeToMinutes(a.start_time) - timeToMinutes(b.start_time));
  const last = dayEvents[dayEvents.length - 1];
  const conflictEnd = timeToMinutes(last.end_time);
  const endAdj = last.end_time <= last.start_time ? conflictEnd + 1440 : conflictEnd;
  const duration = timeToMinutes(endTime) - timeToMinutes(startTime);
  if (duration <= 0) return null;

  const newStart = endAdj;
  const adjStart = `${Math.floor(newStart / 60) % 24}:${(newStart % 60).toString().padStart(2, "0")}`;
  const adjEnd = `${Math.floor((newStart + duration) / 60) % 24}:${((newStart + duration) % 60).toString().padStart(2, "0")}`;

  const recheck = getEventsForDate(events, formDate).filter((e) => {
    if (editingEvent && e.id === editingEvent.id) return false;
    return checkTimeOverlap(adjStart, adjEnd, e.start_time, e.end_time);
  });
  if (recheck.length > 0) return { conflict: last, adjStart: null, adjEnd: null };
  return { conflict: last, adjStart, adjEnd };
}
