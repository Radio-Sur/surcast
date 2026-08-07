import { describe, expect, it } from "vitest";
import {
  checkTimeOverlap,
  DAY_NAMES,
  fmtDuration,
  formatDate,
  getEventsForDate,
  getWeekStart,
  HOURS,
  isDateInRecurrence,
  minutesToTime,
  RECURRENCE_LABELS,
  SOURCE_TYPE_LABELS,
  timeToMinutes,
} from "@/components/schedule/schedule-utils";

describe("timeToMinutes", () => {
  it("converts HH:MM to minutes", () => {
    expect(timeToMinutes("00:00")).toBe(0);
    expect(timeToMinutes("01:00")).toBe(60);
    expect(timeToMinutes("02:30")).toBe(150);
    expect(timeToMinutes("23:59")).toBe(1439);
  });
});

describe("minutesToTime", () => {
  it("converts minutes to HH:MM", () => {
    expect(minutesToTime(0)).toBe("00:00");
    expect(minutesToTime(60)).toBe("01:00");
    expect(minutesToTime(150)).toBe("02:30");
    expect(minutesToTime(1440)).toBe("00:00");
    expect(minutesToTime(1500)).toBe("01:00");
  });
});

describe("fmtDuration", () => {
  it("formats seconds", () => {
    expect(fmtDuration(0)).toBe("0 min");
    expect(fmtDuration(1800)).toBe("30 min");
    expect(fmtDuration(3600)).toBe("1h 0m");
    expect(fmtDuration(5400)).toBe("1h 30m");
  });
});

describe("getWeekStart", () => {
  it("returns Monday of the same week", () => {
    const wed = new Date("2026-07-22T12:00:00");
    const monday = getWeekStart(wed);
    expect(monday.getDay()).toBe(1);
    expect(monday.getDate()).toBe(20);
    expect(monday.getHours()).toBe(0);
    expect(monday.getMinutes()).toBe(0);
  });
});

describe("formatDate", () => {
  it("formats date as YYYY-MM-DD", () => {
    expect(formatDate(new Date("2026-07-22"))).toBe("2026-07-22");
    expect(formatDate(new Date("2026-01-05"))).toBe("2026-01-05");
  });
});

describe("checkTimeOverlap", () => {
  it("detects overlapping times", () => {
    expect(checkTimeOverlap("09:00", "10:00", "09:30", "10:30")).toBe(true);
    expect(checkTimeOverlap("09:00", "10:00", "10:00", "11:00")).toBe(false);
    expect(checkTimeOverlap("09:00", "10:00", "08:00", "09:00")).toBe(false);
  });

  it("handles overnight events", () => {
    expect(checkTimeOverlap("22:00", "02:00", "23:00", "01:00")).toBe(true);
    expect(checkTimeOverlap("22:00", "02:00", "03:00", "04:00")).toBe(false);
  });
});

describe("isDateInRecurrence", () => {
  const baseEvent = {
    id: "1",
    start_date: "2026-01-01",
    start_time: "09:00",
    end_time: "10:00",
    source_type: "playlist" as const,
    playlist_id: "p1",
    auto_dj_mode: null,
    auto_dj_avoid_repeat: null,
    auto_dj_min_gap: null,
    auto_dj_songs_ahead: null,
    recurrence_type: "none" as const,
    recurrence_interval: null,
    recurrence_days: null,
    recurrence_end_date: null,
    recurrence_count: null,
    title: null,
    playlist_name: "Test",
    station_id: "s1",
    created_at: "2026-01-01T00:00:00Z",
  };

  it("matches same date for one-time events", () => {
    expect(isDateInRecurrence(baseEvent, new Date("2026-01-01"))).toBe(true);
    expect(isDateInRecurrence(baseEvent, new Date("2026-01-02"))).toBe(false);
  });

  it("matches daily recurrence", () => {
    const event = { ...baseEvent, recurrence_type: "daily" as const };
    expect(isDateInRecurrence(event, new Date("2026-01-01"))).toBe(true);
    expect(isDateInRecurrence(event, new Date("2026-06-01"))).toBe(true);
  });

  it("respects recurrence end date", () => {
    const event = { ...baseEvent, recurrence_type: "daily" as const, recurrence_end_date: "2026-01-10" };
    expect(isDateInRecurrence(event, new Date("2026-01-05"))).toBe(true);
    expect(isDateInRecurrence(event, new Date("2026-01-15"))).toBe(false);
  });

  it("does not match dates before start", () => {
    const event = { ...baseEvent, recurrence_type: "daily" as const };
    expect(isDateInRecurrence(event, new Date("2025-12-31"))).toBe(false);
  });
});

describe("getEventsForDate", () => {
  it("filters events for a given date", () => {
    const events = [
      { ...baseEvent(), start_date: "2026-01-01" },
      { ...baseEvent(), id: "2", start_date: "2026-01-02" },
    ];

    const result = getEventsForDate(events, new Date("2026-01-01"));
    expect(result).toHaveLength(1);
    expect(result[0].id).toBe("1");
  });

  function baseEvent() {
    return {
      id: "1",
      start_date: "2026-01-01",
      start_time: "09:00",
      end_time: "10:00",
      source_type: "playlist" as const,
      playlist_id: "p1",
      auto_dj_mode: null,
      auto_dj_avoid_repeat: null,
      auto_dj_min_gap: null,
      auto_dj_songs_ahead: null,
      recurrence_type: "none" as const,
      recurrence_interval: null,
      recurrence_days: null,
      recurrence_end_date: null,
      recurrence_count: null,
      title: null,
      playlist_name: null,
      station_id: "s1",
      created_at: "2026-01-01T00:00:00Z",
    };
  }
});

describe("constants", () => {
  it("DAY_NAMES has 7 days", () => {
    expect(DAY_NAMES).toHaveLength(7);
    expect(DAY_NAMES[0]).toBe("Monday");
  });

  it("HOURS has 24 entries", () => {
    expect(HOURS).toHaveLength(24);
  });

  it("SOURCE_TYPE_LABELS contains expected keys", () => {
    const keys = Object.keys(SOURCE_TYPE_LABELS);
    expect(keys).toContain("playlist");
    expect(keys).toContain("station_library");
    expect(keys).toContain("global_library");
    expect(keys).toContain("weighted_playlists");
  });

  it("RECURRENCE_LABELS contains expected keys", () => {
    const keys = Object.keys(RECURRENCE_LABELS);
    expect(keys).toContain("none");
    expect(keys).toContain("daily");
    expect(keys).toContain("weekly");
    expect(keys).toContain("monthly");
  });
});
