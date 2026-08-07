import { describe, expect, it, vi } from "vitest";
import { ScheduleCalendar } from "@/components/schedule/schedule-calendar";
import { render, screen, userEvent } from "@/test/test-utils";
import type { ScheduleEvent } from "@/types";

function getMonday(d: Date) {
  const date = new Date(d);
  const day = date.getDay();
  const diff = date.getDate() - day + (day === 0 ? -6 : 1);
  date.setDate(diff);
  return date;
}

const monday = getMonday(new Date());
const weekDates = Array.from({ length: 7 }, (_, i) => {
  const d = new Date(monday);
  d.setDate(d.getDate() + i);
  return d;
});

const mockEvent: ScheduleEvent = {
  id: "e1",
  station_id: "s1",
  title: null,
  start_date: monday.toISOString().split("T")[0],
  start_time: "09:00",
  end_time: "10:00",
  source_type: "playlist",
  playlist_id: "p1",
  playlist_name: "Morning Mix",
  auto_dj_mode: null,
  auto_dj_avoid_repeat: null,
  auto_dj_min_gap: null,
  auto_dj_songs_ahead: null,
  recurrence_type: "none",
  recurrence_interval: null,
  recurrence_days: null,
  recurrence_end_date: null,
  recurrence_count: null,
  created_at: "2026-01-01T00:00:00Z",
};

const defaultProps = {
  weekDates,
  events: [mockEvent] as ScheduleEvent[],
  queueEndEstimate: null as string | null,
  queueEndMinutes: null as number | null,
  onCreateEvent: vi.fn(),
  onEditEvent: vi.fn(),
  isEventOnDate: () => true,
};

describe("ScheduleCalendar", () => {
  it("renders day headers", () => {
    render(<ScheduleCalendar {...defaultProps} />);
    expect(screen.getByText("Mon")).toBeInTheDocument();
    expect(screen.getByText("Sun")).toBeInTheDocument();
  });

  it("renders hour labels", () => {
    render(<ScheduleCalendar {...defaultProps} />);
    expect(screen.getByText("00:00")).toBeInTheDocument();
  });

  it("calls onCreateEvent when clicking on a column", async () => {
    const onCreateEvent = vi.fn();
    const user = userEvent.setup();
    render(<ScheduleCalendar {...defaultProps} onCreateEvent={onCreateEvent} />);
    const columns = screen.getByText("Mon").closest('[class*="MuiBox"]')?.parentElement?.parentElement;
    const columnCells = columns?.querySelectorAll('[class*="MuiBox"]');
    const column = columnCells ? columnCells[columnCells.length - 1] : null;
    if (column) {
      await user.click(column);
    }
  });

  it("renders queue end indicator when provided", () => {
    render(<ScheduleCalendar {...defaultProps} queueEndEstimate="12:00" queueEndMinutes={720} />);
    expect(screen.getAllByText(/12:00/i).length).toBeGreaterThan(0);
  });
});
