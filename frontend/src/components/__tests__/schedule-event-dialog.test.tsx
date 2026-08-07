import { beforeEach, describe, expect, it, vi } from "vitest";
import { ScheduleEventDialog } from "@/components/schedule/schedule-event-dialog";
import { server } from "@/mocks/server";
import { render, screen, setupAuth } from "@/test/test-utils";
import type { Playlist, ScheduleEvent } from "@/types";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

const mockPlaylists: Playlist[] = [
  {
    id: "p1",
    name: "Morning Mix",
    slug: "morning-mix",
    description: "",
    song_count: 5,
    total_duration_seconds: 1800,
    created_by: "1",
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
  },
];

const mockEvents: ScheduleEvent[] = [];

describe("ScheduleEventDialog", () => {
  it("renders when open", () => {
    render(
      <ScheduleEventDialog
        open={true}
        editingEvent={null}
        stationId="s1"
        playlists={mockPlaylists}
        events={mockEvents}
        onClose={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    expect(screen.getByRole("heading", { name: /new event/i })).toBeInTheDocument();
  });

  it("does not render when closed", () => {
    render(
      <ScheduleEventDialog
        open={false}
        editingEvent={null}
        stationId="s1"
        playlists={mockPlaylists}
        events={mockEvents}
        onClose={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shows date and time fields", () => {
    render(
      <ScheduleEventDialog
        open={true}
        editingEvent={null}
        stationId="s1"
        playlists={mockPlaylists}
        events={mockEvents}
        onClose={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    expect(screen.getByLabelText("Date")).toBeInTheDocument();
    expect(screen.getByLabelText("Start")).toBeInTheDocument();
    expect(screen.getByLabelText("End")).toBeInTheDocument();
  });

  it("shows create button", () => {
    render(
      <ScheduleEventDialog
        open={true}
        editingEvent={null}
        stationId="s1"
        playlists={mockPlaylists}
        events={mockEvents}
        onClose={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    expect(screen.getByRole("button", { name: /create/i })).toBeInTheDocument();
  });

  it("shows edit title when editing an event", () => {
    const editEvent: ScheduleEvent = {
      id: "e1",
      station_id: "s1",
      title: null,
      start_date: "2026-01-01",
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
    render(
      <ScheduleEventDialog
        open={true}
        editingEvent={editEvent}
        stationId="s1"
        playlists={mockPlaylists}
        events={[editEvent]}
        onClose={vi.fn()}
        onDelete={vi.fn()}
      />,
    );
    expect(screen.getByRole("heading", { name: /edit event/i })).toBeInTheDocument();
  });
});
