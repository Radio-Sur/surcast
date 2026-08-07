import { describe, expect, it, vi } from "vitest";
import { SongRow } from "@/components/queue/song-row";
import { render, screen } from "@/test/test-utils";
import type { QueueItem } from "@/types";

const mockSong: QueueItem = {
  id: "q1",
  station_id: "s1",
  song_id: "s1",
  position: 0,
  title: "Test Song",
  artist: "Test Artist",
  album: "Test Album",
  duration: 200,
  has_cover: false,
  mime_type: "audio/mpeg",
  origin_playlist_id: null,
  playlist_name: null,
  is_auto_dj: false,
};

describe("SongRow", () => {
  it("renders song title and artist", () => {
    render(<SongRow song={mockSong} index={0} />);
    expect(screen.getByText("Test Song")).toBeInTheDocument();
    expect(screen.getByText(/Test Artist/)).toBeInTheDocument();
  });

  it("shows duration", () => {
    render(<SongRow song={mockSong} index={0} />);
    expect(screen.getByText("3:20")).toBeInTheDocument();
  });

  it("shows checkbox when not dimmed", () => {
    render(<SongRow song={mockSong} index={0} />);
    expect(screen.getByRole("checkbox")).toBeInTheDocument();
  });

  it("hides checkbox when dimmed", () => {
    render(<SongRow song={mockSong} index={0} dimmed />);
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("shows re-add button when onReAdd is provided", () => {
    render(<SongRow song={mockSong} index={0} onReAdd={vi.fn()} />);
    const reAddBtn = screen.getByRole("button");
    expect(reAddBtn).toBeInTheDocument();
  });

  it("shows placeholder when song has no duration", () => {
    render(<SongRow song={{ ...mockSong, duration: 0 }} index={0} />);
    expect(screen.getByText("--:--")).toBeInTheDocument();
  });
});
