import { describe, expect, it, vi } from "vitest";
import { SimpleSongRow } from "@/components/queue/simple-song-row";
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

describe("SimpleSongRow", () => {
  it("renders song title", () => {
    render(<SimpleSongRow song={mockSong} index={0} />);
    expect(screen.getByText("Test Song")).toBeInTheDocument();
  });

  it("shows more button when onDelete is provided", () => {
    render(<SimpleSongRow song={mockSong} index={0} onDelete={vi.fn()} />);
    expect(screen.getByRole("button")).toBeInTheDocument();
  });

  it("shows re-add button when dimmed and onReAdd is provided", () => {
    render(<SimpleSongRow song={mockSong} index={0} dimmed onReAdd={vi.fn()} />);
    expect(screen.getByRole("button")).toBeInTheDocument();
  });
});
