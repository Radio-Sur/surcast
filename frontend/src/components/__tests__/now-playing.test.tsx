import { describe, expect, it, vi } from "vitest";
import { NowPlaying } from "@/components/queue/now-playing";
import { render, screen } from "@/test/test-utils";
import type { PlaylistGroup, QueueItem, StreamStatus } from "@/types";

const mockSong: QueueItem = {
  id: "q1",
  station_id: "s1",
  song_id: "s1",
  position: 0,
  title: "Bohemian Rhapsody",
  artist: "Queen",
  album: "A Night at the Opera",
  duration: 354,
  has_cover: false,
  mime_type: "audio/mpeg",
  origin_playlist_id: null,
  playlist_name: null,
  is_auto_dj: false,
};

const mockPlaylistGroup: PlaylistGroup = {
  kind: "playlist_group",
  playlist_id: "p1",
  playlist_name: "Morning Mix",
  songs: [
    { ...mockSong, id: "s1", position: 0, title: "Song One" },
    { ...mockSong, id: "s2", position: 1, title: "Song Two" },
  ],
  total_duration: 600,
  current_song_index: 0,
};

const mockStatus: StreamStatus = {
  playing: true,
  song_index: 0,
  total: 5,
  elapsed: 30,
  title: "Bohemian Rhapsody",
  artist: "Queen",
  duration: 354,
};

const defaultProps = {
  streamStatus: null as StreamStatus | null,
  connected: true,
  elapsed: 30,
  onSkip: vi.fn(),
  isSkipping: false,
};

describe("NowPlaying", () => {
  it("renders song title for a single song", () => {
    render(<NowPlaying item={mockSong} {...defaultProps} />);
    expect(screen.getByText("Bohemian Rhapsody")).toBeInTheDocument();
  });

  it("renders playlist name for a playlist group", () => {
    render(<NowPlaying item={mockPlaylistGroup} {...defaultProps} />);
    expect(screen.getByText("Morning Mix")).toBeInTheDocument();
  });

  it("shows progress bar when streamStatus is provided", () => {
    render(<NowPlaying item={mockSong} {...defaultProps} streamStatus={mockStatus} />);
    expect(screen.getByRole("progressbar")).toBeInTheDocument();
  });

  it("shows skip button", () => {
    render(<NowPlaying item={mockSong} {...defaultProps} />);
    expect(screen.getByRole("button")).toBeInTheDocument();
  });

  it("shows connected status indicator", () => {
    render(<NowPlaying item={mockSong} {...defaultProps} connected={true} />);
    expect(screen.getByText("Bohemian Rhapsody")).toBeInTheDocument();
  });
});
