import { describe, expect, it } from "vitest";
import { PlaylistGroupCard } from "@/components/queue/playlist-group-card";
import { render, screen } from "@/test/test-utils";

const baseGroup = {
  kind: "playlist_group" as const,
  playlist_id: "pl1",
  playlist_name: "Test Playlist",
  songs: [
    {
      id: "q1",
      station_id: "s1",
      song_id: "s1",
      position: 0,
      title: "S1",
      artist: "A",
      album: "B",
      duration: 200,
      has_cover: false,
      mime_type: "audio/mpeg",
      origin_playlist_id: "pl1",
      playlist_name: "Test Playlist",
      is_auto_dj: false,
    },
    {
      id: "q2",
      station_id: "s1",
      song_id: "s2",
      position: 1,
      title: "S2",
      artist: "A",
      album: "B",
      duration: 200,
      has_cover: false,
      mime_type: "audio/mpeg",
      origin_playlist_id: "pl1",
      playlist_name: "Test Playlist",
      is_auto_dj: false,
    },
  ],
  total_duration: 400,
};

describe("PlaylistGroupCard", () => {
  it("renders playlist name", () => {
    render(<PlaylistGroupCard group={baseGroup} />);
    expect(screen.getByText("Test Playlist")).toBeInTheDocument();
  });
});
