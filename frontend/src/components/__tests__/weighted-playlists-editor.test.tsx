import { describe, expect, it, vi } from "vitest";
import { WeightedPlaylistsEditor } from "@/components/weighted-playlists-editor";
import { render, screen } from "@/test/test-utils";

const mockPlaylists = [
  {
    id: "pl1",
    name: "Rock",
    slug: "rock",
    description: "",
    song_count: 10,
    total_duration_seconds: 3600,
    created_by: "1",
    created_at: "",
    updated_at: "",
  },
  {
    id: "pl2",
    name: "Jazz",
    slug: "jazz",
    description: "",
    song_count: 5,
    total_duration_seconds: 1800,
    created_by: "1",
    created_at: "",
    updated_at: "",
  },
];

const mockEntries = [
  { id: "e1", playlist_id: "pl1", playlist_name: "Rock", weight: 50 },
  { id: "e2", playlist_id: "pl2", playlist_name: "Jazz", weight: 30 },
];

describe("WeightedPlaylistsEditor", () => {
  it("shows empty message when no entries", () => {
    render(
      <WeightedPlaylistsEditor
        playlists={mockPlaylists}
        entries={[]}
        onAdd={vi.fn()}
        onUpdateWeight={vi.fn()}
        onRemove={vi.fn()}
        isAdding={false}
      />,
    );
    expect(screen.getAllByText(/weighted/i).length).toBeGreaterThanOrEqual(1);
  });

  it("renders playlist entries", () => {
    render(
      <WeightedPlaylistsEditor
        playlists={mockPlaylists}
        entries={mockEntries}
        onAdd={vi.fn()}
        onUpdateWeight={vi.fn()}
        onRemove={vi.fn()}
        isAdding={false}
      />,
    );
    const chips = screen.getAllByRole("button");
    expect(chips.length).toBeGreaterThanOrEqual(1);
  });

  it("renders add button", () => {
    render(
      <WeightedPlaylistsEditor
        playlists={mockPlaylists}
        entries={mockEntries}
        onAdd={vi.fn()}
        onUpdateWeight={vi.fn()}
        onRemove={vi.fn()}
        isAdding={false}
      />,
    );
    expect(screen.getByTestId("AddIcon")).toBeInTheDocument();
  });
});
