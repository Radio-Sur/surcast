import { describe, expect, it, vi } from "vitest";
import { LibraryTab } from "@/pages/stations/library-tab";
import { render, screen, userEvent } from "@/test/test-utils";

const mockSongs = [
  {
    id: "ss1",
    station_id: "s1",
    song_id: "song1",
    title: "Song One",
    artist: "Artist A",
    album: "Album X",
    duration: 200,
    has_cover: false,
    mime_type: "audio/mpeg",
    added_at: "2026-01-01T00:00:00Z",
  },
  {
    id: "ss2",
    station_id: "s1",
    song_id: "song2",
    title: "Song Two",
    artist: "Artist B",
    album: "Album Y",
    duration: 180,
    has_cover: false,
    mime_type: "audio/mpeg",
    added_at: "2026-01-01T00:00:00Z",
  },
];

describe("LibraryTab", () => {
  const defaultProps = {
    stationId: "s1",
    onRemove: vi.fn(),
    librarySongTotal: 0,
    libraryPage: 0,
    libraryPerPage: 50,
    onLibraryPageChange: vi.fn(),
    onLibraryPerPageChange: vi.fn(),
  };

  it("shows loading skeleton", () => {
    const { container } = render(<LibraryTab {...defaultProps} librarySongs={undefined} libraryLoading={true} />);
    expect(container.querySelector('[class*="MuiSkeleton"]')).toBeInTheDocument();
  });

  it("shows empty state when no songs", () => {
    render(<LibraryTab {...defaultProps} librarySongs={[]} libraryLoading={false} />);
    expect(screen.getByText(/no songs/i)).toBeInTheDocument();
  });

  it("renders song rows", () => {
    render(
      <LibraryTab
        {...defaultProps}
        librarySongs={mockSongs}
        librarySongTotal={mockSongs.length}
        libraryLoading={false}
      />,
    );
    expect(screen.getByText("Song One")).toBeInTheDocument();
    expect(screen.getByText("Song Two")).toBeInTheDocument();
    expect(screen.getByText("Artist A")).toBeInTheDocument();
    expect(screen.getByText("Album X")).toBeInTheDocument();
  });

  it("opens add dialog on button click", async () => {
    render(
      <LibraryTab
        {...defaultProps}
        librarySongs={mockSongs}
        librarySongTotal={mockSongs.length}
        libraryLoading={false}
      />,
    );
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /add/i }));
    expect(screen.getByText("Add to Station Library")).toBeInTheDocument();
  });

  it("calls onRemove when delete clicked", async () => {
    const onRemove = vi.fn();
    render(
      <LibraryTab
        {...defaultProps}
        librarySongs={mockSongs}
        librarySongTotal={mockSongs.length}
        libraryLoading={false}
        onRemove={onRemove}
      />,
    );
    userEvent.setup();
    const deleteBtns = screen.getAllByRole("button");
    const deleteBtn = deleteBtns.find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeTruthy();
  });
});
