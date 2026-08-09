import { fireEvent } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { fakeStationSong } from "@/mocks/data";
import { QueueAddDialog } from "@/pages/stations/queue-add-dialog";
import { render, screen } from "@/test/test-utils";
import type { StationSong } from "@/types";

function makeDialog(librarySongs: StationSong[]) {
  const props = {
    open: true,
    librarySongs,
    isPending: false,
    onAdd: vi.fn().mockResolvedValue(undefined),
    onClose: vi.fn(),
  };
  const utils = render(<QueueAddDialog {...props} />);
  return { ...utils, onAdd: props.onAdd, onClose: props.onClose };
}

function expandArtist(name: string) {
  fireEvent.click(screen.getAllByText(name)[0]);
}

function expandAlbum(album: string) {
  fireEvent.click(screen.getAllByText(album)[0]);
}

describe("QueueAddDialog", () => {
  it("lists artists from the station library with song counts", () => {
    const songs = [
      fakeStationSong({ song_id: "s-1", title: "First", artist: "Artist One", album: "Album A", duration: 120 }),
      fakeStationSong({ song_id: "s-2", title: "Second", artist: "Artist One", album: "Album B", duration: 90 }),
      fakeStationSong({ song_id: "s-3", title: "Third", artist: "Artist Two", album: "", duration: 150 }),
    ];

    makeDialog(songs);

    expect(screen.getByText("Artist One")).toBeInTheDocument();
    expect(screen.getByText("2 song")).toBeInTheDocument();
    expect(screen.getByText("1 song")).toBeInTheDocument();
  });

  it("shows the empty message when the library has no songs", () => {
    makeDialog([]);

    expect(screen.getByText("No songs in the station library.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /add/i })).toBeDisabled();
  });

  it("adds individually selected songs to the queue", async () => {
    const first = fakeStationSong({ song_id: "s-1", title: "Alpha", artist: "Artist A", album: "Album A" });
    const second = fakeStationSong({ song_id: "s-2", title: "Beta", artist: "Artist B", album: "Album B" });
    const { onAdd } = makeDialog([first, second]);

    expandArtist("Artist A");
    expandAlbum("Album A");
    fireEvent.click(screen.getByText("Alpha"));
    expandArtist("Artist B");
    expandAlbum("Album B");
    fireEvent.click(screen.getByText("Beta"));

    const addButton = screen.getByRole("button", { name: "Add 2 song" });
    fireEvent.click(addButton);

    expect(onAdd).toHaveBeenCalledTimes(1);
    expect(onAdd).toHaveBeenCalledWith(["s-1", "s-2"]);
  });

  it("selects all songs of an artist via the artist checkbox", async () => {
    const songs = [
      fakeStationSong({ song_id: "s-1", title: "One", artist: "Artist A", album: "Album A" }),
      fakeStationSong({ song_id: "s-2", title: "Two", artist: "Artist A", album: "Album A" }),
      fakeStationSong({ song_id: "s-3", title: "Three", artist: "Artist A", album: "Album B" }),
    ];
    const { onAdd } = makeDialog(songs);

    fireEvent.click(screen.getAllByRole("checkbox")[0]);

    expect(screen.getByText("Selected: 3")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Add 3 song" }));
    expect(onAdd).toHaveBeenCalledTimes(1);
    expect(onAdd).toHaveBeenCalledWith(["s-1", "s-2", "s-3"]);
  });

  it("selects all songs of an album via the album checkbox", async () => {
    const songs = [
      fakeStationSong({ song_id: "s-1", title: "One", artist: "Artist A", album: "Album One" }),
      fakeStationSong({ song_id: "s-2", title: "Two", artist: "Artist A", album: "Album One" }),
      fakeStationSong({ song_id: "s-3", title: "Three", artist: "Artist A", album: "Album Two" }),
    ];
    const { onAdd } = makeDialog(songs);

    expandArtist("Artist A");
    fireEvent.click(screen.getAllByRole("checkbox")[1]);

    expect(screen.getByText("Selected: 2")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Add 2 song" }));
    expect(onAdd).toHaveBeenCalledTimes(1);
    expect(onAdd).toHaveBeenCalledWith(["s-1", "s-2"]);
  });

  it("clears the selection when using the clear button", () => {
    const songs = [
      fakeStationSong({ song_id: "s-1", title: "One", artist: "Artist A", album: "Album A" }),
      fakeStationSong({ song_id: "s-2", title: "Two", artist: "Artist A", album: "Album B" }),
    ];
    makeDialog(songs);

    fireEvent.click(screen.getAllByRole("checkbox")[0]);
    expect(screen.getByText("Selected: 2")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(screen.queryByText("Selected: 2")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /add/i }).hasAttribute("disabled")).toBe(true);
  });

  it("searches songs by title and adds them from the results", async () => {
    const wanted = fakeStationSong({ song_id: "s-1", title: "Unique Hit", artist: "Artist A", album: "Album A" });
    const other = fakeStationSong({ song_id: "s-2", title: "Plain Track", artist: "Artist B", album: "Album B" });
    const { onAdd } = makeDialog([wanted, other]);

    const searchInput = screen.getByPlaceholderText("Search by title, artist, or album...");
    fireEvent.change(searchInput, { target: { value: "Unique" } });

    expect(screen.getByText("Unique Hit")).toBeInTheDocument();
    expect(screen.queryByText("Plain Track")).not.toBeInTheDocument();

    fireEvent.click(screen.getByText("Unique Hit"));
    fireEvent.click(screen.getByRole("button", { name: "Add 1 song" }));

    expect(onAdd).toHaveBeenCalledTimes(1);
    expect(onAdd).toHaveBeenCalledWith(["s-1"]);
  });

  it("paginates artists beyond the first page", () => {
    const songs = Array.from({ length: 16 }, (_, i) =>
      fakeStationSong({ song_id: `s-${i}`, title: `Song ${i}`, artist: `Artist ${i}`, album: "Album A" }),
    );
    makeDialog(songs);

    expect(screen.getByText("Artist 0")).toBeInTheDocument();
    expect(screen.queryByText("Artist 9")).not.toBeInTheDocument();

    fireEvent.click(screen.getByText("2"));
    expect(screen.getByText("Artist 9")).toBeInTheDocument();
  });
});
