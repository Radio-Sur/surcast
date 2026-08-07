import { describe, expect, it, vi } from "vitest";
import { UploadSongDialog } from "@/components/upload-song-dialog";
import { render, screen } from "@/test/test-utils";
import type { Station } from "@/types";

const mockStations: Station[] = [
  {
    id: "s1",
    name: "Station One",
    description: "",
    slug: "station-one",
    stream_url: "",
    current_song_index: 0,
    prebuffer_bytes: 0,
    played_limit: 100,
    default_fade_ms: 2000,
    transition_mode: "crossfade",
    autocue_fade_max_ms: 5000,
    created_by: "1",
    created_at: "",
    updated_at: "",
  },
];

describe("UploadSongDialog", () => {
  it("renders when open", () => {
    render(
      <UploadSongDialog
        open={true}
        stations={mockStations}
        uploadSongPending={false}
        uploadZipPending={false}
        onUploadSingle={vi.fn()}
        onUploadZip={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByRole("heading", { name: /upload/i })).toBeInTheDocument();
  });

  it("does not render when closed", () => {
    render(
      <UploadSongDialog
        open={false}
        stations={mockStations}
        uploadSongPending={false}
        uploadZipPending={false}
        onUploadSingle={vi.fn()}
        onUploadZip={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shows tabs for single and zip upload", () => {
    render(
      <UploadSongDialog
        open={true}
        stations={mockStations}
        uploadSongPending={false}
        uploadZipPending={false}
        onUploadSingle={vi.fn()}
        onUploadZip={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByText(/single/i)).toBeInTheDocument();
    expect(screen.getByText(/archive/i)).toBeInTheDocument();
  });

  it("shows station checkbox when stations exist", () => {
    render(
      <UploadSongDialog
        open={true}
        stations={mockStations}
        uploadSongPending={false}
        uploadZipPending={false}
        onUploadSingle={vi.fn()}
        onUploadZip={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByText("Station One")).toBeInTheDocument();
  });

  it("shows cancel and upload buttons", () => {
    render(
      <UploadSongDialog
        open={true}
        stations={mockStations}
        uploadSongPending={false}
        uploadZipPending={false}
        onUploadSingle={vi.fn()}
        onUploadZip={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /upload/i })).toBeInTheDocument();
  });
});
