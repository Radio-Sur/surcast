import { fireEvent } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { UploadSongDialog } from "@/components/upload-song-dialog";
import { render, screen } from "@/test/test-utils";
import type { Station } from "@/types";

vi.mock("@/lib/services/uploads", () => ({
  uploadsService: {
    createJob: vi.fn().mockResolvedValue({ job_id: "job-1" }),
    job: vi.fn().mockResolvedValue({
      id: "job-1",
      status: "processing",
      total: 2,
      processed: 1,
      failed: 0,
      current_file: null,
      error: null,
      song_ids: [],
    }),
  },
}));

import { uploadsService } from "@/lib/services/uploads";

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

const audio = (name: string, type = "audio/mpeg") => new File(["x"], name, { type, lastModified: 0 });
const zip = (name: string) => new File(["pk"], name, { type: "application/zip", lastModified: 0 });

function renderDialog() {
  return render(<UploadSongDialog open={true} stations={mockStations} onFinished={vi.fn()} onClose={vi.fn()} />);
}

function getFileInput() {
  return document.querySelector('input[type="file"]') as HTMLInputElement;
}

function selectFiles(files: File[]) {
  const input = getFileInput();
  Object.defineProperty(input, "files", { value: files, configurable: true });
  fireEvent.change(input);
}

function dropFiles(files: File[]) {
  fireEvent.drop(screen.getByTestId("dropzone"), { dataTransfer: { files } });
}

function dropZip(file: File) {
  fireEvent.drop(screen.getByTestId("zip-dropzone"), { dataTransfer: { files: [file] } });
}

function removeRow(index: number) {
  fireEvent.click(screen.getAllByLabelText("Remove file")[index]);
}

function getLastFormData() {
  return vi.mocked(uploadsService.createJob).mock.calls.at(-1)?.[0] as FormData;
}

describe("UploadSongDialog", () => {
  beforeEach(() => {
    vi.mocked(uploadsService.createJob).mockClear();
    vi.mocked(uploadsService.job).mockClear();
  });

  it("renders when open", () => {
    renderDialog();
    expect(screen.getByRole("heading", { name: /upload/i })).toBeInTheDocument();
  });

  it("does not render when closed", () => {
    render(<UploadSongDialog open={false} stations={mockStations} onFinished={vi.fn()} onClose={vi.fn()} />);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("makes the multi-file capability visible right away", () => {
    renderDialog();
    expect(screen.getByTestId("dropzone")).toBeInTheDocument();
    expect(screen.getByText(/each audio file becomes its own track/i)).toBeInTheDocument();
  });

  it("shows station checkbox when stations exist", () => {
    renderDialog();
    expect(screen.getByText("Station One")).toBeInTheDocument();
  });

  it("shows cancel and a disabled upload button when nothing is selected", () => {
    renderDialog();
    expect(screen.getByRole("button", { name: /cancel/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^upload$/i })).toBeDisabled();
  });

  it("uploads multiple single tracks in one batch without metadata overrides", () => {
    renderDialog();
    selectFiles([audio("a.mp3"), audio("b.flac", "")]);

    fireEvent.click(screen.getByRole("button", { name: "Add 2 tracks" }));

    expect(uploadsService.createJob).toHaveBeenCalledTimes(1);
    const fd = getLastFormData();
    expect(fd.getAll("file")).toHaveLength(2);
    expect(fd.get("title")).toBeNull();
    expect(fd.get("artist")).toBeNull();
    expect(fd.get("album")).toBeNull();
  });

  it("applies metadata overrides only for a single file", () => {
    renderDialog();
    selectFiles([audio("a.mp3")]);

    fireEvent.change(screen.getByLabelText("Title"), { target: { value: "Some Title" } });
    fireEvent.change(screen.getByLabelText("Artist"), { target: { value: "Some Artist" } });

    fireEvent.click(screen.getByRole("button", { name: "Add track" }));

    const fd = getLastFormData();
    expect(fd.getAll("file")).toHaveLength(1);
    expect(fd.get("title")).toBe("Some Title");
    expect(fd.get("artist")).toBe("Some Artist");
    expect(fd.get("album")).toBeNull();
  });

  it("accepts a mixed drop of audio files and a zip archive", () => {
    renderDialog();
    dropFiles([audio("a.mp3"), audio("b.flac", ""), zip("batch.zip")]);

    expect(screen.getByText("You will add 3 tracks")).toBeInTheDocument();
    expect(screen.getByText("ZIP archive — batch.zip")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Add 3 tracks" }));

    const fd = getLastFormData();
    expect(fd.getAll("file")).toHaveLength(3);
    expect(fd.get("title")).toBeNull();
  });

  it("keeps the selected tracks visible in the session and invites adding more", () => {
    renderDialog();
    dropFiles([audio("a.mp3"), audio("b.wav")]);

    expect(screen.getByText("You will add 2 tracks")).toBeInTheDocument();
    expect(screen.getByText("a.mp3")).toBeInTheDocument();
    expect(screen.getByText("b.wav")).toBeInTheDocument();
    expect(screen.getByText("Add more files...")).toBeInTheDocument();

    dropFiles([audio("c.flac", "")]);
    expect(screen.getByText("You will add 3 tracks")).toBeInTheDocument();
    expect(screen.getByText("c.flac")).toBeInTheDocument();
  });

  it("removing rows updates the count and resets to the empty state", () => {
    renderDialog();
    dropFiles([audio("a.mp3"), audio("b.wav")]);

    removeRow(0);
    expect(screen.getByText("You will add 1 track")).toBeInTheDocument();

    removeRow(0);
    expect(screen.getByText(/drop audio files here/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^upload$/i })).toBeDisabled();
  });

  it("uploading a whole zip via its own section is visible and works", () => {
    renderDialog();
    expect(screen.getByText("Track archive (ZIP)")).toBeInTheDocument();

    dropZip(zip("albums.zip"));

    expect(screen.getByText("ZIP archive — albums.zip")).toBeInTheDocument();
    expect(screen.getByText("You will add 1 track")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Add track" }));

    const fd = getLastFormData();
    expect(fd.getAll("file")).toHaveLength(1);
    expect((fd.getAll("file")[0] as File).name).toBe("albums.zip");
    expect(fd.get("title")).toBeNull();
  });

  it("combines audio files and a zip from both sections", () => {
    renderDialog();

    dropFiles([audio("a.mp3"), audio("b.wav")]);
    dropZip(zip("albums.zip"));

    expect(screen.getByText("You will add 3 tracks")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Add 3 tracks" }));

    const fd = getLastFormData();
    expect(fd.getAll("file")).toHaveLength(3);
  });

  it("removing the zip row switches back to single-file metadata", () => {
    renderDialog();
    dropFiles([audio("a.mp3"), zip("batch.zip")]);

    expect(screen.getByText("You will add 2 tracks")).toBeInTheDocument();
    removeRow(1);

    expect(screen.getByText("You will add 1 track")).toBeInTheDocument();
    expect(screen.getByLabelText("Title")).toBeInTheDocument();
  });
});
