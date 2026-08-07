import { describe, expect, it } from "vitest";
import { SongCover } from "@/components/song-cover";
import { render, screen } from "@/test/test-utils";

describe("SongCover", () => {
  it("renders placeholder when hasCover is false", () => {
    const { container } = render(<SongCover songId="1" hasCover={false} />);
    expect(container.querySelector("img")).not.toBeInTheDocument();
    expect(container.querySelector('[data-testid="MusicNoteIcon"]')).toBeInTheDocument();
  });

  it("renders img when hasCover is true", () => {
    const { container } = render(<SongCover songId="1" hasCover={true} />);
    const img = container.querySelector("img") as HTMLImageElement;
    expect(img).toBeInTheDocument();
    expect(img.src).toContain("/api/songs/1/cover");
  });

  it("shows auto-dj badge when autoDj is true", () => {
    render(<SongCover songId="1" hasCover={false} autoDj={true} />);
    expect(screen.getByText("DJ")).toBeInTheDocument();
  });

  it("does not show auto-dj badge when autoDj is false", () => {
    render(<SongCover songId="1" hasCover={false} />);
    expect(screen.queryByText("Auto DJ")).not.toBeInTheDocument();
  });
});
