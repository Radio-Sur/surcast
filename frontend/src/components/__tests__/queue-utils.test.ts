import { describe, expect, it } from "vitest";
import {
  durationBetween,
  fmt,
  fmtHms,
  groupId,
  groupItems,
  isGroupId,
  playlistIdFromGroupId,
} from "@/components/queue/queue-utils";

describe("fmt", () => {
  it("formats seconds as m:ss", () => {
    expect(fmt(0)).toBe("--:--");
    expect(fmt(65)).toBe("1:05");
    expect(fmt(3661)).toBe("1:01:01");
  });
});

describe("fmtHms", () => {
  it("formats seconds as h:mm:ss", () => {
    expect(fmtHms(0)).toBe("--:--");
    expect(fmtHms(3661)).toBe("1:01:01");
    expect(fmtHms(7322)).toBe("2:02:02");
  });
});

describe("durationBetween", () => {
  it("computes duration between two times", () => {
    expect(durationBetween("09:00", "10:00")).toBe("1:00:00");
    expect(durationBetween("09:00", "09:30")).toBe("0:30:00");
  });

  it("returns 0:00 when end <= start", () => {
    expect(durationBetween("10:00", "09:00")).toBe("0:00");
  });
});

describe("groupId", () => {
  it("wraps playlist id", () => {
    expect(groupId("abc")).toBe("playlist:abc");
  });
});

describe("isGroupId", () => {
  it("detects group IDs", () => {
    expect(isGroupId("playlist:abc")).toBe(true);
    expect(isGroupId("song-1")).toBe(false);
  });
});

describe("playlistIdFromGroupId", () => {
  it("extracts playlist id", () => {
    expect(playlistIdFromGroupId("playlist:abc")).toBe("abc");
  });
});

describe("groupItems", () => {
  it("groups consecutive songs from same playlist", () => {
    const items = [
      {
        id: "1",
        origin_playlist_id: "p1",
        playlist_name: "P1",
        duration: 100,
        song_id: "s1",
        title: "A",
        artist: "",
        album: "",
        has_cover: false,
        is_auto_dj: false,
      },
      {
        id: "2",
        origin_playlist_id: "p1",
        playlist_name: "P1",
        duration: 200,
        song_id: "s2",
        title: "B",
        artist: "",
        album: "",
        has_cover: false,
        is_auto_dj: false,
      },
      {
        id: "3",
        origin_playlist_id: null,
        playlist_name: null,
        duration: 150,
        song_id: "s3",
        title: "C",
        artist: "",
        album: "",
        has_cover: false,
        is_auto_dj: false,
      },
    ] as any;

    const result = groupItems(items);
    expect(result).toHaveLength(2);
    expect(result[0]).toHaveProperty("playlist_id", "p1");
    expect((result[0] as any).songs).toHaveLength(2);
    expect(result[1]).toHaveProperty("id", "3");
  });

  it("handles empty list", () => {
    expect(groupItems([])).toEqual([]);
  });
});
