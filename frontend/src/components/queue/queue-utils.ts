import type { PlaylistGroup, QueueItem } from "@/types";

const PLAYLIST_PREFIX = "playlist:";
const DURATION_UNKNOWN = "--:--";

export function fmtHms(sec: number) {
  if (sec <= 0) return DURATION_UNKNOWN;
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  return `${h}:${m.toString().padStart(2, "0")}:${s.toString().padStart(2, "0")}`;
}

export function fmt(sec: number) {
  if (sec <= 0) return DURATION_UNKNOWN;
  if (sec >= 3600) return fmtHms(sec);
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}:${s.toString().padStart(2, "0")}`;
}

export function durationBetween(start: string, end: string) {
  const [sh, sm] = start.split(":").map(Number);
  const [eh, em] = end.split(":").map(Number);
  const totalMin = eh * 60 + em - (sh * 60 + sm);
  if (totalMin <= 0) return "0:00";
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  return `${h}:${m.toString().padStart(2, "0")}:00`;
}

export function computeGroupEndsAt(
  group: PlaylistGroup,
  nowPlaying: QueueItem | null,
  upcoming: QueueItem[],
  elapsed: number,
): string | null {
  if (!nowPlaying) return null;

  const remainingCurrent = Math.max(0, nowPlaying.duration - elapsed);

  const firstSongIdx = upcoming.findIndex((s) => s.origin_playlist_id === group.playlist_id);
  const precedingUpcomingDuration =
    firstSongIdx >= 0 ? upcoming.slice(0, firstSongIdx).reduce((sum, s) => sum + s.duration, 0) : 0;

  const isNowPlayingGroup = group.playlist_id === nowPlaying.origin_playlist_id && group.current_song_index != null;
  const startIdx = isNowPlayingGroup ? group.current_song_index! + 1 : 0;
  const groupRemainingDuration = group.songs.slice(startIdx).reduce((sum, s) => sum + s.duration, 0);

  const totalDuration = remainingCurrent + precedingUpcomingDuration + groupRemainingDuration;

  const now = new Date();
  now.setSeconds(now.getSeconds() + totalDuration);
  const h = now.getHours().toString().padStart(2, "0");
  const m = now.getMinutes().toString().padStart(2, "0");
  return `${h}:${m}`;
}

export function groupItems(items: QueueItem[]): (QueueItem | PlaylistGroup)[] {
  const groups: (QueueItem | PlaylistGroup)[] = [];
  let i = 0;
  while (i < items.length) {
    const item = items[i];
    if (item.origin_playlist_id) {
      const groupSongs: QueueItem[] = [item];
      while (i + 1 < items.length && items[i + 1].origin_playlist_id === item.origin_playlist_id) {
        groupSongs.push(items[i + 1]);
        i++;
      }
      groups.push({
        kind: "playlist_group",
        playlist_id: item.origin_playlist_id,
        playlist_name: item.playlist_name || "Unknown playlist",
        songs: groupSongs,
        total_duration: groupSongs.reduce((sum, s) => sum + s.duration, 0),
        current_song_index: 0,
      });
    } else {
      groups.push(item);
    }
    i++;
  }
  return groups;
}

export function groupId(p: string) {
  return `${PLAYLIST_PREFIX}${p}`;
}

export function isGroupId(id: string) {
  return id.startsWith(PLAYLIST_PREFIX);
}

export function playlistIdFromGroupId(id: string) {
  return id.slice(PLAYLIST_PREFIX.length);
}
