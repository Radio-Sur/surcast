import { isGroupId, playlistIdFromGroupId, QUEUE_END } from "@/components/queue";
import { UPCOMING_KEY_PREFIX } from "@/lib/constants";
import type { QueueItem } from "@/types";

export const UPCOMING_KEY = UPCOMING_KEY_PREFIX;

export function getDropTargetIndex(overId: string, items: QueueItem[], pointerY: number | null): number {
  if (overId === QUEUE_END) return items.length;
  if (isGroupId(overId)) {
    const gId = playlistIdFromGroupId(overId);
    const groupSongs = items.filter((s) => s.origin_playlist_id === gId);
    if (groupSongs.length === 0) return -1;
    const first = items.indexOf(groupSongs[0]);
    const last = items.indexOf(groupSongs[groupSongs.length - 1]);
    const el = document.querySelector(`[data-group-id="${gId}"]`);
    let before = true;
    if (pointerY !== null && el) {
      const r = el.getBoundingClientRect();
      before = pointerY <= r.top + r.height / 2;
    }
    return before ? first : last + 1;
  }
  const targetSong = items.find((s) => s.id === overId);
  if (!targetSong) return -1;
  if (targetSong.origin_playlist_id) {
    const groupSongs = items.filter((s) => s.origin_playlist_id === targetSong.origin_playlist_id);
    if (groupSongs.length > 0) {
      const el = document.querySelector(`[data-group-id="${targetSong.origin_playlist_id}"]`);
      let before = true;
      if (pointerY !== null && el) {
        const r = el.getBoundingClientRect();
        before = pointerY <= r.top + r.height / 2;
      }
      if (before) return items.indexOf(groupSongs[0]);
      return items.indexOf(groupSongs[groupSongs.length - 1]) + 1;
    }
  }
  return items.indexOf(targetSong);
}
