import { useEffect, useRef, useState } from "react";
import type { StreamStatus } from "@/types";

export function useElapsedTimer(streamStatus: StreamStatus | null | undefined) {
  const [elapsed, setElapsed] = useState(0);
  const previousSongIndex = useRef<number | null>(null);
  const previousPlaying = useRef<boolean | null>(null);
  const durationRef = useRef(0);

  useEffect(() => {
    if (!streamStatus) return;
    durationRef.current = streamStatus.duration;
    const songChanged = streamStatus.song_index !== previousSongIndex.current;
    // stopped/paused -> playing with the same track (e.g. restart resumes
    // the persisted cursor): the backend clock starts over, so resync.
    const restarted = previousPlaying.current === false && streamStatus.playing;
    previousSongIndex.current = streamStatus.song_index;
    previousPlaying.current = streamStatus.playing;
    if (songChanged || restarted) {
      setElapsed(streamStatus.elapsed);
    } else {
      // Same track, still playing, but the backend clock jumped backwards
      // (fresh replace after restart/rebuild): trust it instead of ticking
      // on from a stale pre-restart timestamp. Forward jumps are ignored —
      // a lagging duplicate status must not fast-forward the clock.
      setElapsed((current) => (streamStatus.elapsed + 3 < current ? streamStatus.elapsed : current));
    }
  }, [streamStatus]);

  useEffect(() => {
    if (!streamStatus?.playing) return;
    const id = setInterval(() => {
      setElapsed((e) => {
        const dur = durationRef.current;
        if (dur > 0 && e >= dur) return dur;
        return e + 1;
      });
    }, 1000);
    return () => clearInterval(id);
  }, [streamStatus?.playing]);

  return elapsed;
}
