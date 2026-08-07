import { useEffect, useRef, useState } from "react";
import type { StreamStatus } from "@/types";

export function useElapsedTimer(streamStatus: StreamStatus | null | undefined) {
  const [elapsed, setElapsed] = useState(0);
  const previousSongIndex = useRef<number | null>(null);
  const durationRef = useRef(0);

  useEffect(() => {
    if (!streamStatus) return;
    durationRef.current = streamStatus.duration;
    if (streamStatus.song_index !== previousSongIndex.current) {
      setElapsed(streamStatus.elapsed);
      previousSongIndex.current = streamStatus.song_index;
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
