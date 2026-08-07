import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import Box from "@mui/material/Box";
import Collapse from "@mui/material/Collapse";
import Typography from "@mui/material/Typography";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { groupItems } from "@/components/queue";
import { PlaylistGroupCard } from "@/components/queue/playlist-group-card";
import { SongRow } from "@/components/queue/song-row";
import { isPlaylistGroup, type QueueItem } from "@/types";

const PLAYED_KEY = "queue_played_open_";

export function PlayedQueue({
  stationId,
  played,
  onReAddToQueue,
}: {
  stationId: string;
  played: QueueItem[];
  onReAddToQueue: (songId: string) => void;
}) {
  const { t } = useTranslation();
  const [playedOpen, setPlayedOpen] = useState(() => localStorage.getItem(`${PLAYED_KEY}${stationId}`) !== "false");

  useEffect(() => {
    localStorage.setItem(`${PLAYED_KEY}${stationId}`, String(playedOpen));
  }, [playedOpen, stationId]);

  const playedGroups = groupItems(played);
  const playedStartPositions = playedGroups.map((_, i) => i + 1);

  if (played.length === 0) return null;

  return (
    <Box sx={{ mb: 2 }}>
      <Box
        onClick={() => setPlayedOpen(!playedOpen)}
        sx={{ display: "flex", alignItems: "center", gap: 0.5, cursor: "pointer", px: 1, mb: 0.5 }}
      >
        {playedOpen ? <ExpandLess fontSize="small" /> : <ExpandMore fontSize="small" />}
        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 600 }}>
          {t("stations:queue_played", {
            count: playedGroups.reduce((sum, g) => sum + (isPlaylistGroup(g) ? g.songs.length : 1), 0),
          })}
        </Typography>
      </Box>
      <Collapse in={playedOpen}>
        <Box sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>
          {playedGroups.map((g, gi) => {
            if (isPlaylistGroup(g)) {
              return (
                <PlaylistGroupCard
                  key={g.playlist_id}
                  group={g}
                  dimmed
                  playlistNumber={playedStartPositions[gi]}
                  onReAddSong={onReAddToQueue}
                />
              );
            }
            return (
              <SongRow
                key={g.id}
                song={g}
                index={playedStartPositions[gi] - 1}
                dimmed
                onReAdd={() => onReAddToQueue(g.song_id)}
              />
            );
          })}
        </Box>
      </Collapse>
    </Box>
  );
}
