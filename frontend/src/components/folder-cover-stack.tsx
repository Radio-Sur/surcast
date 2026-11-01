import Folder from "@mui/icons-material/Folder";
import Box from "@mui/material/Box";
import { SongCover } from "@/components/song-cover";

const MAX_COVERS = 4;
const COVER_SIZE = 22;

const LAYOUTS: Record<number, Array<{ left: number; top: number; rotate: number }>> = {
  1: [{ left: 13, top: 2, rotate: -4 }],
  2: [
    { left: 6, top: 3, rotate: -9 },
    { left: 20, top: 1, rotate: 8 },
  ],
  3: [
    { left: 2, top: 4, rotate: -11 },
    { left: 13, top: 1, rotate: 0 },
    { left: 24, top: 4, rotate: 11 },
  ],
  4: [
    { left: 0, top: 5, rotate: -13 },
    { left: 8, top: 1, rotate: -4 },
    { left: 18, top: 2, rotate: 5 },
    { left: 26, top: 5, rotate: 13 },
  ],
};

export function FolderCoverStack({ covers }: { covers: string[] }) {
  const items = covers.slice(0, MAX_COVERS);
  if (items.length === 0) return <Folder color="primary" />;

  const layout = LAYOUTS[items.length];

  return (
    <Box aria-hidden sx={{ position: "relative", width: 48, height: 46, flexShrink: 0 }}>
      {items.map((songId, index) => (
        <Box
          key={songId}
          sx={{
            position: "absolute",
            left: layout[index].left,
            top: layout[index].top,
            zIndex: 2 + index,
            transform: `rotate(${layout[index].rotate}deg)`,
            filter: "drop-shadow(0 1px 3px rgba(0,0,0,0.35))",
          }}
        >
          <SongCover songId={songId} hasCover size={COVER_SIZE} />
        </Box>
      ))}
      <Folder
        sx={{
          position: "absolute",
          bottom: 0,
          left: 4,
          zIndex: 1,
          fontSize: 42,
          color: "warning.main",
        }}
      />
    </Box>
  );
}
