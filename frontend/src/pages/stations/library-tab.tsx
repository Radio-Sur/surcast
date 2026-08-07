import Add from "@mui/icons-material/Add";
import Delete from "@mui/icons-material/Delete";
import LibraryMusic from "@mui/icons-material/LibraryMusic";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import IconButton from "@mui/material/IconButton";
import Paper from "@mui/material/Paper";
import Skeleton from "@mui/material/Skeleton";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { fmt } from "@/components/queue";
import { SongCover } from "@/components/song-cover";
import type { StationSong } from "@/types";
import { AddSongsToStationDialog } from "./add-songs-dialog";

export function LibraryTab({
  librarySongs,
  librarySongTotal,
  libraryPage,
  libraryPerPage,
  onLibraryPageChange,
  onLibraryPerPageChange,
  libraryLoading,
  onRemove,
  stationId,
}: {
  librarySongs: StationSong[] | undefined;
  librarySongTotal: number;
  libraryPage: number;
  libraryPerPage: number;
  onLibraryPageChange: (page: number) => void;
  onLibraryPerPageChange: (perPage: number) => void;
  libraryLoading: boolean;
  onRemove: (songId: string) => void;
  stationId: string;
}) {
  const { t } = useTranslation();
  const [dialogOpen, setDialogOpen] = useState(false);

  return (
    <Box>
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          mb: 2,
          px: 4,
        }}
      >
        <Typography variant="h6">{t("stations:library_title")}</Typography>
        <Button size="small" startIcon={<Add />} variant="contained" onClick={() => setDialogOpen(true)}>
          {t("stations:library_add")}
        </Button>
      </Box>

      {libraryLoading ? (
        <Skeleton variant="rounded" height={200} />
      ) : librarySongs && librarySongs.length > 0 ? (
        <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 3 }}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, pl: 6 }} />
                <TableCell sx={{ fontWeight: 600 }}>{t("stations:table_title")}</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>{t("stations:table_artist")}</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>{t("stations:table_album")}</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>{t("stations:table_duration")}</TableCell>
                <TableCell sx={{ fontWeight: 600, width: 60, pr: 6 }} />
              </TableRow>
            </TableHead>
            <TableBody>
              {librarySongs.map((song) => (
                <TableRow key={song.song_id} hover>
                  <TableCell sx={{ pl: 6 }}>
                    <SongCover songId={song.song_id} hasCover={song.has_cover} />
                  </TableCell>
                  <TableCell sx={{ fontWeight: 500 }}>{song.title}</TableCell>
                  <TableCell color="text.secondary">{song.artist || "\u2014"}</TableCell>
                  <TableCell color="text.secondary">{song.album || "\u2014"}</TableCell>
                  <TableCell>
                    <Typography variant="body2" color="text.secondary">
                      {song.duration > 0 ? fmt(song.duration) : t("common:duration_unknown")}
                    </Typography>
                  </TableCell>
                  <TableCell sx={{ pr: 6 }}>
                    <IconButton size="small" onClick={() => onRemove(song.song_id)} color="error">
                      <Delete fontSize="small" />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <TablePagination
            component="div"
            count={librarySongTotal}
            page={libraryPage}
            onPageChange={(_, newPage) => onLibraryPageChange(newPage)}
            rowsPerPage={libraryPerPage}
            onRowsPerPageChange={(e) => {
              onLibraryPerPageChange(parseInt(e.target.value, 10));
              onLibraryPageChange(0);
            }}
            rowsPerPageOptions={[25, 50, 100]}
          />
        </TableContainer>
      ) : (
        <Box sx={{ py: 6, textAlign: "center", color: "text.secondary" }}>
          <LibraryMusic sx={{ fontSize: 48, mb: 1, opacity: 0.3 }} />
          <Typography>{t("stations:library_empty")}</Typography>
          <Typography variant="body2">{t("stations:library_empty_hint")}</Typography>
        </Box>
      )}

      <AddSongsToStationDialog open={dialogOpen} stationId={stationId} onClose={() => setDialogOpen(false)} />
    </Box>
  );
}
