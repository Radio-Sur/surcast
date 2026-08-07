import Add from "@mui/icons-material/Add";
import Close from "@mui/icons-material/Close";
import Delete from "@mui/icons-material/Delete";
import Edit from "@mui/icons-material/Edit";
import Search from "@mui/icons-material/Search";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import InputAdornment from "@mui/material/InputAdornment";
import Paper from "@mui/material/Paper";
import Skeleton from "@mui/material/Skeleton";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { EditSongDialog } from "@/components/edit-song-dialog";
import { fmt } from "@/components/queue";
import { SongCover } from "@/components/song-cover";
import { UploadSongDialog } from "@/components/upload-song-dialog";
import {
  useDeleteSong,
  useDeleteSongsBatch,
  useSongs,
  useUpdateSong,
  useUploadSong,
  useUploadZip,
} from "@/hooks/use-songs";
import { useStations } from "@/hooks/use-stations";
import { useSnackbar } from "@/providers/snackbar-provider";

function useSongSearch(search: string) {
  return useSongs((songs) =>
    search
      ? songs.filter(
          (s) =>
            s.title.toLowerCase().includes(search.toLowerCase()) ||
            s.artist.toLowerCase().includes(search.toLowerCase()) ||
            s.album.toLowerCase().includes(search.toLowerCase()),
        )
      : songs,
  );
}

export function SongsPage() {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const { data: stations } = useStations();
  const uploadSong = useUploadSong();
  const uploadZip = useUploadZip();
  const deleteSong = useDeleteSong();
  const deleteSongsBatch = useDeleteSongsBatch();

  const [search, setSearch] = useState("");
  const [uploadOpen, setUploadOpen] = useState(false);
  const [deleteId, setDeleteId] = useState<string | null>(null);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [editSong, setEditSong] = useState<{
    id: string;
    title: string;
    artist: string;
    album: string;
  } | null>(null);
  const updateSong = useUpdateSong();

  const [uploadResult, setUploadResult] = useState<{
    type: "single" | "zip";
    count: number;
  } | null>(null);

  const { data: filtered, isLoading, isError, error } = useSongSearch(search);

  const handleDelete = async () => {
    if (deleteId) {
      try {
        await deleteSong.mutateAsync(deleteId);
        setDeleteId(null);
      } catch (err) {
        console.error("Failed to delete song", err);
        showSnackbar("Failed to delete song", "error");
      }
    }
  };

  const handleSelectAll = () => {
    if (!filtered) return;
    if (selectedIds.size === filtered.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(filtered.map((s) => s.id)));
    }
  };

  const handleSelectOne = (id: string) => {
    const next = new Set(selectedIds);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    setSelectedIds(next);
  };

  const handleBatchDelete = async () => {
    try {
      await deleteSongsBatch.mutateAsync(Array.from(selectedIds));
      setSelectedIds(new Set());
      showSnackbar(`Deleted ${selectedIds.size} song(s)`, "success");
    } catch (err) {
      console.error("Failed to batch delete songs", err);
      showSnackbar("Failed to delete songs", "error");
    }
  };

  if (isError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">{error instanceof Error ? error.message : "Failed to load songs"}</Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          flexWrap: "wrap",
          gap: 2,
        }}
      >
        <Box>
          <Typography variant="h4">{t("songs:title")}</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            {t("songs:subtitle")}
          </Typography>
        </Box>
        <Box sx={{ display: "flex", gap: 1 }}>
          {selectedIds.size > 0 && (
            <>
              <Button
                variant="outlined"
                color="error"
                startIcon={<Delete />}
                onClick={handleBatchDelete}
                disabled={deleteSongsBatch.isPending}
              >
                {t("songs:delete_selected", { count: selectedIds.size })}
              </Button>
              <Button variant="text" size="small" startIcon={<Close />} onClick={() => setSelectedIds(new Set())}>
                {t("common:clear")}
              </Button>
            </>
          )}
          <Button variant="contained" startIcon={<Add />} onClick={() => setUploadOpen(true)}>
            {t("songs:upload")}
          </Button>
        </Box>
      </Box>

      {uploadResult && (
        <Alert severity="success" onClose={() => setUploadResult(null)}>
          {uploadResult.type === "zip"
            ? t("songs:uploaded_zip", { count: uploadResult.count })
            : t("songs:uploaded_single")}
        </Alert>
      )}

      <TextField
        placeholder={t("songs:search_placeholder")}
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        slotProps={{
          input: {
            startAdornment: (
              <InputAdornment position="start">
                <Search fontSize="small" />
              </InputAdornment>
            ),
          },
        }}
        sx={{ maxWidth: 400 }}
      />

      {isLoading ? (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <Skeleton variant="rounded" height={300} />
        </Box>
      ) : (
        <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 3 }}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell sx={{ fontWeight: 600, width: 50 }}>
                  <Checkbox
                    size="small"
                    indeterminate={selectedIds.size > 0 && selectedIds.size < (filtered?.length ?? 0)}
                    checked={filtered ? selectedIds.size === filtered.length : false}
                    onChange={handleSelectAll}
                  />
                </TableCell>
                <TableCell sx={{ fontWeight: 600, width: 50 }} />
                <TableCell sx={{ fontWeight: 600 }}>{t("songs:table_title")}</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>{t("songs:table_artist")}</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>{t("songs:table_album")}</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>{t("songs:table_duration")}</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>{t("songs:table_stations")}</TableCell>
                <TableCell sx={{ fontWeight: 600, width: 100, pr: 4 }} />
              </TableRow>
            </TableHead>
            <TableBody>
              {filtered && filtered.length > 0 ? (
                filtered.map((song) => (
                  <TableRow key={song.id} hover>
                    <TableCell>
                      <Checkbox
                        size="small"
                        checked={selectedIds.has(song.id)}
                        onChange={() => handleSelectOne(song.id)}
                      />
                    </TableCell>
                    <TableCell>
                      <SongCover songId={song.id} hasCover={song.has_cover} />
                    </TableCell>
                    <TableCell
                      sx={{
                        fontWeight: 500,
                        maxWidth: 200,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {song.title}
                    </TableCell>
                    <TableCell color="text.secondary">{song.artist || "\u2014"}</TableCell>
                    <TableCell color="text.secondary">{song.album || "\u2014"}</TableCell>
                    <TableCell>
                      <Typography variant="body2" color="text.secondary">
                        {song.duration > 0 ? fmt(song.duration) : t("common:duration_unknown")}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="text.secondary">
                        {song.station_ids.length}
                      </Typography>
                    </TableCell>
                    <TableCell sx={{ pr: 4 }}>
                      <Box sx={{ display: "flex", gap: 0.5 }}>
                        <IconButton
                          size="small"
                          onClick={() =>
                            setEditSong({
                              id: song.id,
                              title: song.title,
                              artist: song.artist,
                              album: song.album,
                            })
                          }
                        >
                          <Edit fontSize="small" />
                        </IconButton>
                        <IconButton size="small" onClick={() => setDeleteId(song.id)} color="error">
                          <Delete fontSize="small" />
                        </IconButton>
                      </Box>
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={8} align="center" sx={{ py: 6, color: "text.secondary", pr: 4 }}>
                    {search ? t("songs:empty_search") : t("songs:empty_library")}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      <UploadSongDialog
        open={uploadOpen}
        stations={stations}
        uploadSongPending={uploadSong.isPending}
        uploadZipPending={uploadZip.isPending}
        onUploadSingle={async (data) => {
          try {
            await uploadSong.mutateAsync({
              file: data.file,
              title: data.title,
              artist: data.artist,
              album: data.album,
              assign_to_all: data.assignToAll,
              station_ids: data.stationIds,
            });
            setUploadOpen(false);
            setUploadResult({ type: "single", count: 1 });
            setTimeout(() => setUploadResult(null), 3000);
          } catch (err) {
            console.error("Failed to upload song", err);
            showSnackbar("Failed to upload song", "error");
          }
        }}
        onUploadZip={async (data) => {
          try {
            const result = await uploadZip.mutateAsync({
              file: data.file,
              assign_to_all: data.assignToAll,
              station_ids: data.stationIds,
            });
            setUploadOpen(false);
            setUploadResult({ type: "zip", count: result.length });
            setTimeout(() => setUploadResult(null), 3000);
          } catch (err) {
            console.error("Failed to upload ZIP", err);
            showSnackbar("Failed to upload ZIP file", "error");
          }
        }}
        onClose={() => setUploadOpen(false)}
      />

      <Dialog open={!!deleteId} onClose={() => setDeleteId(null)} slotProps={{ paper: { sx: { borderRadius: 3 } } }}>
        <DialogTitle sx={{ px: 3, pt: 3, pb: 0 }}>{t("songs:delete_title")}</DialogTitle>
        <DialogContent sx={{ px: 3, pt: 3, pb: 2 }}>
          <DialogContentText>{t("songs:delete_message")}</DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 3 }}>
          <Button onClick={() => setDeleteId(null)}>{t("common:cancel")}</Button>
          <Button onClick={handleDelete} variant="contained" color="error" disabled={deleteSong.isPending}>
            {deleteSong.isPending ? t("common:deleting") : t("common:delete")}
          </Button>
        </DialogActions>
      </Dialog>

      <EditSongDialog
        song={editSong}
        isPending={updateSong.isPending}
        onSave={(data) =>
          updateSong.mutate(
            {
              id: data.id,
              data: { title: data.title, artist: data.artist, album: data.album },
            },
            {
              onError: (err) => {
                console.error("Failed to update song", err);
                showSnackbar("Failed to update song", "error");
              },
            },
          )
        }
        onClose={() => setEditSong(null)}
      />
    </Box>
  );
}
