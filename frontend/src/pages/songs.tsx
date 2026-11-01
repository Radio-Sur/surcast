import Add from "@mui/icons-material/Add";
import Close from "@mui/icons-material/Close";
import Delete from "@mui/icons-material/Delete";
import Edit from "@mui/icons-material/Edit";
import FilterAlt from "@mui/icons-material/FilterAlt";
import InfoOutlined from "@mui/icons-material/InfoOutlined";
import Search from "@mui/icons-material/Search";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import Chip from "@mui/material/Chip";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import InputAdornment from "@mui/material/InputAdornment";
import MenuItem from "@mui/material/MenuItem";
import Paper from "@mui/material/Paper";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TableSortLabel from "@mui/material/TableSortLabel";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { EditSongDialog } from "@/components/edit-song-dialog";
import { ErrorDetails } from "@/components/error-details";
import { fmt } from "@/components/queue";
import { SongCover } from "@/components/song-cover";
import { SongDetailsDialog } from "@/components/song-details-dialog";
import { UploadSongDialog } from "@/components/upload-song-dialog";
import { useFolders } from "@/hooks/use-files";
import {
  useDeleteSong,
  useDeleteSongsBatch,
  usePurgeMissingSongs,
  useScanLibrary,
  useSongs,
  useUpdateSong,
} from "@/hooks/use-songs";
import { useStations } from "@/hooks/use-stations";
import { isHttpError, serverMessage } from "@/lib/is-http-error";
import { reportHttpError } from "@/lib/report-error";
import { useSnackbar } from "@/providers/snackbar-provider";
import type { Song } from "@/types";

type SongSortKey = "title" | "artist" | "album" | "duration";

function matchesFolder(song: Song, folder: string) {
  const dir = song.file_path.includes("/") ? song.file_path.slice(0, song.file_path.lastIndexOf("/")) : "";
  return dir === folder;
}

export function SongsPage() {
  const { t } = useTranslation();
  const { showSnackbar, showError } = useSnackbar();
  const { data: stations } = useStations();
  const queryClient = useQueryClient();
  const deleteSong = useDeleteSong();
  const deleteSongsBatch = useDeleteSongsBatch();
  const purgeMissing = usePurgeMissingSongs();
  const { data: folders } = useFolders();

  const [search, setSearch] = useState("");
  const [uploadOpen, setUploadOpen] = useState(false);
  const [deleteId, setDeleteId] = useState<string | null>(null);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [detailsSongId, setDetailsSongId] = useState<string | null>(null);
  const [purgeOpen, setPurgeOpen] = useState(false);
  const [folderFilter, setFolderFilter] = useState("");
  const [missingOnly, setMissingOnly] = useState(false);
  const [sortKey, setSortKey] = useState<SongSortKey>("title");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("asc");
  const [editSong, setEditSong] = useState<{
    id: string;
    title: string;
    artist: string;
    album: string;
  } | null>(null);
  const updateSong = useUpdateSong();

  const [uploadResult, setUploadResult] = useState<{
    created: number;
    failed: number;
    skipped: number;
  } | null>(null);

  const { data: songs, isLoading, isError, error } = useSongs();

  const filtered = useMemo(() => {
    const term = search.toLowerCase();
    const result = (songs ?? []).filter((song) => {
      if (
        term &&
        !song.title.toLowerCase().includes(term) &&
        !song.artist.toLowerCase().includes(term) &&
        !song.album.toLowerCase().includes(term)
      ) {
        return false;
      }
      if (missingOnly && !song.missing_since) return false;
      if (folderFilter && !matchesFolder(song, folderFilter)) return false;
      return true;
    });
    const direction = sortOrder === "asc" ? 1 : -1;
    return result.sort((a, b) => {
      let cmp: number;
      if (sortKey === "duration") cmp = a.duration - b.duration;
      else cmp = a[sortKey].localeCompare(b[sortKey]);
      return cmp * direction;
    });
  }, [songs, search, missingOnly, folderFilter, sortKey, sortOrder]);

  const missingCount = useMemo(() => (songs ?? []).filter((song) => song.missing_since).length, [songs]);

  // Lazy library scan: files dropped into the music folder (FTP/manual
  // copies) appear on the next library open. Runs once per mount; the
  // backend additionally throttles repeat scans.
  const { mutate: scanOnMount } = useScanLibrary();
  const scannedRef = useRef(false);
  useEffect(() => {
    if (scannedRef.current) return;
    scannedRef.current = true;
    scanOnMount(undefined, {
      onSuccess: (report) => {
        if (!report.throttled && report.added > 0) {
          showSnackbar(t("songs:scanned_added", { count: report.added }), "success");
        }
      },
      onError: (err) => {
        reportHttpError(showError, err, "Failed to scan music library");
      },
    });
  }, [scanOnMount, showSnackbar, showError, t]);

  const handleDelete = async () => {
    if (deleteId) {
      try {
        await deleteSong.mutateAsync(deleteId);
        setDeleteId(null);
      } catch (err) {
        reportHttpError(showError, err, "Failed to delete song");
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
      reportHttpError(showError, err, "Failed to delete songs");
    }
  };

  const handlePurgeMissing = async () => {
    try {
      await purgeMissing.mutateAsync();
      setPurgeOpen(false);
      showSnackbar(t("songs:delete_missing_done", { count: missingCount }), "success");
    } catch (err) {
      reportHttpError(showError, err, "Failed to delete missing songs");
    }
  };

  const handleSort = (key: SongSortKey) => {
    if (sortKey === key) setSortOrder((prev) => (prev === "asc" ? "desc" : "asc"));
    else {
      setSortKey(key);
      setSortOrder("asc");
    }
  };

  if (isError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">
          {(() => {
            const info = isHttpError(error);
            const server = serverMessage(error);
            return (
              <ErrorDetails
                title={server ?? "Failed to load songs"}
                details={{ ...info, message: server ?? "Failed to load songs" }}
              />
            );
          })()}
        </Alert>
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
        <Box sx={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: 1 }}>
          <Chip
            icon={<FilterAlt />}
            label={t("songs:filter_missing")}
            onClick={() => setMissingOnly((value) => !value)}
            color={missingOnly ? "primary" : "default"}
            variant={missingOnly ? "filled" : "outlined"}
          />
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
          {missingCount > 0 && (
            <Button variant="outlined" color="error" startIcon={<Delete />} onClick={() => setPurgeOpen(true)}>
              {t("songs:delete_all_missing")}
            </Button>
          )}
          <Button variant="contained" startIcon={<Add />} onClick={() => setUploadOpen(true)}>
            {t("songs:upload")}
          </Button>
        </Box>
      </Box>

      {uploadResult && (
        <Alert severity="success" onClose={() => setUploadResult(null)}>
          {uploadResult.failed > 0
            ? t("songs:uploaded_result_failed", { created: uploadResult.created, failed: uploadResult.failed })
            : uploadResult.skipped > 0
              ? t("songs:uploaded_result_skipped", { created: uploadResult.created, skipped: uploadResult.skipped })
              : t("songs:uploaded_result", { created: uploadResult.created })}
        </Alert>
      )}

      <Stack direction="row" spacing={1.5} useFlexGap sx={{ flexWrap: "wrap", alignItems: "center" }}>
        <TextField
          size="small"
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
          sx={{ maxWidth: 400, minWidth: 240 }}
        />
        <TextField
          select
          size="small"
          label={t("songs:filter_folder")}
          value={folderFilter}
          onChange={(e) => setFolderFilter(e.target.value)}
          sx={{ minWidth: 180 }}
        >
          <MenuItem value="">{t("songs:filter_all_folders")}</MenuItem>
          {(folders ?? []).map((folder) => (
            <MenuItem key={folder} value={folder}>
              {folder}
            </MenuItem>
          ))}
        </TextField>
      </Stack>

      {isLoading ? (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <Skeleton variant="rounded" height={300} />
        </Box>
      ) : (
        <TableContainer
          component={Paper}
          variant="outlined"
          sx={{
            borderRadius: 3,
            "& .MuiTableCell-root:first-of-type": { pl: 3 },
            "& .MuiTableCell-root:last-of-type": { pr: 3 },
          }}
        >
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
                <TableCell sortDirection={sortKey === "title" ? sortOrder : false}>
                  <TableSortLabel
                    active={sortKey === "title"}
                    direction={sortOrder}
                    onClick={() => handleSort("title")}
                  >
                    {t("songs:table_title")}
                  </TableSortLabel>
                </TableCell>
                <TableCell sortDirection={sortKey === "artist" ? sortOrder : false}>
                  <TableSortLabel
                    active={sortKey === "artist"}
                    direction={sortOrder}
                    onClick={() => handleSort("artist")}
                  >
                    {t("songs:table_artist")}
                  </TableSortLabel>
                </TableCell>
                <TableCell sortDirection={sortKey === "album" ? sortOrder : false}>
                  <TableSortLabel
                    active={sortKey === "album"}
                    direction={sortOrder}
                    onClick={() => handleSort("album")}
                  >
                    {t("songs:table_album")}
                  </TableSortLabel>
                </TableCell>
                <TableCell sortDirection={sortKey === "duration" ? sortOrder : false}>
                  <TableSortLabel
                    active={sortKey === "duration"}
                    direction={sortOrder}
                    onClick={() => handleSort("duration")}
                  >
                    {t("songs:table_duration")}
                  </TableSortLabel>
                </TableCell>
                <TableCell sx={{ fontWeight: 600 }}>{t("songs:table_stations")}</TableCell>
                <TableCell sx={{ fontWeight: 600, width: 120 }} />
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
                      <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                        <Box
                          component="span"
                          sx={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                        >
                          {song.title}
                        </Box>
                        {song.missing_since && (
                          <Chip label={t("songs:missing_file")} color="error" size="small" variant="outlined" />
                        )}
                      </Box>
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
                    <TableCell>
                      <Box sx={{ display: "flex", gap: 0.5 }}>
                        <IconButton size="small" onClick={() => setDetailsSongId(song.id)} title={t("songs:details")}>
                          <InfoOutlined fontSize="small" />
                        </IconButton>
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
                  <TableCell colSpan={8} align="center" sx={{ py: 6, color: "text.secondary" }}>
                    {missingOnly
                      ? t("songs:empty_missing")
                      : search
                        ? t("songs:empty_search")
                        : t("songs:empty_library")}
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
        onFinished={({ created, failed, skipped }) => {
          queryClient.invalidateQueries({ queryKey: ["songs"] });
          if (created > 0) {
            setUploadResult({ created, failed, skipped });
            setTimeout(() => setUploadResult(null), 4000);
          } else {
            showSnackbar(t("songs:upload_failed"), "error");
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
                reportHttpError(showError, err, "Failed to update song");
              },
            },
          )
        }
        onClose={() => setEditSong(null)}
      />

      <Dialog open={purgeOpen} onClose={() => setPurgeOpen(false)} slotProps={{ paper: { sx: { borderRadius: 3 } } }}>
        <DialogTitle sx={{ px: 3, pt: 3, pb: 0 }}>{t("songs:delete_missing_title")}</DialogTitle>
        <DialogContent sx={{ px: 3, pt: 3, pb: 2 }}>
          <DialogContentText>{t("songs:delete_missing_message", { count: missingCount })}</DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 3 }}>
          <Button onClick={() => setPurgeOpen(false)}>{t("common:cancel")}</Button>
          <Button onClick={handlePurgeMissing} variant="contained" color="error" disabled={purgeMissing.isPending}>
            {purgeMissing.isPending ? t("common:deleting") : t("common:delete")}
          </Button>
        </DialogActions>
      </Dialog>

      <SongDetailsDialog songId={detailsSongId} onClose={() => setDetailsSongId(null)} />
    </Box>
  );
}
