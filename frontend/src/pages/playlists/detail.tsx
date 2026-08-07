import {
  closestCenter,
  DndContext,
  type DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import { arrayMove, SortableContext, useSortable, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import Add from "@mui/icons-material/Add";
import ArrowBack from "@mui/icons-material/ArrowBack";
import Close from "@mui/icons-material/Close";
import Delete from "@mui/icons-material/Delete";
import MusicNote from "@mui/icons-material/MusicNote";
import QueueMusic from "@mui/icons-material/QueueMusic";
import Search from "@mui/icons-material/Search";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import IconButton from "@mui/material/IconButton";
import InputAdornment from "@mui/material/InputAdornment";
import Paper from "@mui/material/Paper";
import Skeleton from "@mui/material/Skeleton";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TablePagination from "@mui/material/TablePagination";
import TableRow from "@mui/material/TableRow";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate, useParams } from "react-router-dom";
import { fmt, GripDots } from "@/components/queue";
import { SongCover } from "@/components/song-cover";
import {
  useAddPlaylistToQueue,
  usePlaylist,
  usePlaylistSongs,
  useRemovePlaylistSong,
  useRemovePlaylistSongsBatch,
  useReorderPlaylistSongs,
  useUpdatePlaylist,
} from "@/hooks/use-playlists";
import { useStations } from "@/hooks/use-stations";
import { useSnackbar } from "@/providers/snackbar-provider";
import type { PlaylistSong } from "@/types";
import { AddSongsToPlaylistDialog } from "./add-songs-dialog";
import { AddPlaylistToQueueDialog } from "./add-to-queue-dialog";
import { PlaylistEditDialog } from "./playlist-edit-dialog";

function SortablePlaylistSongRow({
  song,
  selected,
  onToggleSelect,
  onRemove,
  removeDisabled,
}: {
  song: PlaylistSong;
  selected: boolean;
  onToggleSelect: () => void;
  onRemove: () => void;
  removeDisabled: boolean;
}) {
  const { t } = useTranslation();
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: song.id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    zIndex: isDragging ? 10 : undefined,
    position: "relative" as const,
  };

  return (
    <TableRow ref={setNodeRef} style={style} hover>
      <TableCell>
        <Checkbox size="small" checked={selected} onChange={onToggleSelect} />
      </TableCell>
      <TableCell sx={{ color: "text.secondary" }}>{song.position + 1}</TableCell>
      <TableCell>
        <Box
          {...attributes}
          {...listeners}
          sx={{
            cursor: "grab",
            touchAction: "none",
            display: "flex",
            color: "text.secondary",
            "&:hover": { color: "text.primary" },
          }}
        >
          <GripDots />
        </Box>
      </TableCell>
      <TableCell>
        <SongCover songId={song.song_id} hasCover={song.has_cover} />
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
      <TableCell sx={{ pr: 4 }}>
        <IconButton size="small" color="error" disabled={removeDisabled} onClick={onRemove}>
          <Delete fontSize="small" />
        </IconButton>
      </TableCell>
    </TableRow>
  );
}

export function PlaylistDetailPage() {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const { id: unsafeId } = useParams<{ id: string }>();
  const id = unsafeId!;
  const navigate = useNavigate();

  const [search, setSearch] = useState("");
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [page, setPage] = useState(0);
  const [perPage, setPerPage] = useState(50);
  const [editOpen, setEditOpen] = useState(false);
  const [addOpen, setAddOpen] = useState(false);
  const [queueDialogOpen, setQueueDialogOpen] = useState(false);

  const {
    data: playlist,
    isLoading: loadingPlaylist,
    isError: playlistError,
    error: playlistLoadError,
  } = usePlaylist(id);

  useEffect(() => {
    if (playlist?.slug && id !== playlist.slug) {
      navigate(`/playlists/${playlist.slug}`, { replace: true });
    }
  }, [playlist?.slug, id, navigate]);
  const {
    data: songsData,
    isLoading: loadingSongs,
    isError: songsError,
    error: songsLoadError,
  } = usePlaylistSongs(id, page + 1, perPage);
  const songs = songsData?.songs;
  const songTotal = songsData?.total ?? 0;
  const { data: stations } = useStations();
  const removeSong = useRemovePlaylistSong(id);
  const updatePlaylist = useUpdatePlaylist(id);
  const addToQueue = useAddPlaylistToQueue();
  const removeSongsBatch = useRemovePlaylistSongsBatch(id);
  const reorderSongs = useReorderPlaylistSongs(id);

  const filteredSongs = useMemo(() => {
    if (!songs) return [];
    if (!search.trim()) return songs;
    const q = search.toLowerCase();
    return songs.filter(
      (s) =>
        s.title.toLowerCase().includes(q) || s.artist.toLowerCase().includes(q) || s.album.toLowerCase().includes(q),
    );
  }, [songs, search]);

  const handleSelectAll = () => {
    if (selectedIds.size === filteredSongs.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(filteredSongs.map((s) => s.song_id)));
    }
  };

  const handleSelectOne = (songId: string) => {
    const next = new Set(selectedIds);
    if (next.has(songId)) next.delete(songId);
    else next.add(songId);
    setSelectedIds(next);
  };

  const handleBatchRemove = async () => {
    try {
      await removeSongsBatch.mutateAsync(Array.from(selectedIds));
      setSelectedIds(new Set());
      showSnackbar(`Removed ${selectedIds.size} song(s) from playlist`, "success");
    } catch (err) {
      console.error("Failed to remove songs from playlist", err);
      showSnackbar("Failed to remove songs", "error");
    }
  };

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor),
  );

  const handleDragEnd = (event: DragEndEvent) => {
    if (!songs) return;
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    const oldIdx = songs.findIndex((s) => s.id === active.id);
    const newIdx = songs.findIndex((s) => s.id === over.id);
    if (oldIdx === -1 || newIdx === -1) return;
    const reordered = arrayMove(songs, oldIdx, newIdx);
    reorderSongs.mutate(reordered.map((s) => s.song_id));
  };

  if (!id) return <Typography>Playlist not found.</Typography>;

  const handleAddToQueue = async (stationId: string) => {
    try {
      await addToQueue.mutateAsync({ playlist_id: id, station_id: stationId });
      setQueueDialogOpen(false);
    } catch (err) {
      console.error("Failed to add playlist to queue", err);
      showSnackbar("Failed to add playlist to queue", "error");
    }
  };

  const isLoading = loadingPlaylist || loadingSongs;

  if (isLoading) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Skeleton variant="text" width={200} height={40} />
        <Skeleton variant="rounded" height={300} />
      </Box>
    );
  }

  if (playlistError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">
          {playlistLoadError instanceof Error ? playlistLoadError.message : "Failed to load playlist"}
        </Alert>
      </Box>
    );
  }

  if (songsError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">
          {songsLoadError instanceof Error ? songsLoadError.message : "Failed to load playlist songs"}
        </Alert>
      </Box>
    );
  }

  if (!playlist) {
    return <Typography>{t("playlists:not_found")}</Typography>;
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <Box sx={{ display: "flex", alignItems: "center", gap: 1.5 }}>
        <IconButton onClick={() => navigate("/playlists")}>
          <ArrowBack />
        </IconButton>
        <Box sx={{ flex: 1 }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            <Typography variant="h4" sx={{ fontWeight: 700 }}>
              {playlist.name}
            </Typography>
            <Button size="small" variant="outlined" onClick={() => setEditOpen(true)}>
              {t("common:edit")}
            </Button>
          </Box>
          {playlist.description && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
              {playlist.description}
            </Typography>
          )}
          <Typography variant="caption" color="text.secondary">
            {t("playlists:song_count", { count: playlist.song_count })}
          </Typography>
        </Box>
        <Box sx={{ display: "flex", gap: 1 }}>
          {selectedIds.size > 0 && (
            <>
              <Button
                variant="outlined"
                color="error"
                startIcon={<Delete />}
                onClick={handleBatchRemove}
                disabled={removeSongsBatch.isPending}
              >
                {t("playlists:delete_selected", { count: selectedIds.size })}
              </Button>
              <Button variant="text" size="small" startIcon={<Close />} onClick={() => setSelectedIds(new Set())}>
                {t("common:clear")}
              </Button>
            </>
          )}
          <Button variant="outlined" startIcon={<Add />} onClick={() => setAddOpen(true)}>
            {t("playlists:add_songs")}
          </Button>
          <Button
            variant="contained"
            startIcon={<QueueMusic />}
            onClick={() => setQueueDialogOpen(true)}
            disabled={playlist.song_count === 0}
          >
            {t("playlists:add_to_queue")}
          </Button>
        </Box>
      </Box>

      {!songs || songs.length === 0 ? (
        <Paper variant="outlined" sx={{ p: 6, textAlign: "center", borderRadius: 3 }}>
          <MusicNote sx={{ fontSize: 48, mb: 1, opacity: 0.3 }} />
          <Typography>{t("playlists:empty_detail")}</Typography>
          <Typography variant="body2" color="text.secondary">
            {t("playlists:empty_detail_hint")}
          </Typography>
        </Paper>
      ) : (
        <>
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
          <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 3 }}>
            <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
              <SortableContext items={songs?.map((s) => s.id) ?? []} strategy={verticalListSortingStrategy}>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 600, width: 50 }}>
                        <Checkbox
                          size="small"
                          indeterminate={selectedIds.size > 0 && selectedIds.size < filteredSongs.length}
                          checked={filteredSongs.length > 0 && selectedIds.size === filteredSongs.length}
                          onChange={handleSelectAll}
                        />
                      </TableCell>
                      <TableCell sx={{ fontWeight: 600, width: 50 }}>{t("playlists:table_hash")}</TableCell>
                      <TableCell sx={{ fontWeight: 600, width: 40 }} />
                      <TableCell sx={{ fontWeight: 600, width: 50 }} />
                      <TableCell sx={{ fontWeight: 600 }}>{t("playlists:table_title")}</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>{t("playlists:table_artist")}</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>{t("playlists:table_album")}</TableCell>
                      <TableCell sx={{ fontWeight: 600 }}>{t("playlists:table_duration")}</TableCell>
                      <TableCell sx={{ fontWeight: 600, width: 50, pr: 4 }} />
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {filteredSongs.length > 0 ? (
                      filteredSongs.map((s) => (
                        <SortablePlaylistSongRow
                          key={s.id}
                          song={s}
                          selected={selectedIds.has(s.song_id)}
                          onToggleSelect={() => handleSelectOne(s.song_id)}
                          onRemove={() =>
                            removeSong.mutate(s.song_id, {
                              onError: (err) => {
                                console.error("Failed to remove song from playlist", err);
                                showSnackbar("Failed to remove song from playlist", "error");
                              },
                            })
                          }
                          removeDisabled={removeSong.isPending}
                        />
                      ))
                    ) : (
                      <TableRow>
                        <TableCell colSpan={9} align="center" sx={{ py: 6, color: "text.secondary", pr: 4 }}>
                          {t("songs:empty_search")}
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </SortableContext>
            </DndContext>
            <TablePagination
              component="div"
              count={songTotal}
              page={page}
              onPageChange={(_, newPage) => setPage(newPage)}
              rowsPerPage={perPage}
              onRowsPerPageChange={(e) => {
                setPerPage(parseInt(e.target.value, 10));
                setPage(0);
              }}
              rowsPerPageOptions={[25, 50, 100]}
            />
          </TableContainer>
        </>
      )}

      <PlaylistEditDialog
        open={editOpen}
        initialName={playlist.name}
        initialDescription={playlist.description}
        isPending={updatePlaylist.isPending}
        onSave={(name, description) => {
          updatePlaylist.mutate(
            { name, description: description.trim() || undefined },
            {
              onError: (err) => {
                console.error("Failed to update playlist", err);
                showSnackbar("Failed to update playlist", "error");
              },
            },
          );
          setEditOpen(false);
        }}
        onClose={() => setEditOpen(false)}
      />

      <AddSongsToPlaylistDialog open={addOpen} playlistId={id} onClose={() => setAddOpen(false)} />

      <AddPlaylistToQueueDialog
        open={queueDialogOpen}
        stations={stations}
        isPending={addToQueue.isPending}
        onAddToQueue={handleAddToQueue}
        onClose={() => setQueueDialogOpen(false)}
      />
    </Box>
  );
}
