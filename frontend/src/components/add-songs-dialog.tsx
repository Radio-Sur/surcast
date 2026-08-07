import Close from "@mui/icons-material/Close";
import LibraryMusic from "@mui/icons-material/LibraryMusic";
import Person from "@mui/icons-material/Person";
import Search from "@mui/icons-material/Search";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import Chip from "@mui/material/Chip";
import CircularProgress from "@mui/material/CircularProgress";
import Collapse from "@mui/material/Collapse";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import InputAdornment from "@mui/material/InputAdornment";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Pagination from "@mui/material/Pagination";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { fmt } from "@/components/queue";
import { SongCover } from "@/components/song-cover";
import { useArtists, useSongCount, useSongSearch } from "@/hooks/use-songs";
import type { AlbumSelector, Song } from "@/types";

const PER_PAGE = 15;

function groupSongsByAlbum(songs: Song[]): Record<string, Song[]> {
  const groups: Record<string, Song[]> = {};
  for (const song of songs) {
    const album = song.album || "";
    if (!groups[album]) groups[album] = [];
    groups[album].push(song);
  }
  return groups;
}

function filterExisting(songs: Song[], existingSongIds: Set<string>): Song[] {
  if (existingSongIds.size === 0) return songs;
  return songs.filter((s) => !existingSongIds.has(s.id));
}

export interface AddSongsDialogProps {
  open: boolean;
  onClose: () => void;
  onAdd: (selections: { songIds: string[]; artistNames: string[]; albumSelectors: AlbumSelector[] }) => Promise<void>;
  isPending: boolean;
  existingSongIds?: Set<string>;
  existingArtistCounts?: Record<string, number>;
  title: string;
  searchPlaceholder: string;
  addLabel: (count: number) => string;
  emptyLabel: string;
}

export function AddSongsDialog({
  open,
  onClose,
  onAdd,
  isPending,
  existingSongIds,
  existingArtistCounts,
  title,
  searchPlaceholder,
  addLabel,
  emptyLabel,
}: AddSongsDialogProps) {
  const { t } = useTranslation();

  const [search, setSearch] = useState("");
  const [artistPage, setArtistPage] = useState(1);
  const [searchPage, setSearchPage] = useState(1);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [selectedArtists, setSelectedArtists] = useState<Set<string>>(new Set());
  const [selectedAlbums, setSelectedAlbums] = useState<Set<AlbumSelector>>(new Set());
  const [expandedArtists, setExpandedArtists] = useState<Set<string>>(new Set());
  const [expandedAlbums, setExpandedAlbums] = useState<Set<string>>(new Set());
  const [showSelected, setShowSelected] = useState(false);

  const [activeArtist, setActiveArtist] = useState<string | null>(null);
  const [artistCache, setArtistCache] = useState<Map<string, Song[]>>(new Map());
  const cacheRef = useRef(artistCache);
  cacheRef.current = artistCache;

  const query = search.trim();

  const artistsQuery = useArtists(
    query ? { q: query, page: artistPage, per_page: PER_PAGE } : { page: artistPage, per_page: PER_PAGE },
    { enabled: open },
  );

  const searchQuery = useSongSearch(
    query ? { q: query, page: searchPage, per_page: PER_PAGE } : { q: "", page: 0, per_page: 0 },
    { enabled: !!query && open },
  );

  const artistSongsQuery = useSongSearch(
    activeArtist !== null
      ? query
        ? { q: query, artist: activeArtist, per_page: 200 }
        : { artist: activeArtist, per_page: 200 }
      : { q: "", page: 0, per_page: 0 },
    { enabled: activeArtist !== null && open },
  );

  const existingSet = existingSongIds ?? new Set<string>();

  const countQuery = useSongCount(
    {
      artistNames: Array.from(selectedArtists),
      albumSelectors: Array.from(selectedAlbums),
    },
    { enabled: open && (selectedArtists.size > 0 || selectedAlbums.size > 0) },
    existingSet,
  );

  const totalSongCount = selectedIds.size + (countQuery.data?.count ?? 0);

  useEffect(() => {
    if (artistSongsQuery.data?.songs && activeArtist !== null && !artistSongsQuery.isPlaceholderData) {
      setArtistCache((prev) => {
        const next = new Map(prev);
        next.set(activeArtist, artistSongsQuery.data.songs);
        return next;
      });
    }
  }, [artistSongsQuery.data, activeArtist, artistSongsQuery.isPlaceholderData]);

  const getArtistSongs = useCallback(
    (artistName: string): Song[] | undefined => {
      if (activeArtist === artistName && artistSongsQuery.data?.songs && !artistSongsQuery.isPlaceholderData) {
        return artistSongsQuery.data.songs;
      }
      return cacheRef.current.get(artistName);
    },
    [activeArtist, artistSongsQuery.data, artistSongsQuery.isPlaceholderData],
  );

  const totalSelectedCount = selectedIds.size + selectedArtists.size + selectedAlbums.size;

  const selectedSongs = useMemo(() => {
    if (totalSelectedCount === 0) return [];
    const map = new Map<string, Song>();
    for (const song of searchQuery.data?.songs ?? []) {
      map.set(song.id, song);
    }
    for (const [, songs] of artistCache) {
      for (const song of songs) {
        map.set(song.id, song);
      }
    }
    return Array.from(selectedIds)
      .map((id) => map.get(id))
      .filter((s): s is Song => !!s);
  }, [totalSelectedCount, selectedIds, searchQuery.data, artistCache]);

  const toggleId = useCallback((id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const toggleArtist = useCallback((name: string) => {
    setSelectedArtists((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }, []);

  const toggleAlbumSelector = useCallback((sel: AlbumSelector) => {
    setSelectedAlbums((prev) => {
      const next = new Set(prev);
      const key = `${sel.artist}||${sel.album}`;
      for (const a of next) {
        if (`${a.artist}||${a.album}` === key) {
          next.delete(a);
          return next;
        }
      }
      next.add(sel);
      return next;
    });
  }, []);

  const handleAdd = async () => {
    if (totalSelectedCount === 0) return;
    try {
      await onAdd({
        songIds: Array.from(selectedIds),
        artistNames: Array.from(selectedArtists),
        albumSelectors: Array.from(selectedAlbums),
      });
      setSelectedIds(new Set());
      setSelectedArtists(new Set());
      setSelectedAlbums(new Set());
      setSearch("");
      setExpandedArtists(new Set());
      setExpandedAlbums(new Set());
      setArtistCache(new Map());
      setActiveArtist(null);
    } catch {
      // error handled by parent
    }
  };

  const handleClose = () => {
    if (isPending) return;
    setSelectedIds(new Set());
    setSelectedArtists(new Set());
    setSelectedAlbums(new Set());
    setSearch("");
    setArtistPage(1);
    setSearchPage(1);
    setExpandedArtists(new Set());
    setExpandedAlbums(new Set());
    setArtistCache(new Map());
    setActiveArtist(null);
    onClose();
  };

  const toggleAlbumExpanded = (artistAndAlbum: string) => {
    setExpandedAlbums((prev) => {
      const next = new Set(prev);
      if (next.has(artistAndAlbum)) next.delete(artistAndAlbum);
      else next.add(artistAndAlbum);
      return next;
    });
  };

  const toggleArtistExpanded = (name: string) => {
    setExpandedArtists((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
    setActiveArtist(name);
  };

  const isArtistSelected = (name: string) => selectedArtists.has(name);

  const isAlbumSelected = (artist: string, album: string) => {
    for (const a of selectedAlbums) {
      if (a.artist === artist && a.album === album) return true;
    }
    return false;
  };

  const isArtistIndeterminate = (name: string, songs?: Song[]) => {
    if (isArtistSelected(name)) return false;
    if (songs && songs.length > 0) {
      return songs.some((s) => selectedIds.has(s.id));
    }
    for (const a of selectedAlbums) {
      if (a.artist === name) return true;
    }
    return false;
  };

  const isAlbumIndeterminate = (artist: string, album: string, songs: Song[]) => {
    if (isAlbumSelected(artist, album)) return false;
    if (songs.length === 0) return false;
    return songs.some((s) => selectedIds.has(s.id)) && !songs.every((s) => selectedIds.has(s.id));
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="lg" fullWidth>
      <DialogTitle>{title}</DialogTitle>
      <DialogContent sx={{ minHeight: 400 }}>
        <TextField
          value={search}
          onChange={(e) => {
            setSearch(e.target.value);
            setSearchPage(1);
            setArtistPage(1);
            setExpandedArtists(new Set());
            setExpandedAlbums(new Set());
            setArtistCache(new Map());
            setActiveArtist(null);
          }}
          placeholder={searchPlaceholder}
          fullWidth
          size="small"
          sx={{ mb: 2, mt: 1 }}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position="start">
                  <Search fontSize="small" color="action" />
                </InputAdornment>
              ),
            },
          }}
        />

        {!!artistsQuery.data?.artists?.length && (
          <Box>
            {artistsQuery.data?.artists?.map((artist) => {
              const isExpanded = expandedArtists.has(artist.name);
              const rawExpandedSongs = isExpanded ? getArtistSongs(artist.name) : undefined;
              const expandedSongs = rawExpandedSongs ? filterExisting(rawExpandedSongs, existingSet) : undefined;
              const albums = expandedSongs ? groupSongsByAlbum(expandedSongs) : undefined;
              const isQueryLoading = isExpanded && activeArtist === artist.name && artistSongsQuery.isLoading;
              const hasCached = isExpanded && !!expandedSongs;
              const allExisting = existingArtistCounts
                ? (existingArtistCounts[artist.name] ?? 0) >= artist.song_count
                : false;

              return (
                <Box key={artist.name} sx={{ mb: 1 }}>
                  <ListItemButton
                    dense
                    sx={{ borderRadius: 1 }}
                    onClick={() => !allExisting && toggleArtistExpanded(artist.name)}
                  >
                    <ListItemIcon sx={{ minWidth: 48 }}>
                      <Checkbox
                        edge="start"
                        checked={isArtistSelected(artist.name)}
                        indeterminate={
                          isExpanded && hasCached
                            ? isArtistIndeterminate(artist.name, expandedSongs)
                            : isArtistIndeterminate(artist.name)
                        }
                        disabled={allExisting}
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleArtist(artist.name);
                        }}
                        tabIndex={-1}
                        disableRipple
                      />
                    </ListItemIcon>
                    <ListItemText
                      primary={artist.name || t("common:unknown_artist")}
                      slotProps={{
                        primary: {
                          sx: { fontWeight: 600, fontSize: "0.875rem" },
                        },
                      }}
                      secondary={t("common:song_count", {
                        count: artist.song_count,
                      })}
                    />
                    {allExisting ? (
                      <Chip
                        label={t("common:already_added")}
                        size="small"
                        variant="outlined"
                        color="success"
                        sx={{ height: 20, fontSize: "0.65rem", flexShrink: 0, mx: 1 }}
                      />
                    ) : (
                      <Chip
                        label={t("common:label_artist")}
                        size="small"
                        variant="outlined"
                        color="primary"
                        sx={{ height: 20, fontSize: "0.65rem", flexShrink: 0, mx: 1 }}
                      />
                    )}
                  </ListItemButton>

                  {isExpanded && (
                    <Box sx={{ ml: 5 }}>
                      {isQueryLoading && !hasCached ? (
                        <Box sx={{ display: "flex", justifyContent: "center", py: 2 }}>
                          <CircularProgress size={24} />
                        </Box>
                      ) : albums && Object.keys(albums).length > 0 ? (
                        Object.entries(albums).map(([album, albumSongs]) => {
                          const albumKey = `${artist.name}||${album}`;
                          const isAlbumExpanded = expandedAlbums.has(albumKey);
                          const albumDuration = albumSongs.reduce((sum, s) => sum + s.duration, 0);
                          const coverSong = albumSongs.find((s) => s.has_cover) ?? albumSongs[0];
                          const albumLabel = album || t("common:unknown_album");
                          return (
                            <Box key={albumKey}>
                              <ListItemButton
                                dense
                                sx={{ borderRadius: 1, py: 0.5 }}
                                onClick={() => toggleAlbumExpanded(albumKey)}
                              >
                                <ListItemIcon sx={{ minWidth: 40 }}>
                                  <Checkbox
                                    size="small"
                                    edge="start"
                                    checked={isArtistSelected(artist.name) || isAlbumSelected(artist.name, album)}
                                    indeterminate={
                                      !isArtistSelected(artist.name) &&
                                      !isAlbumSelected(artist.name, album) &&
                                      (albumSongs.some((s) => selectedIds.has(s.id)) ||
                                        isAlbumIndeterminate(artist.name, album, albumSongs))
                                    }
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      toggleAlbumSelector({ artist: artist.name, album });
                                    }}
                                    tabIndex={-1}
                                    disableRipple
                                  />
                                </ListItemIcon>
                                {coverSong && (
                                  <SongCover songId={coverSong.id} hasCover={coverSong.has_cover} size={28} />
                                )}
                                <ListItemText
                                  primary={albumLabel}
                                  slotProps={{
                                    primary: {
                                      sx: {
                                        fontWeight: 500,
                                        fontSize: "0.875rem",
                                      },
                                    },
                                  }}
                                  secondary={t("common:song_count", {
                                    count: albumSongs.length,
                                  })}
                                  sx={{ ml: 1.5 }}
                                />
                                <Chip
                                  label={t("common:label_album")}
                                  size="small"
                                  variant="outlined"
                                  color="primary"
                                  sx={{ height: 20, fontSize: "0.65rem", flexShrink: 0, mx: 1 }}
                                />
                                <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                                  {fmt(albumDuration)}
                                </Typography>
                              </ListItemButton>

                              {isAlbumExpanded && (
                                <List dense disablePadding>
                                  {albumSongs.map((song) => (
                                    <ListItem key={song.id} disablePadding>
                                      <ListItemButton
                                        dense
                                        onClick={() => toggleId(song.id)}
                                        sx={{ borderRadius: 1, pl: 7 }}
                                      >
                                        <ListItemIcon sx={{ minWidth: 36 }}>
                                          <Checkbox
                                            size="small"
                                            edge="start"
                                            checked={
                                              selectedIds.has(song.id) ||
                                              isArtistSelected(song.artist) ||
                                              isAlbumSelected(song.artist, song.album)
                                            }
                                            tabIndex={-1}
                                            disableRipple
                                          />
                                        </ListItemIcon>
                                        <SongCover songId={song.id} hasCover={song.has_cover} size={28} />
                                        <Typography variant="body2" sx={{ ml: 1.5, flex: 1, minWidth: 0 }}>
                                          {song.title}
                                        </Typography>
                                        <Chip
                                          label={t("common:label_song")}
                                          size="small"
                                          variant="outlined"
                                          color="primary"
                                          sx={{ height: 20, fontSize: "0.65rem", flexShrink: 0, mx: 1 }}
                                        />
                                        <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                                          {fmt(song.duration)}
                                        </Typography>
                                      </ListItemButton>
                                    </ListItem>
                                  ))}
                                </List>
                              )}
                            </Box>
                          );
                        })
                      ) : albums ? (
                        <Typography variant="body2" color="text.secondary" sx={{ textAlign: "center", py: 2 }}>
                          {t("common:all_in_library")}
                        </Typography>
                      ) : (
                        <Box sx={{ display: "flex", justifyContent: "center", py: 2 }}>
                          <CircularProgress size={24} />
                        </Box>
                      )}
                    </Box>
                  )}
                </Box>
              );
            })}

            {(artistsQuery.data?.total ?? 0) > PER_PAGE && (
              <Box sx={{ display: "flex", justifyContent: "center", mt: 2 }}>
                <Pagination
                  size="small"
                  count={Math.ceil((artistsQuery.data?.total ?? 0) / PER_PAGE)}
                  page={artistPage}
                  onChange={(_, p) => {
                    setArtistPage(p);
                    setExpandedArtists(new Set());
                    setExpandedAlbums(new Set());
                    setArtistCache(new Map());
                    setActiveArtist(null);
                  }}
                />
              </Box>
            )}
          </Box>
        )}

        {!query ? (
          artistsQuery.isLoading ? (
            <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
              <CircularProgress />
            </Box>
          ) : !artistsQuery.data?.artists?.length ? (
            <Typography color="text.secondary" sx={{ py: 4, textAlign: "center" }}>
              {emptyLabel}
            </Typography>
          ) : null
        ) : (
          <>
            {!!artistsQuery.data?.artists?.length && !!searchQuery.data?.songs?.length && <Divider sx={{ my: 2 }} />}

            {searchQuery.isLoading ? (
              !artistsQuery.data?.artists?.length && (
                <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
                  <CircularProgress />
                </Box>
              )
            ) : searchQuery.data?.songs?.length ? (
              <List dense disablePadding>
                {filterExisting(searchQuery.data.songs, existingSet).map((song) => (
                  <ListItem key={song.id} disablePadding>
                    <ListItemButton dense onClick={() => toggleId(song.id)} sx={{ borderRadius: 1 }}>
                      <ListItemIcon sx={{ minWidth: 36 }}>
                        <Checkbox
                          size="small"
                          edge="start"
                          checked={selectedIds.has(song.id)}
                          tabIndex={-1}
                          disableRipple
                        />
                      </ListItemIcon>
                      <SongCover songId={song.id} hasCover={song.has_cover} size={28} />
                      <ListItemText
                        primary={song.title}
                        secondary={(song.album || t("common:unknown_album")) + (song.artist ? ` — ${song.artist}` : "")}
                        slotProps={{
                          primary: { sx: { fontSize: "0.875rem" } },
                          secondary: { variant: "caption" },
                        }}
                        sx={{ ml: 1.5 }}
                      />
                      <Chip
                        label={t("common:label_song")}
                        size="small"
                        variant="outlined"
                        color="primary"
                        sx={{ height: 20, fontSize: "0.65rem", flexShrink: 0, mx: 1 }}
                      />
                      <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                        {fmt(song.duration)}
                      </Typography>
                    </ListItemButton>
                  </ListItem>
                ))}
              </List>
            ) : !artistsQuery.isLoading && !artistsQuery.data?.artists?.length ? (
              <Typography color="text.secondary" sx={{ py: 4, textAlign: "center" }}>
                {t("common:search_empty")}
              </Typography>
            ) : null}

            {(searchQuery.data?.total ?? 0) > PER_PAGE && (
              <Box sx={{ display: "flex", justifyContent: "center", mt: 2 }}>
                <Pagination
                  size="small"
                  count={Math.ceil((searchQuery.data?.total ?? 0) / PER_PAGE)}
                  page={searchPage}
                  onChange={(_, p) => setSearchPage(p)}
                />
              </Box>
            )}
          </>
        )}

        {totalSelectedCount > 0 && (
          <Box sx={{ borderTop: 1, borderColor: "divider", mt: 2, pt: 1.5 }}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                {t("common:selected_count", { count: totalSongCount })}
              </Typography>
              <Button
                size="small"
                onClick={() => {
                  setSelectedIds(new Set());
                  setSelectedArtists(new Set());
                  setSelectedAlbums(new Set());
                }}
              >
                {t("common:clear")}
              </Button>
              <Box sx={{ flex: 1 }} />
              {selectedSongs.length > 0 && (
                <Button size="small" onClick={() => setShowSelected(!showSelected)}>
                  {showSelected ? t("common:hide") : t("common:show")}
                </Button>
              )}
            </Box>
            {selectedArtists.size > 0 && (
              <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mt: 1 }}>
                {Array.from(selectedArtists).map((name) => (
                  <Chip
                    key={name}
                    icon={<Person fontSize="small" />}
                    label={`${name || t("common:unknown_artist")} (${t("common:all_songs")})`}
                    size="small"
                    color="primary"
                    variant="outlined"
                    onDelete={() => toggleArtist(name)}
                  />
                ))}
              </Box>
            )}
            {selectedAlbums.size > 0 && (
              <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mt: 0.5 }}>
                {Array.from(selectedAlbums).map((sel) => (
                  <Chip
                    key={`${sel.artist}||${sel.album}`}
                    icon={<LibraryMusic fontSize="small" />}
                    label={`${sel.album || t("common:unknown_album")} — ${sel.artist} (${t("common:all_songs")})`}
                    size="small"
                    color="secondary"
                    variant="outlined"
                    onDelete={() => toggleAlbumSelector(sel)}
                  />
                ))}
              </Box>
            )}
            <Collapse in={showSelected}>
              <List dense disablePadding sx={{ mt: 1 }}>
                {selectedSongs.map((song) => (
                  <ListItem
                    key={song.id}
                    disablePadding
                    secondaryAction={
                      <IconButton edge="end" size="small" onClick={() => toggleId(song.id)}>
                        <Close fontSize="small" />
                      </IconButton>
                    }
                  >
                    <ListItemIcon sx={{ minWidth: 32 }}>
                      <SongCover songId={song.id} hasCover={song.has_cover} size={28} />
                    </ListItemIcon>
                    <ListItemText
                      primary={song.title}
                      secondary={song.artist || t("common:unknown_artist")}
                      slotProps={{ primary: { variant: "body2" }, secondary: { variant: "caption" } }}
                    />
                  </ListItem>
                ))}
              </List>
            </Collapse>
          </Box>
        )}
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={handleClose} disabled={isPending}>
          {t("common:cancel")}
        </Button>
        <Button variant="contained" onClick={handleAdd} disabled={totalSelectedCount === 0 || isPending}>
          {isPending && <CircularProgress size={16} sx={{ mr: 1 }} />}
          {isPending ? t("common:adding") : addLabel(totalSongCount)}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
