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
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { fmt } from "@/components/queue";
import { SongCover } from "@/components/song-cover";
import type { AlbumSelector, StationSong } from "@/types";

const PER_PAGE = 15;

interface AlbumGroup {
  album: string;
  songs: StationSong[];
}

interface ArtistGroup {
  name: string;
  songs: StationSong[];
  albums: AlbumGroup[];
}

function buildArtistGroups(songs: StationSong[]): ArtistGroup[] {
  const map = new Map<string, ArtistGroup>();
  for (const song of songs) {
    const artist = song.artist || "";
    let group = map.get(artist);
    if (!group) {
      group = { name: artist, songs: [], albums: [] };
      map.set(artist, group);
    }
    group.songs.push(song);
    const album = song.album || "";
    let albumGroup = group.albums.find((a) => a.album === album);
    if (!albumGroup) {
      albumGroup = { album, songs: [] };
      group.albums.push(albumGroup);
    }
    albumGroup.songs.push(song);
  }
  return Array.from(map.values()).sort((a, b) => a.name.localeCompare(b.name));
}

export function QueueAddDialog({
  open,
  librarySongs,
  isPending,
  onAdd,
  onClose,
}: {
  open: boolean;
  librarySongs: StationSong[];
  isPending: boolean;
  onAdd: (songIds: string[]) => Promise<void>;
  onClose: () => void;
}) {
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

  const query = search.trim().toLowerCase();

  const artistGroups = useMemo(() => buildArtistGroups(librarySongs), [librarySongs]);

  const filteredArtists = useMemo(() => {
    if (!query) return artistGroups;
    return artistGroups.filter((g) => g.name.toLowerCase().includes(query));
  }, [artistGroups, query]);

  const filteredSongs = useMemo(() => {
    if (!query) return [];
    return librarySongs.filter((s) => s.title.toLowerCase().includes(query) || s.album.toLowerCase().includes(query));
  }, [librarySongs, query]);

  const artistSongCount = useCallback(
    (name: string) => {
      const g = artistGroups.find((x) => x.name === name);
      return g ? g.songs.length : 0;
    },
    [artistGroups],
  );

  const albumSongCount = useCallback(
    (artist: string, album: string) => {
      const g = artistGroups.find((x) => x.name === artist);
      return g ? (g.albums.find((a) => a.album === album)?.songs.length ?? 0) : 0;
    },
    [artistGroups],
  );

  const totalSongCount = useMemo(() => {
    let n = selectedIds.size;
    for (const name of selectedArtists) n += artistSongCount(name);
    for (const sel of selectedAlbums) n += albumSongCount(sel.artist, sel.album);
    return n;
  }, [selectedIds, selectedArtists, selectedAlbums, artistSongCount, albumSongCount]);

  const totalSelectedCount = selectedIds.size + selectedArtists.size + selectedAlbums.size;

  const selectedSongIds = useMemo(() => {
    const ids = new Set(selectedIds);
    for (const name of selectedArtists) {
      const g = artistGroups.find((x) => x.name === name);
      if (g) for (const s of g.songs) ids.add(s.song_id);
    }
    for (const sel of selectedAlbums) {
      const g = artistGroups.find((x) => x.name === sel.artist);
      if (g) {
        const ag = g.albums.find((a) => a.album === sel.album);
        if (ag) for (const s of ag.songs) ids.add(s.song_id);
      }
    }
    return Array.from(ids);
  }, [selectedIds, selectedArtists, selectedAlbums, artistGroups]);

  const selectedSongs = useMemo(
    () => librarySongs.filter((s) => selectedIds.has(s.song_id)),
    [librarySongs, selectedIds],
  );

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

  const isArtistSelected = (name: string) => selectedArtists.has(name);

  const isAlbumSelected = (artist: string, album: string) => {
    for (const a of selectedAlbums) {
      if (a.artist === artist && a.album === album) return true;
    }
    return false;
  };

  const isArtistIndeterminate = (name: string) => {
    if (isArtistSelected(name)) return false;
    const g = artistGroups.find((x) => x.name === name);
    if (!g) return false;
    if (g.albums.some((ag) => isAlbumSelected(name, ag.album))) return true;
    const anySelected = g.songs.some((s) => selectedIds.has(s.song_id));
    const allSelected = g.songs.every((s) => selectedIds.has(s.song_id));
    return anySelected && !allSelected;
  };

  const isAlbumIndeterminate = (artist: string, album: string) => {
    if (isAlbumSelected(artist, album)) return false;
    const g = artistGroups.find((x) => x.name === artist);
    const ag = g?.albums.find((a) => a.album === album);
    if (!ag || ag.songs.length === 0) return false;
    const anySelected = ag.songs.some((s) => selectedIds.has(s.song_id));
    const allSelected = ag.songs.every((s) => selectedIds.has(s.song_id));
    return anySelected && !allSelected;
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
  };

  const clearSelections = () => {
    setSelectedIds(new Set());
    setSelectedArtists(new Set());
    setSelectedAlbums(new Set());
  };

  const resetState = () => {
    setSearch("");
    setArtistPage(1);
    setSearchPage(1);
    setSelectedIds(new Set());
    setSelectedArtists(new Set());
    setSelectedAlbums(new Set());
    setExpandedArtists(new Set());
    setExpandedAlbums(new Set());
    setShowSelected(false);
  };

  const handleAdd = async () => {
    if (selectedSongIds.length === 0) return;
    try {
      await onAdd(selectedSongIds);
      resetState();
    } catch {
      // error handled by parent
    }
  };

  const handleClose = () => {
    if (isPending) return;
    resetState();
    onClose();
  };

  const handleSearchChange = (value: string) => {
    setSearch(value);
    setArtistPage(1);
    setSearchPage(1);
    setExpandedArtists(new Set());
    setExpandedAlbums(new Set());
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="lg" fullWidth>
      <DialogTitle>{t("stations:queue_add_dialog_title")}</DialogTitle>
      <DialogContent sx={{ minHeight: 400 }}>
        <TextField
          value={search}
          onChange={(e) => handleSearchChange(e.target.value)}
          placeholder={t("songs:search_placeholder")}
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

        {!!filteredArtists.length && (
          <Box>
            {filteredArtists.slice((artistPage - 1) * PER_PAGE, artistPage * PER_PAGE).map((artist) => {
              const isExpanded = expandedArtists.has(artist.name);

              return (
                <Box key={artist.name} sx={{ mb: 1 }}>
                  <ListItemButton dense sx={{ borderRadius: 1 }} onClick={() => toggleArtistExpanded(artist.name)}>
                    <ListItemIcon sx={{ minWidth: 48 }}>
                      <Checkbox
                        edge="start"
                        checked={isArtistSelected(artist.name)}
                        indeterminate={isArtistIndeterminate(artist.name)}
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
                        count: artist.songs.length,
                      })}
                    />
                    <Chip
                      label={t("common:label_artist")}
                      size="small"
                      variant="outlined"
                      color="primary"
                      sx={{ height: 20, fontSize: "0.65rem", flexShrink: 0, mx: 1 }}
                    />
                  </ListItemButton>

                  {isExpanded && (
                    <Box sx={{ ml: 5 }}>
                      {artist.albums.map((album) => {
                        const albumKey = `${artist.name}||${album.album}`;
                        const isAlbumExpanded = expandedAlbums.has(albumKey);
                        const albumDuration = album.songs.reduce((sum, s) => sum + s.duration, 0);
                        const coverSong = album.songs.find((s) => s.has_cover) ?? album.songs[0];
                        const albumLabel = album.album || t("common:unknown_album");
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
                                  checked={isArtistSelected(artist.name) || isAlbumSelected(artist.name, album.album)}
                                  indeterminate={isAlbumIndeterminate(artist.name, album.album)}
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    toggleAlbumSelector({ artist: artist.name, album: album.album });
                                  }}
                                  tabIndex={-1}
                                  disableRipple
                                />
                              </ListItemIcon>
                              {coverSong && (
                                <SongCover songId={coverSong.song_id} hasCover={coverSong.has_cover} size={28} />
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
                                  count: album.songs.length,
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
                                {album.songs.map((song) => (
                                  <ListItem key={song.song_id} disablePadding>
                                    <ListItemButton
                                      dense
                                      onClick={() => toggleId(song.song_id)}
                                      sx={{ borderRadius: 1, pl: 7 }}
                                    >
                                      <ListItemIcon sx={{ minWidth: 36 }}>
                                        <Checkbox
                                          size="small"
                                          edge="start"
                                          checked={
                                            selectedIds.has(song.song_id) ||
                                            isArtistSelected(song.artist) ||
                                            isAlbumSelected(song.artist, song.album)
                                          }
                                          tabIndex={-1}
                                          disableRipple
                                        />
                                      </ListItemIcon>
                                      <SongCover songId={song.song_id} hasCover={song.has_cover} size={28} />
                                      <Typography variant="body2" sx={{ ml: 1.5, flex: 1, minWidth: 0 }}>
                                        {song.title}
                                      </Typography>
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
                      })}
                    </Box>
                  )}
                </Box>
              );
            })}

            {filteredArtists.length > PER_PAGE && (
              <Box sx={{ display: "flex", justifyContent: "center", mt: 2 }}>
                <Pagination
                  size="small"
                  count={Math.ceil(filteredArtists.length / PER_PAGE)}
                  page={artistPage}
                  onChange={(_, p) => {
                    setArtistPage(p);
                    setExpandedArtists(new Set());
                    setExpandedAlbums(new Set());
                  }}
                />
              </Box>
            )}
          </Box>
        )}

        {!query ? (
          !filteredArtists.length && (
            <Typography color="text.secondary" sx={{ py: 4, textAlign: "center" }}>
              {t("stations:queue_add_dialog_empty")}
            </Typography>
          )
        ) : (
          <>
            {!!filteredArtists.length && !!filteredSongs.length && <Divider sx={{ my: 2 }} />}

            {filteredSongs.length ? (
              <List dense disablePadding>
                {filteredSongs.slice((searchPage - 1) * PER_PAGE, searchPage * PER_PAGE).map((song) => (
                  <ListItem key={song.song_id} disablePadding>
                    <ListItemButton dense onClick={() => toggleId(song.song_id)} sx={{ borderRadius: 1 }}>
                      <ListItemIcon sx={{ minWidth: 36 }}>
                        <Checkbox
                          size="small"
                          edge="start"
                          checked={selectedIds.has(song.song_id)}
                          tabIndex={-1}
                          disableRipple
                        />
                      </ListItemIcon>
                      <SongCover songId={song.song_id} hasCover={song.has_cover} size={28} />
                      <ListItemText
                        primary={song.title}
                        secondary={(song.album || t("common:unknown_album")) + (song.artist ? ` — ${song.artist}` : "")}
                        slotProps={{
                          primary: { sx: { fontSize: "0.875rem" } },
                          secondary: { variant: "caption" },
                        }}
                        sx={{ ml: 1.5 }}
                      />
                      <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                        {fmt(song.duration)}
                      </Typography>
                    </ListItemButton>
                  </ListItem>
                ))}
              </List>
            ) : !filteredArtists.length ? (
              <Typography color="text.secondary" sx={{ py: 4, textAlign: "center" }}>
                {t("common:search_empty")}
              </Typography>
            ) : null}

            {filteredSongs.length > PER_PAGE && (
              <Box sx={{ display: "flex", justifyContent: "center", mt: 2 }}>
                <Pagination
                  size="small"
                  count={Math.ceil(filteredSongs.length / PER_PAGE)}
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
              <Button size="small" onClick={clearSelections}>
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
                    key={song.song_id}
                    disablePadding
                    secondaryAction={
                      <IconButton edge="end" size="small" onClick={() => toggleId(song.song_id)}>
                        <Close fontSize="small" />
                      </IconButton>
                    }
                  >
                    <ListItemIcon sx={{ minWidth: 32 }}>
                      <SongCover songId={song.song_id} hasCover={song.has_cover} size={28} />
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
        <Button variant="contained" onClick={handleAdd} disabled={selectedSongIds.length === 0 || isPending}>
          {isPending && <CircularProgress size={16} sx={{ mr: 1 }} />}
          {isPending ? t("common:adding") : t("stations:queue_add_dialog_button", { count: totalSongCount })}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
