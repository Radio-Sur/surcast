import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { playlistsService } from "@/lib/services/playlists";
import type { Playlist, SongSelections } from "@/types";

export function usePlaylists<T = Playlist[]>(select?: (playlists: Playlist[]) => T) {
  return useQuery({
    queryKey: ["playlists"],
    queryFn: () => playlistsService.list(),
    select,
  });
}

export function usePlaylist(id: string) {
  return useQuery({
    queryKey: ["playlists", id],
    queryFn: () => playlistsService.get(id),
    enabled: !!id,
  });
}

export function useCreatePlaylist() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (data: { name: string; description?: string }) => playlistsService.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["playlists"] });
    },
  });
}

export function useUpdatePlaylist(id: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (data: { name?: string; description?: string }) => playlistsService.update(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["playlists"] });
    },
  });
}

export function useDeletePlaylist() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (id: string) => playlistsService.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["playlists"] });
    },
  });
}

export function usePlaylistSongs(id: string, page: number = 1, perPage: number = 50) {
  return useQuery({
    queryKey: ["playlists", id, "songs", page, perPage],
    queryFn: () => playlistsService.getSongs(id, { page, per_page: perPage }),
    enabled: !!id,
  });
}

export function usePlaylistSongsAll(id: string) {
  return useQuery({
    queryKey: ["playlists", id, "songs", "all"],
    queryFn: () => playlistsService.getSongs(id, { per_page: 100000 }),
    enabled: !!id,
    staleTime: 30000,
  });
}

export function useAddPlaylistSongs(id: string) {
  return useMutation({
    mutationFn: (data: SongSelections) => playlistsService.addSongs(id, data),
  });
}

export function useRemovePlaylistSong(id: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (song_id: string) => playlistsService.removeSong(id, song_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["playlists", id, "songs"] });
      queryClient.invalidateQueries({ queryKey: ["playlists"] });
    },
  });
}

export function useRemovePlaylistSongsBatch(id: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (song_ids: string[]) => playlistsService.removeSongsBatch(id, song_ids),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["playlists", id, "songs"] });
      queryClient.invalidateQueries({ queryKey: ["playlists"] });
    },
  });
}

export function useReorderPlaylistSongs(id: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (song_ids: string[]) => playlistsService.reorderSongs(id, song_ids),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["playlists", id, "songs"] });
      queryClient.invalidateQueries({ queryKey: ["playlists"] });
    },
  });
}

export function useAddPlaylistToQueue() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ playlist_id, station_id }: { playlist_id: string; station_id: string }) =>
      playlistsService.addToQueue(playlist_id, station_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["station-queue"] });
    },
  });
}
