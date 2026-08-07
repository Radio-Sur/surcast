import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { PaginatedPlaylistSongs, Playlist, PlaylistSong, SongSelections } from "@/types";

export function createPlaylistsService(client: HttpClient) {
  return {
    list: () => client.get<Playlist[]>("/playlists"),
    get: (id: string) => client.get<Playlist>(`/playlists/${id}`),
    create: (data: { name: string; description?: string }) => client.post<Playlist>("/playlists", data),
    update: (id: string, data: { name?: string; description?: string }) =>
      client.put<Playlist>(`/playlists/${id}`, data),
    delete: (id: string) => client.delete(`/playlists/${id}`),
    getSongs: (id: string, params?: { page?: number; per_page?: number }) => {
      const q = params
        ? "?" +
          Object.entries(params)
            .filter(([_, v]) => v !== undefined)
            .map(([k, v]) => `${k}=${v}`)
            .join("&")
        : "";
      return client.get<PaginatedPlaylistSongs>(`/playlists/${id}/songs${q}`);
    },
    addSongs: (id: string, data: SongSelections) =>
      client.post<PlaylistSong[]>(`/playlists/${id}/songs`, {
        song_ids: data.songIds,
        artist_names: data.artistNames,
        album_selectors: data.albumSelectors,
      }),
    removeSong: (id: string, song_id: string) => client.delete(`/playlists/${id}/songs/${song_id}`),
    removeSongsBatch: (id: string, song_ids: string[]) => client.delete(`/playlists/${id}/songs/batch`, { song_ids }),
    reorderSongs: (id: string, song_ids: string[]) =>
      client.put<PlaylistSong[]>(`/playlists/${id}/songs/reorder`, { song_ids }),
    addToQueue: (playlist_id: string, station_id: string) =>
      client.post(`/playlists/${playlist_id}/add-to-queue/${station_id}`),
  };
}

export const playlistsService = createPlaylistsService(httpClient);
