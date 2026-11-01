import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type {
  AlbumSelector,
  PaginatedArtists,
  PaginatedSongs,
  ScanReport,
  Song,
  SongDetails,
  UploadSongResult,
} from "@/types";

function createFormData(data: {
  file: File;
  title?: string;
  artist?: string;
  album?: string;
  assign_to_all?: boolean;
  station_ids?: string[];
}) {
  const fd = new FormData();
  fd.append("file", data.file);
  if (data.title) fd.append("title", data.title);
  if (data.artist) fd.append("artist", data.artist);
  if (data.album) fd.append("album", data.album);
  if (data.assign_to_all) fd.append("assign_to_all", "true");
  else if (data.station_ids?.length) fd.append("station_ids", JSON.stringify(data.station_ids));
  return fd;
}

export function createSongsService(client: HttpClient) {
  return {
    list: () => client.get<Song[]>("/songs"),
    get: (id: string) => client.get<Song>(`/songs/${id}`),
    details: (id: string) => client.get<SongDetails>(`/songs/${id}/details`),
    scan: () => client.post<ScanReport>("/songs/scan", {}),
    upload: (data: Parameters<typeof createFormData>[0] & { allow_duplicate?: boolean }) => {
      const qs = data.allow_duplicate ? "?allow_duplicate=true" : "";
      return client.postFormData<UploadSongResult>(`/songs${qs}`, createFormData(data));
    },
    uploadZip: (data: { file: File; assign_to_all?: boolean; station_ids?: string[]; allow_duplicate?: boolean }) => {
      const fd = new FormData();
      fd.append("file", data.file);
      if (data.assign_to_all) fd.append("assign_to_all", "true");
      else if (data.station_ids?.length) fd.append("station_ids", JSON.stringify(data.station_ids));
      const qs = data.allow_duplicate ? "?allow_duplicate=true" : "";
      return client.postFormData<UploadSongResult[]>(`/songs/zip${qs}`, fd);
    },
    update: (id: string, data: { title?: string; artist?: string; album?: string; duration?: number }) =>
      client.put<Song>(`/songs/${id}`, data),
    delete: (id: string) => client.delete(`/songs/${id}`),
    deleteBatch: (ids: string[]) => client.delete("/songs/batch", { ids }),
    purgeMissing: () => client.delete("/songs/missing"),
    search: (params: { q?: string; artist?: string; album?: string; page?: number; per_page?: number }) => {
      const query = new URLSearchParams();
      if (params.q) query.set("q", params.q);
      if (params.artist !== undefined) query.set("artist", params.artist);
      if (params.album) query.set("album", params.album);
      if (params.page) query.set("page", String(params.page));
      if (params.per_page) query.set("per_page", String(params.per_page));
      const qs = query.toString();
      return client.get<PaginatedSongs>(`/songs/search${qs ? `?${qs}` : ""}`);
    },
    listArtists: (params: { q?: string; page?: number; per_page?: number }) => {
      const query = new URLSearchParams();
      if (params.q) query.set("q", params.q);
      if (params.page) query.set("page", String(params.page));
      if (params.per_page) query.set("per_page", String(params.per_page));
      const qs = query.toString();
      return client.get<PaginatedArtists>(`/songs/artists${qs ? `?${qs}` : ""}`);
    },
    countSongs: (data: { artist_names: string[]; album_selectors: AlbumSelector[]; exclude_ids?: string[] }) =>
      client.post<{ count: number }>("/songs/count", data),
  };
}

export const songsService = createSongsService(httpClient);
