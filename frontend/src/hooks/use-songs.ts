import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { songsService } from "@/lib/services/songs";
import type { AlbumSelector, PaginatedArtists, PaginatedSongs, ScanReport, Song, SongDetails } from "@/types";

function useInvalidateLibrary() {
  const queryClient = useQueryClient();
  return () => {
    queryClient.invalidateQueries({ queryKey: ["songs"] });
    queryClient.invalidateQueries({ queryKey: ["files"] });
  };
}

export function useSongs<T = Song[]>(select?: (songs: Song[]) => T) {
  return useQuery({
    queryKey: ["songs"],
    queryFn: () => songsService.list(),
    select,
  });
}

export function useSong(id: string) {
  return useQuery({
    queryKey: ["songs", id],
    queryFn: () => songsService.get(id),
    enabled: !!id,
  });
}

export function useSongDetails(id: string, options?: { enabled?: boolean }) {
  return useQuery<SongDetails>({
    queryKey: ["songs", id, "details"],
    queryFn: () => songsService.details(id),
    enabled: (options?.enabled ?? true) && !!id,
  });
}

export function useSongSearch(
  params: { q?: string; artist?: string; album?: string; page?: number; per_page?: number },
  options?: { enabled?: boolean },
) {
  return useQuery<PaginatedSongs>({
    queryKey: ["songs", "search", params],
    queryFn: () => songsService.search(params),
    placeholderData: (prev) => prev,
    ...(options?.enabled !== undefined ? { enabled: options.enabled } : {}),
  });
}

export function useSongCount(
  params: { artistNames: string[]; albumSelectors: AlbumSelector[] },
  options?: { enabled?: boolean },
  existingSet?: Set<string>,
) {
  return useQuery({
    queryKey: ["songs", "count", params],
    queryFn: () =>
      songsService.countSongs({
        artist_names: params.artistNames,
        album_selectors: params.albumSelectors,
        exclude_ids: existingSet && existingSet.size > 0 ? Array.from(existingSet) : undefined,
      }),
    enabled: options?.enabled,
  });
}

export function useArtists(params: { q?: string; page?: number; per_page?: number }, options?: { enabled?: boolean }) {
  return useQuery<PaginatedArtists>({
    queryKey: ["songs", "artists", params],
    queryFn: () => songsService.listArtists(params),
    placeholderData: (prev) => prev,
    ...(options?.enabled !== undefined ? { enabled: options.enabled } : {}),
  });
}

export function useDeleteSong() {
  const invalidate = useInvalidateLibrary();
  return useMutation({
    mutationFn: (id: string) => songsService.delete(id),
    onSuccess: invalidate,
  });
}

export function useDeleteSongsBatch() {
  const invalidate = useInvalidateLibrary();
  return useMutation({
    mutationFn: (ids: string[]) => songsService.deleteBatch(ids),
    onSuccess: invalidate,
  });
}

export function usePurgeMissingSongs() {
  const invalidate = useInvalidateLibrary();
  return useMutation({
    mutationFn: () => songsService.purgeMissing(),
    onSuccess: invalidate,
  });
}

export function useUploadSong() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: Parameters<typeof songsService.upload>[0]) => songsService.upload(data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["songs"] }),
  });
}

export function useUploadZip() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: { file: File; assign_to_all?: boolean; station_ids?: string[] }) => songsService.uploadZip(data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["songs"] }),
  });
}

export function useScanLibrary() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => songsService.scan(),
    onSuccess: (report: ScanReport) => {
      if (!report.throttled && (report.added > 0 || report.renamed > 0 || report.marked_missing > 0)) {
        queryClient.invalidateQueries({ queryKey: ["songs"] });
      }
    },
  });
}

export function useUpdateSong() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      data,
    }: {
      id: string;
      data: { title?: string; artist?: string; album?: string; duration?: number };
    }) => songsService.update(id, data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["songs"] }),
  });
}
