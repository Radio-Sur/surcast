import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { stationLibraryService } from "@/lib/services/station-library";
import type { SongSelections } from "@/types";

export function useStationSongs(stationId: string, page: number = 1, perPage: number = 50) {
  return useQuery({
    queryKey: ["station-songs", stationId, page, perPage],
    queryFn: () => stationLibraryService.list(stationId, { page, per_page: perPage }),
    enabled: !!stationId,
  });
}

export function useStationSongsAll(stationId: string) {
  return useQuery({
    queryKey: ["station-songs", stationId, "all"],
    queryFn: () => stationLibraryService.list(stationId, { per_page: 100000 }),
    enabled: !!stationId,
    staleTime: 30000,
  });
}

export function useAddStationSongs(stationId: string) {
  return useMutation({
    mutationFn: (data: SongSelections) => stationLibraryService.add(stationId, data),
  });
}

export function useRemoveStationSong(stationId: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (songId: string) => stationLibraryService.remove(stationId, songId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["station-songs", stationId] });
      queryClient.invalidateQueries({ queryKey: ["station-queue", stationId] });
    },
  });
}
