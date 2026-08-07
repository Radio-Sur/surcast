import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { stationQueueService } from "@/lib/services/station-queue";

export function useStationQueue(stationId: string) {
  return useQuery({
    queryKey: ["station-queue", stationId],
    queryFn: () => stationQueueService.list(stationId),
    enabled: !!stationId,
  });
}

export function useAddToQueue(stationId: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (songIds: string[]) => stationQueueService.add(stationId, songIds),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["station-queue", stationId] });
    },
  });
}

export function useRemoveFromQueue(stationId: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (itemId: string) => stationQueueService.remove(stationId, itemId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["station-queue", stationId] });
    },
  });
}

export function useReorderQueue(stationId: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (itemIds: string[]) => stationQueueService.reorder(stationId, itemIds),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["station-queue", stationId] });
    },
  });
}

export function useInsertIntoQueue(stationId: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (params: { song_id: string; position: number }) => stationQueueService.insert(stationId, params),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["station-queue", stationId] });
    },
  });
}

export function useRemovePlaylistFromQueue(stationId: string) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (playlistId: string) => stationQueueService.removePlaylist(stationId, playlistId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["station-queue", stationId] });
    },
  });
}
