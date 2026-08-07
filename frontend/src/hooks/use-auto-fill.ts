import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { autoFillService } from "@/lib/services/auto-fill";
import type { ScheduleSourceType } from "@/types";

export function useAutoFill(stationId: string) {
  return useQuery({
    queryKey: ["auto-fill", stationId],
    queryFn: () => autoFillService.get(stationId),
    enabled: !!stationId,
  });
}

export function useUpdateAutoFill(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: {
      enabled?: boolean;
      mode?: string;
      source_type?: ScheduleSourceType;
      source_playlist_id?: string | null;
      avoid_artist_repeat?: boolean;
      min_song_gap?: number;
      songs_ahead?: number;
    }) => autoFillService.update(stationId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["auto-fill", stationId] });
    },
  });
}

export function useAddAutoFillPlaylist(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: { playlist_id: string; weight?: number }) => autoFillService.addPlaylist(stationId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["auto-fill", stationId] });
    },
  });
}

export function useUpdateAutoFillPlaylist(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: { weight?: number } }) =>
      autoFillService.updatePlaylist(stationId, id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["auto-fill", stationId] });
    },
  });
}

export function useDeleteAutoFillPlaylist(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => autoFillService.deletePlaylist(stationId, id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["auto-fill", stationId] });
    },
  });
}

export function useTriggerAutoFill(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => autoFillService.trigger(stationId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["station-queue", stationId] });
    },
  });
}
