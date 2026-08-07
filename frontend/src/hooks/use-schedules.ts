import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { schedulesService } from "@/lib/services/schedules";
import type { ScheduleSourceType } from "@/types";

export function useSchedules(stationId: string) {
  return useQuery({
    queryKey: ["schedules", stationId],
    queryFn: () => schedulesService.list(stationId),
    enabled: !!stationId,
  });
}

export function useCreateSchedule(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: {
      day_of_week: number;
      start_time: string;
      end_time: string;
      source_type?: ScheduleSourceType;
      playlist_id?: string | null;
      auto_dj_mode?: string | null;
      auto_dj_avoid_repeat?: boolean | null;
      auto_dj_min_gap?: number | null;
      auto_dj_songs_ahead?: number | null;
    }) => schedulesService.create(stationId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedules", stationId] });
    },
  });
}

export function useUpdateSchedule(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      id,
      data,
    }: {
      id: string;
      data: {
        day_of_week?: number;
        start_time?: string;
        end_time?: string;
        source_type?: ScheduleSourceType;
        playlist_id?: string | null;
        auto_dj_mode?: string | null;
        auto_dj_avoid_repeat?: boolean | null;
        auto_dj_min_gap?: number | null;
        auto_dj_songs_ahead?: number | null;
      };
    }) => schedulesService.update(stationId, id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedules", stationId] });
    },
  });
}

export function useDeleteSchedule(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => schedulesService.delete(stationId, id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedules", stationId] });
    },
  });
}
