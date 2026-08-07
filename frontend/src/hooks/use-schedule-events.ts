import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { scheduleEventsService } from "@/lib/services/schedule-events";
import type { CreateScheduleEventRequest } from "@/types";

export function useScheduleEvents(stationId: string, from?: string, to?: string) {
  return useQuery({
    queryKey: ["schedule-events", stationId, from, to],
    queryFn: () => {
      const params = new URLSearchParams();
      if (from) params.set("from", from);
      if (to) params.set("to", to);
      return scheduleEventsService.list(stationId, from, to);
    },
    enabled: !!stationId,
  });
}

export function useCreateScheduleEvent(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateScheduleEventRequest) => scheduleEventsService.create(stationId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedule-events", stationId] });
      queryClient.invalidateQueries({ queryKey: ["schedules", stationId] });
    },
  });
}

export function useUpdateScheduleEvent(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<CreateScheduleEventRequest> }) =>
      scheduleEventsService.update(stationId, id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedule-events", stationId] });
      queryClient.invalidateQueries({ queryKey: ["schedules", stationId] });
    },
  });
}

export function useDeleteScheduleEvent(stationId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => scheduleEventsService.delete(stationId, id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedule-events", stationId] });
      queryClient.invalidateQueries({ queryKey: ["schedules", stationId] });
    },
  });
}
