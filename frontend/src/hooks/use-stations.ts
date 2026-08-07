import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { stationsService } from "@/lib/services/stations";

export function useStations() {
  return useQuery({
    queryKey: ["stations"],
    queryFn: () => stationsService.list(),
  });
}

export function useStation(id: string) {
  return useQuery({
    queryKey: ["stations", id],
    queryFn: () => stationsService.get(id),
    enabled: !!id,
  });
}

export function useCreateStation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (data: { name: string; description?: string; stream_url?: string; played_limit?: number }) =>
      stationsService.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["stations"] });
    },
  });
}

export function useUpdateStation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({
      id,
      data,
    }: {
      id: string;
      data: {
        name?: string;
        description?: string;
        stream_url?: string;
        prebuffer_bytes?: number;
        played_limit?: number;
        default_fade_ms?: number;
        transition_mode?: "crossfade" | "autocue" | "off";
        autocue_fade_max_ms?: number;
      };
    }) => stationsService.update(id, data),
    onSuccess: (_data, { id }) => {
      queryClient.invalidateQueries({ queryKey: ["stations"] });
      queryClient.invalidateQueries({ queryKey: ["stations", id] });
    },
  });
}

export function useDeleteStation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (id: string) => stationsService.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["stations"] });
    },
  });
}
