import { useQuery } from "@tanstack/react-query";
import { listenersService } from "@/lib/services/listeners";
import type { ListenerRange } from "@/types";

export function useStationListenersHistory(stationId: string, range: ListenerRange) {
  return useQuery({
    queryKey: ["station-listeners-history", stationId, range],
    queryFn: () => listenersService.stationHistory(stationId, range),
    enabled: !!stationId,
    refetchInterval: 30_000,
  });
}

export function useListenersOverview(range: ListenerRange) {
  return useQuery({
    queryKey: ["listeners-overview", range],
    queryFn: () => listenersService.overview(range),
    refetchInterval: 30_000,
  });
}
