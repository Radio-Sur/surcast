import { useMutation } from "@tanstack/react-query";
import { streamService } from "@/lib/services/stream";

export function useStreamSkip(stationId: string) {
  return useMutation({
    mutationFn: () => streamService.skip(stationId),
  });
}

export function useStreamPlay(stationId: string) {
  return useMutation({
    mutationFn: () => streamService.play(stationId),
  });
}

export function useStreamPause(stationId: string) {
  return useMutation({
    mutationFn: () => streamService.pause(stationId),
  });
}

export function useStreamStop(stationId: string) {
  return useMutation({
    mutationFn: () => streamService.stop(stationId),
  });
}

export function useStreamRestart(stationId: string) {
  return useMutation({
    mutationFn: () => streamService.restart(stationId),
  });
}
