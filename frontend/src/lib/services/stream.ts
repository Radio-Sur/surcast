import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { StreamStatus } from "@/types";

export interface ConfirmedStreamStatus extends StreamStatus {
  broadcasting: boolean;
  source_connected: boolean;
  is_started: boolean;
  runtime_active: boolean;
}

export function createStreamService(client: HttpClient, { timeoutMs = 30_000, pollIntervalMs = 300 } = {}) {
  async function transition(stationId: string, action: "play" | "stop" | "restart") {
    await client.post(`/stations/${stationId}/stream/${action}`);
    const deadline = Date.now() + timeoutMs;
    let lastError: unknown;
    while (Date.now() < deadline) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const status = await Promise.race([
          client.get<ConfirmedStreamStatus>(`/stations/${stationId}/stream/status?verify_output=true`),
          new Promise<never>((_, reject) => {
            timer = setTimeout(
              () => reject(new Error("Output verification timed out")),
              Math.min(4000, deadline - Date.now()),
            );
          }),
        ]);
        const complete =
          action === "stop"
            ? status.playing === false &&
              status.is_started === false &&
              status.runtime_active === false &&
              status.source_connected === false &&
              status.broadcasting === false
            : status.playing === true &&
              status.is_started === true &&
              status.runtime_active === true &&
              status.source_connected === true &&
              status.broadcasting === true;
        if (complete) return status;
      } catch (error) {
        // Temporary Icecast/API failures are not proof of either state.
        lastError = error;
      } finally {
        clearTimeout(timer);
      }
      const remaining = deadline - Date.now();
      if (remaining > 0) await new Promise((resolve) => setTimeout(resolve, Math.min(pollIntervalMs, remaining)));
    }
    throw new Error(
      `Station ${action === "stop" ? "shutdown" : "startup"} could not be confirmed in Icecast. Check the station status before retrying.`,
      { cause: lastError },
    );
  }

  return {
    status: (stationId: string) => client.get<StreamStatus>(`/stations/${stationId}/stream/status`),
    skip: (stationId: string) => client.post(`/stations/${stationId}/stream/skip`),
    play: (stationId: string) => transition(stationId, "play"),
    pause: (stationId: string) => client.post(`/stations/${stationId}/stream/pause`),
    stop: (stationId: string) => transition(stationId, "stop"),
    restart: (stationId: string) => transition(stationId, "restart"),
  };
}

export const streamService = createStreamService(httpClient);
