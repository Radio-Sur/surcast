import type { ReactNode } from "react";
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useAuth } from "@/hooks/use-auth";
import { WS_RECONNECT_BASE_MS, WS_RECONNECT_MAX_MS } from "@/lib/constants";
import type { LiveListeners, QueueItem, StreamStatus } from "@/types";

export interface LiveStationState {
  status: StreamStatus | null;
  queue: QueueItem[] | null;
  connected: boolean;
  listeners: LiveListeners | null;
}

interface LiveContextValue {
  socketConnected: boolean;
  stations: Record<string, LiveStationState>;
  subscribe: (stationId: string) => void;
  unsubscribe: (stationId: string) => void;
  skip: (stationId: string) => void;
  play: (stationId: string) => void;
  pause: (stationId: string) => void;
}

const LiveContext = createContext<LiveContextValue | null>(null);

const wsUrl = () => `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/api/ws`;

export function LiveProvider({ children }: { children: ReactNode }) {
  const { token } = useAuth();
  const [stations, setStations] = useState<Record<string, LiveStationState>>({});
  const [socketConnected, setSocketConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>();
  const retryCount = useRef(0);
  const subCountsRef = useRef<Record<string, number>>({});

  const send = useCallback((msg: unknown) => {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  }, []);

  const subscribe = useCallback(
    (stationId: string) => {
      const current = subCountsRef.current[stationId] ?? 0;
      subCountsRef.current[stationId] = current + 1;
      if (current === 0) {
        send({ type: "subscribe", station_id: stationId });
      }
    },
    [send],
  );

  const unsubscribe = useCallback(
    (stationId: string) => {
      const current = subCountsRef.current[stationId] ?? 0;
      const next = current - 1;
      if (next > 0) {
        subCountsRef.current[stationId] = next;
        return;
      }
      delete subCountsRef.current[stationId];
      send({ type: "unsubscribe", station_id: stationId });
      setStations((prev) => {
        if (!(stationId in prev)) return prev;
        const nextState = { ...prev };
        delete nextState[stationId];
        return nextState;
      });
    },
    [send],
  );

  const skip = useCallback((stationId: string) => send({ type: "skip", station_id: stationId }), [send]);
  const play = useCallback((stationId: string) => send({ type: "play", station_id: stationId }), [send]);
  const pause = useCallback((stationId: string) => send({ type: "pause", station_id: stationId }), [send]);

  const handleMessage = useCallback(
    (event: MessageEvent) => {
      try {
        const msg: Record<string, unknown> = JSON.parse(event.data as string);
        switch (msg.type) {
          case "auth_ok": {
            setSocketConnected(true);
            retryCount.current = 0;
            for (const stationId of Object.keys(subCountsRef.current)) {
              send({ type: "subscribe", station_id: stationId });
            }
            return;
          }
          case "error": {
            console.warn("Live WS error:", msg.data);
            return;
          }
          case "status": {
            const stationId = msg.station_id as string;
            const inner = msg.data as { type?: string; data?: Partial<StreamStatus> } | null;
            const data = inner?.data;
            if (!stationId || !data) return;
            setStations((prev) => {
              const cur = prev[stationId] ?? { status: null, queue: null, connected: false, listeners: null };
              return {
                ...prev,
                [stationId]: {
                  ...cur,
                  status: {
                    playing: inner.type === "state" ? !!data.playing : true,
                    song_index: data.song_index ?? 0,
                    total: data.total ?? 0,
                    elapsed: data.elapsed ?? 0,
                    title: data.title ?? "",
                    artist: data.artist ?? "",
                    duration: data.duration ?? 0,
                  },
                  connected: true,
                },
              };
            });
            return;
          }
          case "queue_update": {
            const stationId = msg.station_id as string;
            const queue = msg.data as QueueItem[];
            if (!stationId || !Array.isArray(queue)) return;
            setStations((prev) => {
              const cur = prev[stationId] ?? { status: null, queue: null, connected: false, listeners: null };
              return { ...prev, [stationId]: { ...cur, queue, connected: true } };
            });
            return;
          }
          case "listeners": {
            const stationId = msg.station_id as string;
            if (!stationId) return;
            const listeners: LiveListeners = {
              station_id: stationId,
              listeners: (msg.listeners as number) ?? 0,
              updated_at: (msg.updated_at as string) ?? null,
              online: !!msg.online,
            };
            setStations((prev) => {
              const cur = prev[stationId] ?? { status: null, queue: null, connected: false, listeners: null };
              return { ...prev, [stationId]: { ...cur, listeners } };
            });
            return;
          }
          default:
            return;
        }
      } catch {
        console.warn("Live WS message parse error:", event.data);
      }
    },
    [send],
  );

  useEffect(() => {
    if (!token) return;
    if (typeof WebSocket === "undefined") return;

    let closed = false;

    function connect() {
      if (closed) return;
      const ws = new WebSocket(wsUrl());
      wsRef.current = ws;

      ws.onopen = () => {
        retryCount.current = 0;
        ws.send(JSON.stringify({ type: "auth", token }));
      };

      ws.onmessage = handleMessage;

      ws.onclose = () => {
        setSocketConnected(false);
        if (!closed) {
          const delay = Math.min(WS_RECONNECT_BASE_MS * 2 ** retryCount.current, WS_RECONNECT_MAX_MS);
          retryCount.current += 1;
          reconnectTimer.current = setTimeout(connect, delay);
        }
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      closed = true;
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, [token, handleMessage]);

  const value = useMemo(
    () => ({ socketConnected, stations, subscribe, unsubscribe, skip, play, pause }),
    [socketConnected, stations, subscribe, unsubscribe, skip, play, pause],
  );

  return <LiveContext.Provider value={value}>{children}</LiveContext.Provider>;
}

export function useLiveStation(stationId: string | undefined): LiveStationState {
  const ctx = useContext(LiveContext);
  if (!ctx) throw new Error("useLiveStation must be used within a LiveProvider");

  const { subscribe, unsubscribe, stations } = ctx;

  useEffect(() => {
    if (!stationId) return;
    subscribe(stationId);
    return () => unsubscribe(stationId);
  }, [stationId, subscribe, unsubscribe]);

  const station = stationId ? stations[stationId] : undefined;
  return station ?? { status: null, queue: null, connected: false, listeners: null };
}

export function useLiveSocketConnected(): boolean {
  const ctx = useContext(LiveContext);
  if (!ctx) throw new Error("useLiveSocketConnected must be used within a LiveProvider");
  return ctx.socketConnected;
}
