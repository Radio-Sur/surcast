import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { useState } from "react";
import { I18nextProvider } from "react-i18next";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import i18n from "@/i18n";
import { server } from "@/mocks/server";
import { AuthProvider } from "@/providers/auth-provider";
import { LiveProvider, useLiveSocketConnected, useLiveStation } from "@/providers/live-provider";
import { ThemeProvider } from "@/providers/theme-provider";
import { setupAuth } from "@/test/test-utils";

function createWrapper() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <I18nextProvider i18n={i18n}>
          <ThemeProvider>
            <MemoryRouter>
              <AuthProvider>
                <LiveProvider>{children}</LiveProvider>
              </AuthProvider>
            </MemoryRouter>
          </ThemeProvider>
        </I18nextProvider>
      </QueryClientProvider>
    );
  };
}

function getWS(): MockWebSocket {
  const MockWS = globalThis.WebSocket as unknown as typeof MockWebSocket;
  return MockWS.instances[0];
}

class MockWebSocket {
  static instances: MockWebSocket[] = [];
  static OPEN = 1;
  static CONNECTING = 0;
  static CLOSING = 2;
  static CLOSED = 3;
  onopen: (() => void) | null = null;
  onclose: ((e: { code: number }) => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readyState = 1;
  closeCalled = false;
  sentCommands: string[] = [];

  constructor(public url: string) {
    MockWebSocket.instances.push(this);
    Promise.resolve().then(() => this.onopen?.());
  }

  send(data: string) {
    this.sentCommands.push(data);
    const parsed = JSON.parse(data);
    if (parsed.type === "auth") {
      Promise.resolve().then(() => this.onmessage?.({ data: JSON.stringify({ type: "auth_ok" }) }));
    }
  }

  close() {
    this.closeCalled = true;
    this.readyState = 3;
  }

  static clear() {
    MockWebSocket.instances = [];
  }
}

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
  MockWebSocket.clear();
  vi.stubGlobal("WebSocket", MockWebSocket);
});

describe("LiveProvider", () => {
  it("connects and subscribes to a station", async () => {
    const { result } = renderHook(() => useLiveStation("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.connected).toBe(false));
    const ws = getWS();
    expect(ws).toBeDefined();
    await waitFor(() => {
      expect(ws.sentCommands.some((c) => JSON.parse(c).type === "subscribe")).toBe(true);
    });
  });

  it("updates status and queue from station messages", async () => {
    const { result } = renderHook(() => useLiveStation("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.connected).toBe(false));
    const ws = getWS();

    ws.onmessage?.({
      data: JSON.stringify({
        type: "status",
        station_id: "s1",
        data: {
          type: "state",
          data: {
            playing: true,
            song_index: 0,
            total: 10,
            elapsed: 30,
            title: "Test Song",
            artist: "Test Artist",
            duration: 200,
          },
        },
      }),
    });
    await waitFor(() => expect(result.current.status?.title).toBe("Test Song"));
    expect(result.current.connected).toBe(true);

    ws.onmessage?.({
      data: JSON.stringify({
        type: "queue_update",
        station_id: "s1",
        data: [
          {
            id: "q1",
            song_id: "s1",
            title: "Q Song",
            station_id: "s1",
            position: 0,
            artist: "A",
            album: "B",
            duration: 200,
            has_cover: false,
            mime_type: "audio/mpeg",
            origin_playlist_id: null,
            playlist_name: null,
            is_auto_dj: false,
          },
        ],
      }),
    });
    await waitFor(() => expect(result.current.queue).toHaveLength(1));
  });

  it("updates listeners from listener messages", async () => {
    const { result } = renderHook(() => useLiveStation("s1"), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.connected).toBe(false));
    const ws = getWS();

    ws.onmessage?.({
      data: JSON.stringify({
        type: "listeners",
        station_id: "s1",
        listeners: 42,
        updated_at: "2026-01-01T00:00:00Z",
        online: true,
      }),
    });
    await waitFor(() => expect(result.current.listeners?.listeners).toBe(42));
  });

  it("unsubscribes when a consumer stops using a station", async () => {
    let setVisible: (v: boolean) => void;
    function Harness() {
      const [visible, set] = useState(true);
      setVisible = set;
      return visible ? <Consumer /> : null;
    }
    function Consumer() {
      useLiveStation("s1");
      return null;
    }

    const view = render(<Harness />, { wrapper: createWrapper() });
    const ws = getWS();
    await waitFor(() => expect(ws.sentCommands.some((c) => JSON.parse(c).type === "subscribe")).toBe(true));
    setVisible!(false);
    await waitFor(() => expect(ws.sentCommands.some((c) => JSON.parse(c).type === "unsubscribe")).toBe(true));
    view.unmount();
  });

  it("keeps queue state when only one of two subscribers unmounts", async () => {
    let qLen: number | null = null;
    let setExtraVisible: (v: boolean) => void;
    function Primary() {
      const { queue } = useLiveStation("s1");
      qLen = queue ? queue.length : null;
      return null;
    }
    function Secondary() {
      useLiveStation("s1");
      return null;
    }
    function Harness() {
      const [extra, setExtra] = useState(true);
      setExtraVisible = setExtra;
      return (
        <>
          <Primary />
          {extra ? <Secondary /> : null}
        </>
      );
    }

    render(<Harness />, { wrapper: createWrapper() });
    const ws = getWS();
    await waitFor(() => expect(ws.sentCommands.some((c) => JSON.parse(c).type === "subscribe")).toBe(true));

    ws.onmessage?.({
      data: JSON.stringify({
        type: "queue_update",
        station_id: "s1",
        data: [
          {
            id: "q1",
            song_id: "s1",
            station_id: "s1",
            position: 0,
            title: "T",
            artist: "A",
            album: "B",
            duration: 200,
            has_cover: false,
            mime_type: "audio/mpeg",
            origin_playlist_id: null,
            playlist_name: null,
            is_auto_dj: false,
          },
        ],
      }),
    });
    await waitFor(() => expect(qLen).toBe(1));

    setExtraVisible!(false);
    await new Promise((r) => setTimeout(r, 50));

    expect(ws.sentCommands.some((c) => JSON.parse(c).type === "unsubscribe")).toBe(false);
    expect(qLen).toBe(1);
  });

  it("useLiveSocketConnected reports socket state", async () => {
    const { result } = renderHook(() => useLiveSocketConnected(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current).toBe(true));
  });

  it("returns defaults when stationId is undefined", () => {
    const { result } = renderHook(() => useLiveStation(undefined), { wrapper: createWrapper() });
    expect(result.current.connected).toBe(false);
    expect(result.current.status).toBeNull();
    expect(result.current.queue).toBeNull();
  });
});
