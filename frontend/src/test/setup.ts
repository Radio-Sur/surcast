import "@testing-library/jest-dom/vitest";
import { afterAll, afterEach, beforeAll, vi } from "vitest";
import { resetAuthState } from "@/mocks/handlers/auth";
import { resetIcecastState } from "@/mocks/handlers/icecast";
import { server } from "@/mocks/server";

Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  }),
});

Object.defineProperty(window, "localStorage", {
  value: (() => {
    let store: Record<string, string> = {};
    return {
      getItem: (key: string) => store[key] ?? null,
      setItem: (key: string, value: string) => {
        store[key] = value;
      },
      removeItem: (key: string) => {
        delete store[key];
      },
      clear: () => {
        store = {};
      },
      get length() {
        return Object.keys(store).length;
      },
      key: (i: number) => Object.keys(store)[i] ?? null,
    };
  })(),
});

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
    this.readyState = 3;
  }
}

vi.stubGlobal("WebSocket", MockWebSocket);

beforeAll(() => server.listen({ onUnhandledRequest: "warn" }));
afterEach(() => {
  server.resetHandlers();
  resetAuthState();
  resetIcecastState();
});
afterAll(() => server.close());
