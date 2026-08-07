import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useElapsedTimer } from "@/hooks/use-elapsed-timer";

function makeStatus(overrides: Partial<import("@/types").StreamStatus> = {}): import("@/types").StreamStatus {
  return {
    playing: false,
    song_index: 0,
    total: 0,
    elapsed: 0,
    title: "",
    artist: "",
    duration: 300,
    ...overrides,
  };
}

describe("useElapsedTimer", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns 0 when no stream status", () => {
    const { result } = renderHook(() => useElapsedTimer(null));
    expect(result.current).toBe(0);
  });

  it("returns 0 when stream status is undefined", () => {
    const { result } = renderHook(() => useElapsedTimer(undefined));
    expect(result.current).toBe(0);
  });

  it("uses elapsed from stream status", () => {
    const { result } = renderHook(() => useElapsedTimer(makeStatus({ elapsed: 42 })));
    expect(result.current).toBe(42);
  });

  it("increments elapsed every second when playing", () => {
    const { result } = renderHook(() => useElapsedTimer(makeStatus({ playing: true, elapsed: 0 })));

    act(() => {
      vi.advanceTimersByTime(3000);
    });

    expect(result.current).toBe(3);
  });

  it("caps elapsed at duration", () => {
    const { result } = renderHook(() => useElapsedTimer(makeStatus({ playing: true, elapsed: 0, duration: 3 })));

    act(() => {
      vi.advanceTimersByTime(5000);
    });

    expect(result.current).toBe(3);
  });

  it("resets when song_index changes", () => {
    const { result, rerender } = renderHook(({ status }) => useElapsedTimer(status), {
      initialProps: { status: makeStatus({ playing: true, elapsed: 0, song_index: 0 }) },
    });

    act(() => {
      vi.advanceTimersByTime(2000);
    });
    expect(result.current).toBe(2);

    rerender({ status: makeStatus({ playing: true, elapsed: 0, song_index: 1 }) });
    expect(result.current).toBe(0);
  });

  it("stops incrementing when not playing", () => {
    const { result, rerender } = renderHook(({ status }) => useElapsedTimer(status), {
      initialProps: { status: makeStatus({ playing: true, elapsed: 0 }) },
    });

    act(() => {
      vi.advanceTimersByTime(3000);
    });
    expect(result.current).toBe(3);

    rerender({ status: makeStatus({ playing: false, elapsed: 3 }) });
    act(() => {
      vi.advanceTimersByTime(5000);
    });
    expect(result.current).toBe(3);
  });
});
