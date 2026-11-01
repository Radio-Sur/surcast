import { afterEach, describe, expect, it, vi } from "vitest";
import type { HttpClient } from "@/lib/http-client";
import { createStreamService } from "@/lib/services/stream";

const live = { playing: true, broadcasting: true, source_connected: true, is_started: true, runtime_active: true };
const stopped = {
  playing: false,
  broadcasting: false,
  source_connected: false,
  is_started: false,
  runtime_active: false,
};

function fixture() {
  const get = vi.fn().mockResolvedValue(live);
  const post = vi.fn().mockResolvedValue({ ok: true });
  const service = createStreamService({ get, post } as unknown as HttpClient, { timeoutMs: 1000, pollIntervalMs: 100 });
  return { service, get, post };
}

afterEach(() => vi.useRealTimers());

describe("stream confirmation", () => {
  it("waits for received audio, not just Playing or a connected source", async () => {
    vi.useFakeTimers();
    const { service, get, post } = fixture();
    get.mockResolvedValueOnce({ ...live, broadcasting: false });
    const success = vi.fn();
    const result = service.play("s1").then(success);
    await vi.advanceTimersByTimeAsync(99);
    expect(post).toHaveBeenCalledExactlyOnceWith("/stations/s1/stream/play");
    expect(success).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await result;
    expect(success).toHaveBeenCalledWith(live);
    expect(get).toHaveBeenCalledWith("/stations/s1/stream/status?verify_output=true");
  });

  it("waits for source removal and terminal runtime shutdown", async () => {
    vi.useFakeTimers();
    const { service, get } = fixture();
    get
      .mockResolvedValue(stopped)
      .mockResolvedValueOnce({ ...stopped, source_connected: true })
      .mockResolvedValueOnce({ ...stopped, runtime_active: true })
      .mockResolvedValueOnce({ ...stopped, is_started: true });
    const success = vi.fn();
    const result = service.stop("s1").then(success);
    await vi.advanceTimersByTimeAsync(299);
    expect(success).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await result;
    expect(success).toHaveBeenCalledWith(stopped);
  });

  it("retries verification errors without treating them as stopped", async () => {
    vi.useFakeTimers();
    const { service, get } = fixture();
    get.mockRejectedValueOnce(new Error("Icecast unavailable")).mockResolvedValue(stopped);
    const success = vi.fn();
    const result = service.stop("s1").then(success);
    await vi.advanceTimersByTimeAsync(99);
    expect(success).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await result;
    expect(success).toHaveBeenCalledOnce();
  });

  it.each(["play", "stop", "restart"] as const)(
    "fails %s without success if output never converges",
    async (action) => {
      vi.useFakeTimers();
      const { service, get } = fixture();
      get.mockResolvedValue(action === "stop" ? live : stopped);
      const result = service[action]("s1");
      const assertion = expect(result).rejects.toThrow("could not be confirmed");
      await vi.advanceTimersByTimeAsync(1000);
      await assertion;
    },
  );

  it("bounds even an unresponsive verification request", async () => {
    vi.useFakeTimers();
    const { service, get } = fixture();
    get.mockReturnValue(new Promise(() => {}));
    const assertion = expect(service.play("s1")).rejects.toThrow("could not be confirmed");
    await vi.advanceTimersByTimeAsync(1000);
    await assertion;
  });

  it.each(["play", "stop", "restart"] as const)("does not poll or report success when %s fails", async (action) => {
    const { service, get, post } = fixture();
    post.mockRejectedValue(new Error("Command failed"));
    await expect(service[action]("s1")).rejects.toThrow("Command failed");
    expect(get).not.toHaveBeenCalled();
  });
});
