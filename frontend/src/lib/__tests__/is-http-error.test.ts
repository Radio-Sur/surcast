import { describe, expect, it } from "vitest";
import { isHttpError, serverMessage } from "@/lib/is-http-error";

describe("isHttpError", () => {
  it("parses the new flat envelope", () => {
    // axios.isAxiosError checks a flag; simulate via real shape is complex,
    // so assert the fallback paths and the pure helpers here.
    expect(isHttpError(new Error("boom"))).toEqual({ message: "boom", raw: "boom" });
    expect(isHttpError("plain")).toEqual({ message: "plain", raw: "plain" });
    expect(isHttpError(undefined).message).toBe("An unexpected error occurred");
  });

  it("extracts server message only when present", () => {
    expect(serverMessage("backend says no")).toBe("backend says no");
    expect(serverMessage(new Error("x"))).toBeUndefined();
    expect(serverMessage(undefined)).toBeUndefined();
  });
});

describe("isHttpError via axios errors", () => {
  it("reads code and category from the envelope", async () => {
    const axios = await import("axios");
    const err = new axios.AxiosError("Request failed with status code 409", "ERR_BAD_REQUEST", undefined, undefined, {
      status: 409,
      statusText: "Conflict",
      headers: {},
      config: {} as never,
      data: { error: "Mount taken", code: "CONFLICT", category: "conflict" },
    });
    const info = isHttpError(err);
    expect(info).toMatchObject({ status: 409, message: "Mount taken", code: "CONFLICT", category: "conflict" });
    expect(serverMessage(err)).toBe("Mount taken");
  });

  it("falls back to category from status without a code", async () => {
    const axios = await import("axios");
    const err = new axios.AxiosError("Request failed with status code 500", "ERR_BAD_RESPONSE", undefined, undefined, {
      status: 500,
      statusText: "Error",
      headers: {},
      config: {} as never,
      data: { error: "Internal server error" },
    });
    const info = isHttpError(err);
    expect(info).toMatchObject({ status: 500, message: "Internal server error", category: "server" });
    expect(info.code).toBeUndefined();
  });
});
