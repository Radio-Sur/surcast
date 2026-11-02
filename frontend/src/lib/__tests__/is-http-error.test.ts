import { AxiosError } from "axios";
import { afterEach, describe, expect, it } from "vitest";
import i18n from "@/i18n";
import en from "@/i18n/en/errors.json";
import pl from "@/i18n/pl/errors.json";
import { ErrorCode, errorMessage } from "@/lib/error-codes";
import { isHttpError, serverMessage } from "@/lib/is-http-error";

const httpError = (status: number, data: unknown) =>
  new AxiosError("Transport prose must not be shown", "ERR_BAD_RESPONSE", undefined, undefined, {
    status,
    statusText: "Error",
    headers: {},
    config: {} as never,
    data,
  });
afterEach(async () => {
  await i18n.changeLanguage("en");
});

describe("localized error contract", () => {
  it("has matching Polish and English translations for every supported code", () => {
    expect(Object.keys(en.codes).sort()).toEqual(Object.values(ErrorCode).sort());
    expect(Object.keys(pl.codes).sort()).toEqual(Object.values(ErrorCode).sort());
  });

  it.each(["en", "pl"])("translates API codes in %s without using backend prose", async (language) => {
    await i18n.changeLanguage(language);
    const error = httpError(403, { code: "DEMO_RESTRICTED", error: "UNTRANSLATED SERVER MESSAGE", category: "auth" });
    expect(isHttpError(error)).toMatchObject({
      code: "DEMO_RESTRICTED",
      category: "auth",
      status: 403,
      message: language === "pl" ? pl.codes.DEMO_RESTRICTED : en.codes.DEMO_RESTRICTED,
    });
    expect(serverMessage(error)).not.toContain("UNTRANSLATED");
    for (const code of Object.values(ErrorCode)) {
      expect(errorMessage(code)).toBe((language === "pl" ? pl : en).codes[code]);
    }
  });

  it("localizes legacy and unknown envelopes by HTTP status", async () => {
    await i18n.changeLanguage("pl");
    for (const data of [
      { error: "English only" },
      { code: "FUTURE_ERROR", error: "English only" },
      "Plain proxy error",
    ]) {
      expect(isHttpError(httpError(400, data)).message).toBe(pl.codes.BAD_REQUEST);
    }
    expect(isHttpError(httpError(500, { error: "Secret diagnostic" })).message).toBe(pl.codes.INTERNAL);
  });

  it("keeps raw errors as diagnostics only", () => {
    for (const error of [new Error("boom"), "plain", undefined]) {
      expect(isHttpError(error).message).toBe(en.codes.UNKNOWN);
      expect(serverMessage(error)).toBe(en.codes.UNKNOWN);
    }
    expect(isHttpError(new Error("boom")).raw).toBe("boom");
  });

  it("handles network errors, timeouts and structured upload/WS errors", () => {
    expect(isHttpError(new AxiosError("raw", "ERR_NETWORK")).message).toBe(en.codes.NETWORK_ERROR);
    expect(isHttpError(new AxiosError("raw", "ECONNABORTED")).code).toBe(ErrorCode.REQUEST_TIMEOUT);
    expect(isHttpError({ code: "UPLOAD_FAILED", error: "raw" }).message).toBe(en.codes.UPLOAD_FAILED);
    expect(isHttpError({ code: "AUTH_INVALID_CREDENTIALS" }).message).toBe(en.codes.AUTH_INVALID_CREDENTIALS);
  });
});
