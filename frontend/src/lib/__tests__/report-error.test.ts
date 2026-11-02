import { AxiosError } from "axios";
import { afterEach, expect, it, vi } from "vitest";
import i18n from "@/i18n";
import { reportHttpError } from "@/lib/report-error";

afterEach(async () => {
  await i18n.changeLanguage("en");
  vi.restoreAllMocks();
});

it.each(["en", "pl"])("reports localized codes, never raw server text or caller fallback (%s)", async (language) => {
  await i18n.changeLanguage(language);
  vi.spyOn(console, "error").mockImplementation(() => {});
  const showError = vi.fn();
  const error = new AxiosError("Raw transport error", "ERR_BAD_REQUEST", undefined, undefined, {
    status: 403,
    statusText: "Forbidden",
    headers: {},
    config: {} as never,
    data: { code: "DEMO_STATION_READONLY", error: "Raw English server error", category: "auth" },
  });
  reportHttpError(showError, error, "English caller fallback");
  expect(showError).toHaveBeenCalledWith(
    expect.objectContaining({ title: i18n.t("errors:codes.DEMO_STATION_READONLY") }),
  );
  expect(showError.mock.calls[0][0].title).not.toMatch(/Raw|fallback/);
});
