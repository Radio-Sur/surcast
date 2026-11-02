import { isHttpError } from "@/lib/is-http-error";
import type { ShowErrorOptions } from "@/providers/snackbar-provider";

/**
 * Reports a failure as an expandable error notification: the title is the
 * localized code translation. Caller context and raw server prose are for
 * diagnostics only, never for the notification title.
 */
export function reportHttpError(
  showError: (options: ShowErrorOptions) => void,
  err: unknown,
  fallbackTitle: string,
  severity: ShowErrorOptions["severity"] = "error",
): void {
  // eslint-disable-next-line no-console
  console.error(fallbackTitle, err);
  const info = isHttpError(err);
  showError({
    title: info.message,
    details: {
      message: info.message,
      code: info.code,
      status: info.status,
      category: info.category,
      details: info.details,
      raw: info.details ? undefined : info.raw,
    },
    severity,
  });
}
