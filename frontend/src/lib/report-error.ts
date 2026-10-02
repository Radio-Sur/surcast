import axios from "axios";
import { isHttpError } from "@/lib/is-http-error";
import type { ShowErrorOptions } from "@/providers/snackbar-provider";

/**
 * Reports a failure as an expandable error notification: the title is the
 * server message when the backend provided one, otherwise the caller
 * fallback; the (i) details carry the status code, backend code, category
 * and raw payload.
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
  const responseError = axios.isAxiosError(err)
    ? (err.response?.data as { error?: unknown } | undefined)?.error
    : undefined;
  const serverMessage =
    (typeof responseError === "string" && responseError) || (typeof err === "string" && err) || undefined;
  showError({
    title: serverMessage || fallbackTitle,
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
