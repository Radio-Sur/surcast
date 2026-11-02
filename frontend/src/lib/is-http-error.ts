import axios from "axios";
import { codeFromStatus, ErrorCode, errorMessage } from "@/lib/error-codes";

export type ErrorCategory = "auth" | "validation" | "not_found" | "conflict" | "stream" | "icecast" | "server";

export interface HttpErrorInfo {
  status?: number;
  message: string;
  code?: string;
  category?: ErrorCategory;
  /** Sanitized backend cause (stream pipeline reason, never secrets). */
  details?: string;
  raw?: unknown;
}

function isCategory(value: unknown): value is ErrorCategory {
  return (
    value === "auth" ||
    value === "validation" ||
    value === "not_found" ||
    value === "conflict" ||
    value === "stream" ||
    value === "icecast" ||
    value === "server"
  );
}

function categoryFromStatus(status?: number): ErrorCategory | undefined {
  if (status === 401 || status === 403) return "auth";
  if (status === 400 || status === 422) return "validation";
  if (status === 404) return "not_found";
  if (status === 409) return "conflict";
  if (status !== undefined && status >= 500) return "server";
  return undefined;
}

/** Localized public message. Backend prose is retained only as diagnostics. */
export function serverMessage(err: unknown): string {
  return isHttpError(err).message;
}
export function isHttpError(err: unknown): HttpErrorInfo {
  if (axios.isAxiosError(err)) {
    const data = err.response?.data as
      | { error?: unknown; code?: unknown; category?: unknown; message?: unknown; details?: unknown }
      | undefined;
    const status = err.response?.status;
    const fallback =
      err.code === "ERR_CANCELED"
        ? ErrorCode.REQUEST_CANCELLED
        : err.code === "ECONNABORTED" || err.code === "ETIMEDOUT"
          ? ErrorCode.REQUEST_TIMEOUT
          : !err.response
            ? ErrorCode.NETWORK_ERROR
            : codeFromStatus(status);
    const code = typeof data?.code === "string" ? data.code : fallback;
    const dataCategory = isCategory(data?.category) ? data.category : undefined;
    const details = typeof data?.details === "string" && data.details ? data.details : undefined;
    return {
      status,
      message: errorMessage(code, fallback),
      code,
      category: dataCategory ?? categoryFromStatus(err.response?.status),
      details,
      raw: data ?? err.message,
    };
  }
  if (err && typeof err === "object" && "code" in err) {
    const code = typeof err.code === "string" ? err.code : ErrorCode.UNKNOWN;
    return { code, message: errorMessage(code), raw: err };
  }
  const code = err instanceof Error && err.name === "AbortError" ? ErrorCode.REQUEST_CANCELLED : ErrorCode.UNKNOWN;
  return { code, message: errorMessage(code), raw: err instanceof Error ? err.message : err };
}
