import axios from "axios";

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

/**
 * The backend-provided message, if any (the `error` string of the
 * `{error, code, category}` envelope). Undefined for network failures and
 * non-HTTP errors — callers fall back to a contextual title then.
 */
export function serverMessage(err: unknown): string | undefined {
  if (axios.isAxiosError(err)) {
    const data = err.response?.data as { error?: unknown } | undefined;
    const rawError = data?.error;
    if (typeof rawError === "string" && rawError) return rawError;
  }
  if (typeof err === "string" && err) return err;
  return undefined;
}
export function isHttpError(err: unknown): HttpErrorInfo {
  if (axios.isAxiosError(err)) {
    const data = err.response?.data as
      | { error?: unknown; code?: unknown; category?: unknown; message?: unknown; details?: unknown }
      | undefined;
    const rawError = data?.error;
    const message =
      (typeof rawError === "string" && rawError) || (typeof data?.message === "string" && data.message) || err.message;
    const code = typeof data?.code === "string" ? data.code : undefined;
    const dataCategory = isCategory(data?.category) ? data.category : undefined;
    const details = typeof data?.details === "string" && data.details ? data.details : undefined;
    return {
      status: err.response?.status,
      message,
      code,
      category: dataCategory ?? categoryFromStatus(err.response?.status),
      details,
      raw: data ?? err.message,
    };
  }
  if (err instanceof Error) return { message: err.message, raw: err.message };
  if (typeof err === "string") return { message: err, raw: err };
  return { message: "An unexpected error occurred", raw: err };
}
