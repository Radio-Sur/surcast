import axios from "axios";

export interface HttpErrorInfo {
  status?: number;
  message: string;
}

export function isHttpError(err: unknown): HttpErrorInfo {
  if (axios.isAxiosError(err)) {
    return {
      status: err.response?.status,
      message: err.response?.data?.error || err.message,
    };
  }
  if (err instanceof Error) return { message: err.message };
  if (typeof err === "string") return { message: err };
  return { message: "An unexpected error occurred" };
}
