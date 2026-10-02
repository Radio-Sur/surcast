import type { DefaultError, MutationKey, MutationOptions } from "@tanstack/react-query";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { isHttpError } from "@/lib/is-http-error";
import { reportHttpError } from "@/lib/report-error";
import { useSnackbar } from "@/providers/snackbar-provider";

export function useMutationWithError<TData = unknown, TError = DefaultError, TVariables = void, TContext = unknown>(
  options: MutationOptions<TData, TError, TVariables, TContext> & {
    errorMessage?: string;
    successMessage?: string;
    invalidateKeys?: MutationKey[];
  },
) {
  const { showSnackbar, showError } = useSnackbar();
  const queryClient = useQueryClient();
  const { errorMessage, successMessage, invalidateKeys, ...mutationOptions } = options;

  return useMutation({
    ...mutationOptions,
    onError(error: TError, variables: TVariables, context: TContext | undefined) {
      const info = isHttpError(error);
      reportHttpError(showError, error, errorMessage || info.message || "Operation failed");
      (
        mutationOptions.onError as
          | ((error: TError, variables: TVariables, context: TContext | undefined) => void)
          | undefined
      )?.(error, variables, context);
    },
    onSuccess(data: TData, variables: TVariables, context: TContext | undefined) {
      if (successMessage) {
        showSnackbar(successMessage, "success");
      }
      if (invalidateKeys) {
        for (const key of invalidateKeys) {
          queryClient.invalidateQueries({ queryKey: key });
        }
      }
      (
        mutationOptions.onSuccess as
          | ((data: TData, variables: TVariables, context: TContext | undefined) => void)
          | undefined
      )?.(data, variables, context);
    },
  } as never);
}
