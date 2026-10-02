import Snackbar from "@mui/material/Snackbar";
import type { ReactNode } from "react";
import { createContext, useCallback, useContext, useState } from "react";
import { ErrorDetails, type ErrorDetailsInfo } from "@/components/error-details";

type Severity = "success" | "error" | "warning" | "info";

interface SnackbarState {
  id: number;
  open: boolean;
  message: string;
  severity: Severity;
  details?: ErrorDetailsInfo | null;
}

export interface ShowErrorOptions {
  title: string;
  details?: ErrorDetailsInfo | null;
  severity?: Severity;
}

interface SnackbarContextValue {
  showSnackbar: (message: string, severity?: Severity) => void;
  showError: (options: ShowErrorOptions) => void;
}

const SnackbarContext = createContext<SnackbarContextValue | null>(null);

export function SnackbarProvider({ children }: { children: ReactNode }) {
  const [snackbar, setSnackbar] = useState<SnackbarState>({ id: 0, open: false, message: "", severity: "info" });

  // The incrementing id remounts the Snackbar per message, so every
  // notification — including success toasts — gets a fresh auto-hide
  // timer and reliably disappears after a few seconds.
  const showSnackbar = useCallback((message: string, severity: Severity = "info") => {
    setSnackbar((prev) => ({ id: prev.id + 1, open: true, message, severity, details: null }));
  }, []);

  const showError = useCallback(({ title, details = null, severity = "error" }: ShowErrorOptions) => {
    setSnackbar((prev) => ({ id: prev.id + 1, open: true, message: title, severity, details }));
  }, []);

  const handleClose = useCallback(() => {
    setSnackbar((prev) => ({ ...prev, open: false, details: null }));
  }, []);

  const hasDetails = snackbar.severity === "error" && snackbar.details != null;

  return (
    <SnackbarContext.Provider value={{ showSnackbar, showError }}>
      {children}
      <Snackbar
        key={snackbar.id}
        open={snackbar.open}
        autoHideDuration={hasDetails ? 8000 : 4000}
        onClose={handleClose}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
      >
        <ErrorDetails
          title={snackbar.message}
          details={snackbar.details}
          severity={snackbar.severity}
          onClose={handleClose}
        />
      </Snackbar>
    </SnackbarContext.Provider>
  );
}

export function useSnackbar() {
  const ctx = useContext(SnackbarContext);
  if (!ctx) throw new Error("useSnackbar must be used within SnackbarProvider");
  return ctx;
}
