import WifiOff from "@mui/icons-material/WifiOff";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import type { ReactNode } from "react";
import { createContext, useCallback, useContext, useEffect, useState } from "react";

interface OnlineStatusContextValue {
  online: boolean;
}

const OnlineStatusContext = createContext<OnlineStatusContextValue>({ online: true });

export function OnlineStatusProvider({ children }: { children: ReactNode }) {
  const [online, setOnline] = useState(() => navigator.onLine);

  const handleOnline = useCallback(() => setOnline(true), []);
  const handleOffline = useCallback(() => setOnline(false), []);

  useEffect(() => {
    window.addEventListener("online", handleOnline);
    window.addEventListener("offline", handleOffline);
    return () => {
      window.removeEventListener("online", handleOnline);
      window.removeEventListener("offline", handleOffline);
    };
  }, [handleOnline, handleOffline]);

  return (
    <OnlineStatusContext.Provider value={{ online }}>
      {!online && (
        <Box sx={{ position: "fixed", top: 0, left: 0, right: 0, zIndex: 9999 }}>
          <Alert
            severity="warning"
            icon={<WifiOff fontSize="small" />}
            sx={{ borderRadius: 0, justifyContent: "center" }}
          >
            You are offline. Changes will not be saved until connection is restored.
          </Alert>
        </Box>
      )}
      {children}
    </OnlineStatusContext.Provider>
  );
}

export function useOnlineStatus() {
  return useContext(OnlineStatusContext);
}
