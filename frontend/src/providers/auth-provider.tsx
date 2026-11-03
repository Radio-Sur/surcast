import { useQuery, useQueryClient } from "@tanstack/react-query";
import { createContext, type ReactNode, useEffect, useState, useSyncExternalStore } from "react";
import { httpClient } from "@/lib/api";
import { STORAGE_KEYS } from "@/lib/constants";
import { localTokenStorage, subscribeToken } from "@/lib/token-storage";
import type { AuthResponse, User } from "@/types";

interface AuthContextType {
  user: User | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  setupComplete: boolean | null;
  token: string | null;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
  refreshSetupStatus: () => Promise<void>;
}

export const AuthContext = createContext<AuthContextType | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const token = useSyncExternalStore(subscribeToken, localTokenStorage.getAccessToken);
  const [loginUser, setLoginUser] = useState<User | null>(null);

  useEffect(() => {
    const changedInAnotherTab = (event: StorageEvent) => {
      if (event.key === STORAGE_KEYS.ACCESS_TOKEN || event.key === null) {
        setLoginUser(null);
        void queryClient.resetQueries({ predicate: (query) => query.queryKey[0] !== "setup" });
      }
    };
    window.addEventListener("storage", changedInAnotherTab);
    return () => window.removeEventListener("storage", changedInAnotherTab);
  }, [queryClient]);

  const setupQuery = useQuery({
    queryKey: ["setup", "status"],
    queryFn: () => httpClient.get<{ setup_complete: boolean }>("/setup/status"),
    staleTime: Infinity,
    retry: false,
  });

  const meQuery = useQuery({
    queryKey: ["auth", "me"],
    queryFn: () => httpClient.get<User>("/auth/me"),
    enabled: !!token,
    staleTime: 30_000,
    retry: false,
  });

  const setupComplete = setupQuery.isSuccess ? setupQuery.data.setup_complete : setupQuery.isError ? false : null;

  const isInitializing = setupQuery.isPending || (!!token && meQuery.isLoading);

  const user = token ? (loginUser ?? meQuery.data ?? null) : null;

  useEffect(() => {
    if (meQuery.isError) {
      console.warn("Failed to fetch current user, clearing session");
      localTokenStorage.clear();
    }
  }, [meQuery.isError]);

  useEffect(() => {
    if (!token) {
      queryClient.removeQueries({ predicate: (query) => query.queryKey[0] !== "setup" });
      setLoginUser(null);
    }
  }, [token, queryClient]);

  return (
    <AuthContext.Provider
      value={{
        user,
        isLoading: isInitializing,
        isAuthenticated: !!user,
        setupComplete,
        token,
        login: async (username: string, password: string) => {
          const authResponse = await httpClient.post<AuthResponse>("/auth/login", {
            username,
            password,
          });
          queryClient.removeQueries({ predicate: (query) => query.queryKey[0] !== "setup" });
          localTokenStorage.setRefreshToken(authResponse.refresh_token);
          localTokenStorage.setAccessToken(authResponse.access_token);
          setLoginUser(authResponse.user);
          queryClient.setQueryData(["auth", "me"], authResponse.user);
        },
        logout: () => {
          localTokenStorage.clear();
          queryClient.removeQueries({ predicate: (query) => query.queryKey[0] !== "setup" });
          setLoginUser(null);
        },
        refreshSetupStatus: () => queryClient.invalidateQueries({ queryKey: ["setup", "status"] }),
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}
