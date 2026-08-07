import { useQuery, useQueryClient } from "@tanstack/react-query";
import { createContext, type ReactNode, useEffect, useState } from "react";
import { httpClient } from "@/lib/api";
import { localTokenStorage } from "@/lib/token-storage";
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
  const [token, setToken] = useState<string | null>(() => localTokenStorage.getAccessToken());
  const [loginUser, setLoginUser] = useState<User | null>(null);

  const setupQuery = useQuery({
    queryKey: ["setup", "status"],
    queryFn: () => httpClient.get<{ setup_complete: boolean }>("/setup/status"),
    staleTime: Infinity,
    retry: false,
  });

  const [storedToken] = useState(() => localTokenStorage.getAccessToken());

  const meQuery = useQuery({
    queryKey: ["auth", "me"],
    queryFn: () => httpClient.get<User>("/auth/me"),
    enabled: !!storedToken,
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
      setToken(null);
    }
  }, [meQuery.isError]);

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
          localTokenStorage.setAccessToken(authResponse.access_token);
          localTokenStorage.setRefreshToken(authResponse.refresh_token);
          setToken(authResponse.access_token);
          setLoginUser(authResponse.user);
        },
        logout: () => {
          localTokenStorage.clear();
          setToken(null);
          setLoginUser(null);
        },
        refreshSetupStatus: () => queryClient.invalidateQueries({ queryKey: ["setup", "status"] }),
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}
