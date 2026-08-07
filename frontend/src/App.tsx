import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { ErrorBoundary } from "@/components/error-boundary";
import { AppLayout } from "@/components/layout/app-layout";
import { AdminIcecastPage } from "@/pages/admin/icecast";
import { ApiKeysPage } from "@/pages/api-keys";
import { DashboardPage } from "@/pages/dashboard";
import { LoginPage } from "@/pages/login";
import { PlaylistDetailPage } from "@/pages/playlists/detail";
import { PlaylistsListPage } from "@/pages/playlists/list";
import { SetupPage } from "@/pages/setup";
import { SongsPage } from "@/pages/songs";
import { StationDetailPage } from "@/pages/stations/detail";
import { EditStationPage } from "@/pages/stations/edit";
import { StationsListPage } from "@/pages/stations/list";
import { UsersPage } from "@/pages/users";
import { AuthProvider } from "@/providers/auth-provider";
import { LiveProvider } from "@/providers/live-provider";
import { OnlineStatusProvider } from "@/providers/online-status-provider";
import { SnackbarProvider } from "@/providers/snackbar-provider";
import { ThemeProvider } from "@/providers/theme-provider";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 30_000,
      refetchOnWindowFocus: false,
      gcTime: 300_000,
    },
    mutations: {
      onError(error) {
        console.error("Unhandled mutation error:", error);
      },
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <ThemeProvider>
          <AuthProvider>
            <ErrorBoundary>
              <SnackbarProvider>
                <OnlineStatusProvider>
                  <LiveProvider>
                    <Routes>
                      <Route path="/login" element={<LoginPage />} />
                      <Route path="/setup" element={<SetupPage />} />
                      <Route element={<AppLayout />}>
                        <Route path="/" element={<DashboardPage />} />
                        <Route path="/stations" element={<StationsListPage />} />
                        <Route path="/stations/:id" element={<StationDetailPage />} />
                        <Route path="/stations/:id/edit" element={<EditStationPage />} />
                        <Route path="/songs" element={<SongsPage />} />
                        <Route path="/playlists" element={<PlaylistsListPage />} />
                        <Route path="/playlists/:id" element={<PlaylistDetailPage />} />
                        <Route path="/api-keys" element={<ApiKeysPage />} />
                        <Route path="/users" element={<UsersPage />} />
                        <Route path="/admin/icecast" element={<AdminIcecastPage />} />
                      </Route>
                      <Route path="*" element={<Navigate to="/" replace />} />
                    </Routes>
                  </LiveProvider>
                </OnlineStatusProvider>
              </SnackbarProvider>
            </ErrorBoundary>
          </AuthProvider>
        </ThemeProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
