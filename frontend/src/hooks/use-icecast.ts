import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { icecastService } from "@/lib/services/icecast";

export interface IcecastSettings {
  id: string;
  enabled: boolean;
  mode: string;
  port: number;
  source_password: string;
  admin_user: string;
  admin_password: string;
  external_url: string | null;
  external_source_pw: string | null;
  external_admin_pw: string | null;
}

export interface IcecastStatusResponse {
  settings: IcecastSettings;
  running: boolean;
}

export interface IcecastSettingsUpdate {
  enabled?: boolean;
  mode?: string;
  port?: number;
  source_password?: string;
  admin_user?: string;
  admin_password?: string;
  external_url?: string | null;
  external_source_pw?: string | null;
  external_admin_pw?: string | null;
}

export function useIcecastStatus() {
  return useQuery<IcecastStatusResponse>({
    queryKey: ["icecast"],
    queryFn: () => icecastService.status(),
  });
}

export function useUpdateIcecast() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (update: IcecastSettingsUpdate) => icecastService.update(update),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["icecast"] }),
  });
}

export function useStartIcecast() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => icecastService.start(),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["icecast"] }),
  });
}

export function useStopIcecast() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => icecastService.stop(),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["icecast"] }),
  });
}

export function useTestIcecast() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => icecastService.test(),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["icecast"] }),
  });
}
