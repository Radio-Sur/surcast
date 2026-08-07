import { useQuery } from "@tanstack/react-query";
import { configService } from "@/lib/services/config";

export function useAppConfig() {
  return useQuery({
    queryKey: ["app-config"],
    queryFn: () => configService.get(),
    staleTime: Infinity,
  });
}
