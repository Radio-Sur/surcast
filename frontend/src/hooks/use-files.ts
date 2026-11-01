import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { filesService } from "@/lib/services/files";
import type { DirectoryListing, FileSortKey, SortOrder } from "@/types";

function useInvalidateLibrary() {
  const queryClient = useQueryClient();
  return () => {
    queryClient.invalidateQueries({ queryKey: ["files"] });
    queryClient.invalidateQueries({ queryKey: ["songs"] });
  };
}

export function useDirectory(path: string, sort: FileSortKey, order: SortOrder) {
  return useQuery<DirectoryListing>({
    queryKey: ["files", "list", path, sort, order],
    queryFn: () => filesService.list({ path, sort, order }),
    placeholderData: (prev) => prev,
  });
}

export function useFolders() {
  return useQuery<string[]>({
    queryKey: ["files", "folders"],
    queryFn: () => filesService.listFolders(),
  });
}

export function useCreateFolder() {
  const invalidate = useInvalidateLibrary();
  return useMutation({
    mutationFn: (path: string) => filesService.createFolder(path),
    onSuccess: invalidate,
  });
}

export function useDeleteFolder() {
  const invalidate = useInvalidateLibrary();
  return useMutation({
    mutationFn: (path: string) => filesService.deleteFolder(path),
    onSuccess: invalidate,
  });
}

export function useDeleteFile() {
  const invalidate = useInvalidateLibrary();
  return useMutation({
    mutationFn: (path: string) => filesService.deleteFile(path),
    onSuccess: invalidate,
  });
}

export function useMoveEntry() {
  const invalidate = useInvalidateLibrary();
  return useMutation({
    mutationFn: ({ from, to }: { from: string; to: string }) => filesService.move(from, to),
    onSuccess: invalidate,
  });
}
