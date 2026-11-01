import { httpClient } from "@/lib/api";
import type { HttpClient } from "@/lib/http-client";
import type { DirectoryListing, FileSortKey, SortOrder } from "@/types";

export function createFilesService(client: HttpClient) {
  return {
    list: (params: { path?: string; sort?: FileSortKey; order?: SortOrder }) => {
      const query = new URLSearchParams();
      if (params.path) query.set("path", params.path);
      if (params.sort) query.set("sort", params.sort);
      if (params.order) query.set("order", params.order);
      const qs = query.toString();
      return client.get<DirectoryListing>(`/files${qs ? `?${qs}` : ""}`);
    },
    listFolders: () => client.get<string[]>("/files/folders"),
    createFolder: (path: string) => client.post<{ path: string }>("/files/folders", { path }),
    deleteFolder: (path: string) => client.delete(`/files/folders?path=${encodeURIComponent(path)}`),
    deleteFile: (path: string) => client.delete(`/files?path=${encodeURIComponent(path)}`),
    move: (from: string, to: string) => client.post<void>("/files/move", { from, to }),
  };
}

export const filesService = createFilesService(httpClient);
