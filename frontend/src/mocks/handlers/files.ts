import { HttpResponse, http } from "msw";
import { fakeDirectoryListing, fakeFileEntry, fakeFolderEntry } from "@/mocks/data";
import type { DirectoryListing, FileEntry } from "@/types";

const files = new Map<string, FileEntry>();
const folders = new Set<string>();

export function resetFiles() {
  files.clear();
  folders.clear();
}

export function seedFiles() {
  folders.add("rock");
  folders.add("rock/2020");
  files.set("intro.mp3", fakeFileEntry({ name: "intro.mp3", path: "intro.mp3" }));
  files.set("rock/hit.mp3", fakeFileEntry({ name: "hit.mp3", path: "rock/hit.mp3", size: 2048 }));
}

function baseName(path: string) {
  return path.split("/").pop() ?? path;
}

function parentPath(path: string) {
  return path.includes("/") ? path.slice(0, path.lastIndexOf("/")) : "";
}

function compare(a: string, b: string) {
  return a.localeCompare(b);
}

export const filesHandlers = [
  http.get("/api/files", ({ request }) => {
    const url = new URL(request.url);
    const path = url.searchParams.get("path") ?? "";
    const sort = url.searchParams.get("sort") ?? "name";
    const order = url.searchParams.get("order") ?? "asc";

    const childFolders = Array.from(folders)
      .filter((folder) => parentPath(folder) === path)
      .map((folder) => fakeFolderEntry({ name: baseName(folder), path: folder }));
    const childFiles = Array.from(files.values()).filter((file) => parentPath(file.path) === path);

    const dir = order === "desc" ? -1 : 1;
    const byKey = (a: FileEntry, b: FileEntry) => {
      if (sort === "size") return (a.size - b.size) * dir;
      if (sort === "modified") return ((a.modified_at ?? 0) - (b.modified_at ?? 0)) * dir;
      if (sort === "type") return compare(a.name.split(".").pop() ?? "", b.name.split(".").pop() ?? "") * dir;
      return compare(a.name, b.name) * dir;
    };

    childFolders.sort((a, b) => compare(a.name, b.name));
    childFiles.sort(byKey);

    const listing: DirectoryListing = fakeDirectoryListing({
      path,
      parent: path ? parentPath(path) : null,
      folders: childFolders,
      files: childFiles,
    });
    return HttpResponse.json(listing);
  }),

  http.get("/api/files/folders", () => HttpResponse.json(Array.from(folders))),

  http.post("/api/files/folders", async ({ request }) => {
    const { path } = (await request.json()) as { path: string };
    if (folders.has(path)) return HttpResponse.json({ error: "Already exists" }, { status: 409 });
    folders.add(path);
    return HttpResponse.json({ path }, { status: 201 });
  }),

  http.delete("/api/files/folders", ({ request }) => {
    const path = new URL(request.url).searchParams.get("path") ?? "";
    let deleted = 0;
    for (const folder of Array.from(folders)) {
      if (folder === path || folder.startsWith(`${path}/`)) folders.delete(folder);
    }
    for (const file of Array.from(files.keys())) {
      if (parentPath(file) === path || file.startsWith(`${path}/`)) {
        files.delete(file);
        deleted += 1;
      }
    }
    return HttpResponse.json({ deleted });
  }),

  http.delete("/api/files", ({ request }) => {
    const path = new URL(request.url).searchParams.get("path") ?? "";
    const existed = files.delete(path);
    return HttpResponse.json({ deleted: existed ? 1 : 0 });
  }),

  http.post("/api/files/move", async ({ request }) => {
    const { from, to } = (await request.json()) as { from: string; to: string };
    if (from !== to) {
      for (const folder of Array.from(folders)) {
        if (folder === from || folder.startsWith(`${from}/`)) {
          folders.delete(folder);
          folders.add(folder.replace(from, to));
        }
      }
      for (const [path, file] of Array.from(files)) {
        if (path === from || path.startsWith(`${from}/`)) {
          files.delete(path);
          const next = path.replace(from, to);
          files.set(next, { ...file, path: next, name: baseName(next) });
        }
      }
    }
    return new HttpResponse(null, { status: 204 });
  }),
];
