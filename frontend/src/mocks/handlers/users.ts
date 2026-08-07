import { HttpResponse, http } from "msw";
import { fakeUser } from "@/mocks/data";

const users = new Map<string, ReturnType<typeof fakeUser>>();

export function seedUsers() {
  users.clear();
  const admin = fakeUser({ id: "1", username: "admin", name: "Admin User", role: "admin" });
  users.set(admin.id, admin);
  const manager = fakeUser({ id: "2", username: "manager", name: "Manager User", role: "manager" });
  users.set(manager.id, manager);
  const viewer = fakeUser({ id: "3", username: "viewer", name: "Viewer User", role: "viewer" });
  users.set(viewer.id, viewer);
}

export const usersHandlers = [
  http.get("/api/users", () => {
    return HttpResponse.json(Array.from(users.values()));
  }),

  http.put("/api/users/:id", async ({ params, request }) => {
    const body = (await request.json()) as { role?: "admin" | "manager" | "viewer"; name?: string };
    const user = users.get(params.id as string);
    if (!user) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const updated: typeof user = { ...user, ...body };
    users.set(params.id as string, updated);
    return HttpResponse.json(updated);
  }),

  http.delete("/api/users/:id", ({ params }) => {
    users.delete(params.id as string);
    return HttpResponse.json({ success: true });
  }),
];
