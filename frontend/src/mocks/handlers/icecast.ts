import { HttpResponse, http } from "msw";

let icecastRunning = false;

export function resetIcecastState() {
  icecastRunning = false;
}

export const icecastHandlers = [
  http.get("/api/admin/icecast", () => {
    return HttpResponse.json({
      running: icecastRunning,
      version: "2.4.4",
      admin_username: "admin",
      source_password: "****",
      relay_password: "****",
      admin_password: "****",
      host: "localhost",
      port: 8000,
      mount_point: "/stream",
      name: "Surcast Icecast",
      description: "Icecast streaming server",
      url: "",
      genre: "Various",
    });
  }),

  http.patch("/api/admin/icecast", async ({ request }) => {
    const body = (await request.json()) as Record<string, unknown>;
    return HttpResponse.json({ success: true, ...body });
  }),

  http.post("/api/admin/icecast/start", () => {
    icecastRunning = true;
    return HttpResponse.json({ success: true, running: true });
  }),

  http.post("/api/admin/icecast/stop", () => {
    icecastRunning = false;
    return HttpResponse.json({ success: true, running: false });
  }),

  http.post("/api/admin/icecast/test", () => {
    return HttpResponse.json({ success: true, message: "Connection successful" });
  }),

  http.post("/api/admin/icecast/restart", () => {
    return HttpResponse.json({ success: true, running: true });
  }),
];
