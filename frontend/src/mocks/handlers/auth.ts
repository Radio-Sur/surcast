import { HttpResponse, http } from "msw";
import { fakeAuthResponse, fakeUser } from "@/mocks/data";

let setupComplete = false;

export function resetAuthState() {
  setupComplete = false;
}

export const authHandlers = [
  http.get("/api/setup/status", () => {
    return HttpResponse.json({ setup_complete: setupComplete });
  }),

  http.post("/api/setup/init", async ({ request }) => {
    const body = (await request.json()) as { username: string; password: string; name: string };
    setupComplete = true;
    return HttpResponse.json(fakeAuthResponse({ user: { username: body.username, name: body.name, role: "admin" } }));
  }),

  http.post("/api/auth/login", async ({ request }) => {
    const body = (await request.json()) as { username: string; password: string };
    if (body.username === "fail" || body.password === "wrong") {
      return HttpResponse.json({ error: "Invalid credentials" }, { status: 401 });
    }
    return HttpResponse.json(fakeAuthResponse());
  }),

  http.get("/api/auth/me", () => {
    return HttpResponse.json(fakeUser());
  }),

  http.post("/api/auth/refresh", () => {
    return HttpResponse.json(fakeAuthResponse());
  }),
];
