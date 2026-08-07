import { FullConfig } from "@playwright/test";
import { writeFileSync, mkdirSync } from "fs";
import path from "path";

const AUTH_FILE = path.join(__dirname, ".auth", "user.json");

async function globalSetup(_config: FullConfig) {
  const baseURL = "http://localhost:6767";
  mkdirSync(path.dirname(AUTH_FILE), { recursive: true });

  const statusRes = await fetch(`${baseURL}/api/setup/status`);
  const { setup_complete } = await statusRes.json();

  if (!setup_complete) {
    const initRes = await fetch(`${baseURL}/api/setup/init`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: "admin", password: "password123", name: "Admin" }),
    });
    if (!initRes.ok) {
      throw new Error(`Setup init failed: ${initRes.status} ${await initRes.text()}`);
    }
  }

  const loginRes = await fetch(`${baseURL}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: "admin", password: "password123" }),
  });
  if (!loginRes.ok) {
    throw new Error(`Login failed: ${loginRes.status} ${await loginRes.text()}`);
  }

  const { access_token, refresh_token } = await loginRes.json();

  const storageState = {
    cookies: [],
    origins: [
      {
        origin: baseURL,
        localStorage: [
          { name: "access_token", value: access_token },
          { name: "refresh_token", value: refresh_token },
        ],
      },
    ],
  };

  writeFileSync(AUTH_FILE, JSON.stringify(storageState, null, 2));
}

export default globalSetup;
