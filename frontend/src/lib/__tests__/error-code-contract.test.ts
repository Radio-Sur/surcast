import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { expect, it } from "vitest";
import { parseErrorCode } from "@/lib/error-codes";

function rustSources(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory() ? rustSources(file) : entry.name.endsWith(".rs") ? [file] : [];
  });
}

it("recognizes explicit backend HTTP, authentication and WebSocket error codes", () => {
  const directory = path.resolve(process.cwd(), "../backend/src");
  const expressions = [
    /with_code\(\s*"([A-Z_]+)"/g,
    /\bcode:\s*"([A-Z_]+)"/g,
    /auth_error\([^;]*?"([A-Z_]+)"/g,
    /=>\s*\("([A-Z_]+)",\s*"[a-z_]+"\)/g,
  ];
  const codes = new Set<string>();
  for (const file of rustSources(directory)) {
    const source = readFileSync(file, "utf8");
    for (const expression of expressions) {
      for (const match of source.matchAll(expression)) codes.add(match[1]);
    }
  }
  expect(codes.size).toBeGreaterThan(20);
  for (const code of codes)
    expect(parseErrorCode(code), `Missing frontend translation contract for ${code}`).toBe(code);
});
