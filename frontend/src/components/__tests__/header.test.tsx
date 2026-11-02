import { beforeEach, describe, expect, it } from "vitest";
import { Header } from "@/components/layout/header";
import { server } from "@/mocks/server";
import { render, screen, setupAuth } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

describe("Header", () => {
  it("renders username", async () => {
    render(<Header />, { route: "/" });
    expect(await screen.findByText("admin")).toBeInTheDocument();
  });

  it("renders language switcher button", async () => {
    render(<Header />, { route: "/" });
    expect(await screen.findByText("EN-US")).toBeInTheDocument();
  });

  it("renders theme toggle", async () => {
    render(<Header />, { route: "/" });
    expect(await screen.findByRole("button", { name: /theme settings/i })).toBeInTheDocument();
  });
});
