import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ThemeProvider, useTheme } from "@/providers/theme-provider";

function TestConsumer() {
  const { mode, accent, resolvedTheme, setMode, setAccent } = useTheme();
  return (
    <div>
      <div data-testid="mode">{mode}</div>
      <div data-testid="accent">{accent}</div>
      <div data-testid="resolvedTheme">{resolvedTheme}</div>
      <button type="button" data-testid="setDark" onClick={() => setMode("dark")}>
        Dark
      </button>
      <button type="button" data-testid="setLight" onClick={() => setMode("light")}>
        Light
      </button>
      <button type="button" data-testid="setAccentGreen" onClick={() => setAccent("green")}>
        AccentGreen
      </button>
    </div>
  );
}

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  localStorage.clear();
});

describe("ThemeProvider", () => {
  it("provides default values (mode=system, accent=blue)", () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("mode")).toHaveTextContent("system");
    expect(screen.getByTestId("accent")).toHaveTextContent("blue");
  });

  it("resolvedTheme is light by default in jsdom (prefers-color-scheme: dark returns false)", () => {
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("resolvedTheme")).toHaveTextContent("light");
  });

  it("setMode updates the mode context", async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    await user.click(screen.getByTestId("setDark"));
    expect(screen.getByTestId("mode")).toHaveTextContent("dark");
    expect(screen.getByTestId("resolvedTheme")).toHaveTextContent("dark");
  });

  it("setAccent updates the accent context", async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    await user.click(screen.getByTestId("setAccentGreen"));
    expect(screen.getByTestId("accent")).toHaveTextContent("green");
  });

  it("persists mode to localStorage", async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    await user.click(screen.getByTestId("setDark"));
    expect(localStorage.getItem("surcast-theme")).toBe("dark");
  });

  it("persists accent to localStorage", async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    await user.click(screen.getByTestId("setAccentGreen"));
    expect(localStorage.getItem("surcast-accent")).toBe("green");
  });

  it("restores mode from localStorage on mount", () => {
    localStorage.setItem("surcast-theme", "light");
    localStorage.setItem("surcast-accent", "purple");
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("mode")).toHaveTextContent("light");
    expect(screen.getByTestId("accent")).toHaveTextContent("purple");
  });

  it("falls back to system for invalid stored mode", () => {
    localStorage.setItem("surcast-theme", "invalid");
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("mode")).toHaveTextContent("system");
  });

  it("falls back to blue for invalid stored accent", () => {
    localStorage.setItem("surcast-accent", "nonexistent");
    render(
      <ThemeProvider>
        <TestConsumer />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("accent")).toHaveTextContent("blue");
  });

  it("renders children", () => {
    render(
      <ThemeProvider>
        <div data-testid="child">hi</div>
      </ThemeProvider>,
    );
    expect(screen.getByTestId("child")).toHaveTextContent("hi");
  });
});

describe("useTheme outside ThemeProvider", () => {
  it("throws an error", () => {
    expect(() => render(<TestConsumer />)).toThrow("useTheme must be used within ThemeProvider");
  });
});
