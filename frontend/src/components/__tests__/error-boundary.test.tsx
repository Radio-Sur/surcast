import { afterEach, expect, it, vi } from "vitest";
import { ErrorBoundary } from "@/components/error-boundary";
import i18n from "@/i18n";
import { render, screen } from "@/test/test-utils";

afterEach(async () => {
  await i18n.changeLanguage("en");
  vi.restoreAllMocks();
});

it.each(["en", "pl"])(
  "localizes unexpected frontend errors instead of exposing exception prose (%s)",
  async (language) => {
    await i18n.changeLanguage(language);
    vi.spyOn(console, "error").mockImplementation(() => {});
    function Broken(): never {
      throw new Error("RAW ENGLISH EXCEPTION");
    }
    render(
      <ErrorBoundary>
        <Broken />
      </ErrorBoundary>,
    );
    expect(screen.getByText(language === "pl" ? "Coś poszło nie tak" : "Something went wrong")).toBeInTheDocument();
    expect(
      screen.getByText(language === "pl" ? "Wystąpił nieoczekiwany błąd" : "An unexpected error occurred"),
    ).toBeInTheDocument();
    expect(screen.queryByText("RAW ENGLISH EXCEPTION")).not.toBeInTheDocument();
  },
);
