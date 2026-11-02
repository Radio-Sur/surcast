import { act } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { ErrorDetails } from "@/components/error-details";
import i18n from "@/i18n";
import { render, screen } from "@/test/test-utils";

describe("ErrorDetails", () => {
  it("updates an already visible error when the language changes", async () => {
    render(
      <ErrorDetails
        title="Raw server prose"
        details={{ message: "Raw server prose", code: "DEMO_RESTRICTED", category: "auth" }}
      />,
    );
    expect(screen.getByText("This feature is unavailable in the demo version")).toBeInTheDocument();
    await act(async () => {
      await i18n.changeLanguage("pl");
    });
    expect(screen.getByText("Ta funkcja jest niedostępna w wersji demo")).toBeInTheDocument();
    expect(screen.queryByText("Raw server prose")).not.toBeInTheDocument();
    await act(async () => {
      await i18n.changeLanguage("en");
    });
  });
  it("renders the title without an info button when there are no details", () => {
    render(<ErrorDetails title="Something broke" details={null} />);
    expect(screen.getByText("Something broke")).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("expands backend details after clicking the notification", async () => {
    render(
      <ErrorDetails
        title="Stream playback failed"
        details={{
          message: "Stream playback failed",
          code: "STREAM_PLAYBACK_FAILED",
          status: 500,
          category: "stream",
          details: "state transition to Playing stalled at Paused",
        }}
      />,
    );
    expect(screen.queryByText(/stalled at Paused/)).not.toBeInTheDocument();
    // Clicking anywhere on the notification expands the details.
    await userEvent.click(screen.getByText("Stream playback failed"));
    expect(screen.queryByText(/stalled at Paused/)).not.toBeInTheDocument();
    expect(screen.getByText(/Technical diagnostics/)).toBeInTheDocument();
    expect(screen.getByText(/HTTP 500/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Hide details" })).toBeInTheDocument();
  });

  it("shows exactly one pictogram: the severity icon, text-only toggle", async () => {
    render(
      <ErrorDetails
        title="Stream playback failed"
        details={{ message: "x", code: "STREAM_PLAYBACK_FAILED", status: 500, category: "stream" }}
      />,
    );
    expect(screen.getAllByRole("button")).toHaveLength(1);
    expect(screen.getByTestId("ErrorOutlineIcon")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Show error details" })).toBeInTheDocument();
  });
});
