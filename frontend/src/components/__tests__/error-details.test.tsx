import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { ErrorDetails } from "@/components/error-details";
import { render, screen } from "@/test/test-utils";

describe("ErrorDetails", () => {
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
    expect(screen.queryByText(/stalled at Paused/)).not.toBeVisible();
    // Clicking anywhere on the notification expands the details.
    await userEvent.click(screen.getByText("Stream playback failed"));
    expect(screen.getByText(/stalled at Paused/)).toBeInTheDocument();
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
