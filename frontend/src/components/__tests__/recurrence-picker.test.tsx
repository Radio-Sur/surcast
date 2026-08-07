import { describe, expect, it, vi } from "vitest";
import { RecurrencePicker } from "@/pages/stations/recurrence-picker";
import { render, screen } from "@/test/test-utils";

describe("RecurrencePicker", () => {
  const baseProps = {
    value: "none" as const,
    interval: null,
    days: null,
    endDate: null,
    count: null,
    onChange: vi.fn(),
  };

  it("renders all recurrence options", () => {
    render(<RecurrencePicker {...baseProps} />);
    expect(screen.getByText(/does not repeat/i)).toBeInTheDocument();
    expect(screen.getByText(/every day/i)).toBeInTheDocument();
    expect(screen.getByText(/every week/i)).toBeInTheDocument();
  });

  it("shows day checkboxes when custom_days is selected", () => {
    render(<RecurrencePicker {...baseProps} value="custom_days" days={[0, 2]} />);
    expect(screen.getAllByRole("checkbox").length).toBeGreaterThanOrEqual(2);
  });

  it("shows interval field when every_n_days is selected", () => {
    render(<RecurrencePicker {...baseProps} value="every_n_days" interval={3} />);
    expect(screen.getByDisplayValue("3")).toBeInTheDocument();
  });

  it("renders end date options", () => {
    render(<RecurrencePicker {...baseProps} />);
    expect(screen.getByText(/never/i)).toBeInTheDocument();
    expect(screen.getByText(/after/i)).toBeInTheDocument();
    expect(screen.getByText(/on date/i)).toBeInTheDocument();
  });

  it("renders count field when count is set", () => {
    render(<RecurrencePicker {...baseProps} count={5} />);
    expect(screen.getByDisplayValue("5")).toBeInTheDocument();
  });

  it("renders date picker when endDate is set", () => {
    render(<RecurrencePicker {...baseProps} endDate="2026-12-31" />);
    expect(screen.getByDisplayValue("2026-12-31")).toBeInTheDocument();
  });
});
