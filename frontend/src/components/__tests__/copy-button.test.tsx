import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { CopyButton } from "@/components/copy-button";
import { render, screen } from "@/test/test-utils";

describe("CopyButton", () => {
  it("renders copy button", () => {
    render(<CopyButton text="hello" />);
    expect(screen.getByRole("button")).toBeInTheDocument();
  });

  it("copies text on click", async () => {
    Object.assign(navigator, {
      clipboard: { writeText: vi.fn().mockResolvedValue(undefined) },
    });

    render(<CopyButton text="hello" />);

    await userEvent.click(screen.getByRole("button"));
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith("hello");
  });
});
