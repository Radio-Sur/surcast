import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { PasswordField } from "@/components/admin/password-field";
import { render, screen } from "@/test/test-utils";

describe("PasswordField", () => {
  it("renders with password type by default", () => {
    render(<PasswordField label="Password" value="secret" onChange={() => {}} />);
    const input = screen.getByLabelText("Password") as HTMLInputElement;
    expect(input.type).toBe("password");
  });

  it("toggles visibility on button click", async () => {
    render(<PasswordField label="Password" value="secret" onChange={() => {}} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button"));
    const input = screen.getByLabelText("Password") as HTMLInputElement;
    expect(input.type).toBe("text");
  });

  it("calls onChange when value changes", async () => {
    const onChange = vi.fn();
    render(<PasswordField label="Password" value="" onChange={onChange} />);
    const user = userEvent.setup();
    const input = screen.getByLabelText("Password");
    await user.type(input, "x");
    expect(onChange).toHaveBeenCalledWith("x");
  });
});
