import { describe, expect, it } from "vitest";
import { RoleChip } from "@/components/role-chip";
import { render, screen } from "@/test/test-utils";

describe("RoleChip", () => {
  it("renders admin role capitalized", () => {
    render(<RoleChip roleName="admin" />);
    expect(screen.getByText("Admin")).toBeInTheDocument();
  });

  it("renders demo role capitalized", () => {
    render(<RoleChip roleName="demo" />);
    expect(screen.getByText("Demo")).toBeInTheDocument();
  });
});
