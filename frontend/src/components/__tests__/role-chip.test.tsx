import { describe, expect, it } from "vitest";
import { RoleChip } from "@/components/role-chip";
import { render, screen } from "@/test/test-utils";

describe("RoleChip", () => {
  it("renders admin role", () => {
    render(<RoleChip roleName="admin" />);
    expect(screen.getByText("admin")).toBeInTheDocument();
  });

  it("renders manager role", () => {
    render(<RoleChip roleName="manager" />);
    expect(screen.getByText("manager")).toBeInTheDocument();
  });

  it("renders viewer role", () => {
    render(<RoleChip roleName="viewer" />);
    expect(screen.getByText("viewer")).toBeInTheDocument();
  });
});
