import { describe, expect, it, vi } from "vitest";
import { IcecastManagedForm } from "@/components/admin/icecast-managed-form";
import { render, screen } from "@/test/test-utils";

describe("IcecastManagedForm", () => {
  it("renders port field", () => {
    render(
      <IcecastManagedForm
        port={8000}
        sourcePassword="pass"
        adminUser="admin"
        adminPassword="admin"
        onPortChange={vi.fn()}
        onSourcePasswordChange={vi.fn()}
        onAdminUserChange={vi.fn()}
        onAdminPasswordChange={vi.fn()}
      />,
    );
    expect(screen.getByDisplayValue("8000")).toBeInTheDocument();
  });

  it("renders admin user field", () => {
    render(
      <IcecastManagedForm
        port={8000}
        sourcePassword="src"
        adminUser="icecast"
        adminPassword="secret"
        onPortChange={vi.fn()}
        onSourcePasswordChange={vi.fn()}
        onAdminUserChange={vi.fn()}
        onAdminPasswordChange={vi.fn()}
      />,
    );
    expect(screen.getByDisplayValue("icecast")).toBeInTheDocument();
  });
});
