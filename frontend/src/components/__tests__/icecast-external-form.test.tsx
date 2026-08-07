import { describe, expect, it, vi } from "vitest";
import { IcecastExternalForm } from "@/components/admin/icecast-external-form";
import { render, screen } from "@/test/test-utils";

describe("IcecastExternalForm", () => {
  it("renders external URL field", () => {
    render(
      <IcecastExternalForm
        externalUrl="http://example.com:8000"
        sourcePassword="pass"
        adminPassword="admin"
        onExternalUrlChange={vi.fn()}
        onSourcePasswordChange={vi.fn()}
        onAdminPasswordChange={vi.fn()}
      />,
    );
    expect(screen.getByDisplayValue("http://example.com:8000")).toBeInTheDocument();
  });
});
