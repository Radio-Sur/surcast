import { describe, expect, it } from "vitest";
import { GripDots } from "@/components/queue/grip-dots";
import { render } from "@/test/test-utils";

describe("GripDots", () => {
  it("renders SVG", () => {
    const { container } = render(<GripDots />);
    expect(container.querySelector("svg")).toBeInTheDocument();
  });
});
