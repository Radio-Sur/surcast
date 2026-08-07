import { describe, expect, it } from "vitest";
import { ScheduleInfoBanner } from "@/components/schedule/schedule-info-banner";
import { render, screen } from "@/test/test-utils";

describe("ScheduleInfoBanner", () => {
  it("shows skeleton when loading", () => {
    const { container } = render(
      <ScheduleInfoBanner schedules={undefined} isLoading={true} upcoming={[]} nowPlaying={null} elapsed={0} />,
    );
    expect(container.querySelector('[class*="MuiSkeleton"]')).toBeInTheDocument();
  });

  it("returns null when schedules is undefined", () => {
    const { container } = render(
      <ScheduleInfoBanner schedules={undefined} isLoading={false} upcoming={[]} nowPlaying={null} elapsed={0} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it("shows empty message when no schedules exist", () => {
    render(<ScheduleInfoBanner schedules={[]} isLoading={false} upcoming={[]} nowPlaying={null} elapsed={0} />);
    expect(screen.getByText(/no scheduled events/i)).toBeInTheDocument();
  });
});
