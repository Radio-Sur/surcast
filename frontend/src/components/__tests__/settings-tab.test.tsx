import { describe, expect, it, vi } from "vitest";
import { SettingsTab } from "@/pages/stations/settings-tab";
import { render, screen } from "@/test/test-utils";

describe("SettingsTab", () => {
  const prebufferBytes = 32768;
  const playedLimit = 100;
  const defaultFadeMs = 2000;
  const streamUrl = "http://localhost:8000/main";

  it("renders stream URL", () => {
    render(
      <SettingsTab
        prebufferBytes={prebufferBytes}
        playedLimit={playedLimit}
        defaultFadeMs={defaultFadeMs}
        transitionMode="crossfade"
        autocueFadeMaxMs={5000}
        streamUrl={streamUrl}
        updateStation={vi.fn()}
        updatePending={false}
      />,
    );
    expect(screen.getByText(streamUrl)).toBeInTheDocument();
  });

  it("renders prebuffer slider with current value", () => {
    render(
      <SettingsTab
        prebufferBytes={prebufferBytes}
        playedLimit={playedLimit}
        defaultFadeMs={defaultFadeMs}
        transitionMode="crossfade"
        autocueFadeMaxMs={5000}
        streamUrl={streamUrl}
        updateStation={vi.fn()}
        updatePending={false}
      />,
    );
    expect(screen.getByText(/32,768/i)).toBeInTheDocument();
  });

  it("renders played limit slider", () => {
    render(
      <SettingsTab
        prebufferBytes={prebufferBytes}
        playedLimit={playedLimit}
        defaultFadeMs={defaultFadeMs}
        transitionMode="crossfade"
        autocueFadeMaxMs={5000}
        streamUrl={streamUrl}
        updateStation={vi.fn()}
        updatePending={false}
      />,
    );
    expect(screen.getByText(/played history/i)).toBeInTheDocument();
  });

  it("renders transition mode toggle and crossfade slider by default", () => {
    render(
      <SettingsTab
        prebufferBytes={prebufferBytes}
        playedLimit={playedLimit}
        defaultFadeMs={defaultFadeMs}
        transitionMode="crossfade"
        autocueFadeMaxMs={5000}
        streamUrl={streamUrl}
        updateStation={vi.fn()}
        updatePending={false}
      />,
    );
    expect(screen.getByRole("button", { name: /crossfade/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /autocue/i })).toBeInTheDocument();
    expect(screen.getAllByText(/crossfade/i).length).toBeGreaterThanOrEqual(2);
  });

  it("renders autocue fade slider only in autocue mode", () => {
    const { rerender } = render(
      <SettingsTab
        prebufferBytes={prebufferBytes}
        playedLimit={playedLimit}
        defaultFadeMs={defaultFadeMs}
        transitionMode="crossfade"
        autocueFadeMaxMs={5000}
        streamUrl={streamUrl}
        updateStation={vi.fn()}
        updatePending={false}
      />,
    );
    expect(screen.queryByText(/max autocue fade/i)).not.toBeInTheDocument();

    rerender(
      <SettingsTab
        prebufferBytes={prebufferBytes}
        playedLimit={playedLimit}
        defaultFadeMs={defaultFadeMs}
        transitionMode="autocue"
        autocueFadeMaxMs={5000}
        streamUrl={streamUrl}
        updateStation={vi.fn()}
        updatePending={false}
      />,
    );
    expect(screen.getByText(/max autocue fade/i)).toBeInTheDocument();
  });

  it("has save button", () => {
    const updateStation = vi.fn();
    render(
      <SettingsTab
        prebufferBytes={prebufferBytes}
        playedLimit={playedLimit}
        defaultFadeMs={defaultFadeMs}
        transitionMode="crossfade"
        autocueFadeMaxMs={5000}
        streamUrl={streamUrl}
        updateStation={updateStation}
        updatePending={false}
      />,
    );
    expect(screen.getByRole("button", { name: /save/i })).toBeInTheDocument();
  });
});
