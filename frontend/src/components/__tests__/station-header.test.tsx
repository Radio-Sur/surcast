import { createTheme } from "@mui/material/styles";
import { beforeEach, expect, it } from "vitest";
import i18n from "@/i18n";
import { STORAGE_KEYS } from "@/lib/constants";
import { fakeStation } from "@/mocks/data";
import { StationHeader } from "@/pages/stations/station-header";
import { render, screen, setupAuth } from "@/test/test-utils";

beforeEach(setupAuth);

it.each(["light", "dark"] as const)("keeps the status chip readable with a purple accent in %s mode", (mode) => {
  localStorage.setItem(STORAGE_KEYS.THEME, mode);
  localStorage.setItem(STORAGE_KEYS.ACCENT, "purple");
  const palette = createTheme({ palette: { mode } }).palette;
  const props = {
    station: fakeStation(),
    onBack: () => {},
    onToggle: () => {},
    onRestart: () => {},
    onEdit: () => {},
  };
  const { rerender } = render(<StationHeader {...props} playing />);
  const live = screen.getByText(i18n.t("stations:stream_status_live")).closest(".MuiChip-root");
  expect(live).toHaveClass("MuiChip-colorSuccess");
  expect(live).toHaveStyle({
    backgroundColor: palette.success.light,
    color: palette.getContrastText(palette.success.light),
  });

  rerender(<StationHeader {...props} playing={false} />);
  const stopped = screen.getByText(i18n.t("stations:stream_status_stopped")).closest(".MuiChip-root");
  expect(stopped).toHaveStyle({ color: mode === "light" ? "rgba(0, 0, 0, 0.87)" : "#fff" });
});
