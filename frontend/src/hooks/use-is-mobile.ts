import { useTheme } from "@mui/material/styles";
import useMediaQuery from "@mui/material/useMediaQuery";

/**
 * True on phone-sized viewports (and narrow tablets). Pages use this to switch
 * wide tables for touch-friendly card lists.
 */
export function useIsMobile(breakpoint: "sm" | "md" = "md") {
  const theme = useTheme();
  return useMediaQuery(theme.breakpoints.down(breakpoint));
}
