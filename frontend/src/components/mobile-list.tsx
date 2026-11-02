import Paper from "@mui/material/Paper";
import type { ReactNode } from "react";

/** A single mobile list surface with compact, separated rows. */
export function MobileList({ children }: { children: ReactNode }) {
  return (
    <Paper
      variant="outlined"
      sx={{
        borderRadius: 2,
        overflow: "hidden",
        "& > .MuiCard-root": {
          border: 0,
          borderBottom: 1,
          borderColor: "divider",
          borderRadius: 0,
          bgcolor: "transparent",
          boxShadow: "none",
          "&:last-child": { borderBottom: 0 },
          "&:hover": { bgcolor: "action.hover" },
        },
      }}
    >
      {children}
    </Paper>
  );
}
