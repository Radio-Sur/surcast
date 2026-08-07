import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import Box from "@mui/material/Box";
import Collapse from "@mui/material/Collapse";
import Typography from "@mui/material/Typography";
import type { ReactNode } from "react";

interface CollapsibleQueueSectionProps {
  title: string;
  open: boolean;
  onToggle: () => void;
  children: ReactNode;
  count?: number;
}

export function CollapsibleQueueSection({ title, open, onToggle, children, count }: CollapsibleQueueSectionProps) {
  return (
    <Box>
      <Box
        onClick={onToggle}
        sx={{
          display: "flex",
          alignItems: "center",
          gap: 0.5,
          cursor: "pointer",
          px: 1,
          mb: 0.5,
        }}
      >
        {open ? <ExpandLess fontSize="small" /> : <ExpandMore fontSize="small" />}
        <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 600 }}>
          {title}
        </Typography>
        {count !== undefined && (
          <Box
            sx={{
              ml: 0.5,
              px: 0.75,
              py: 0.125,
              borderRadius: 1,
              bgcolor: "action.selected",
              fontSize: "0.7rem",
              lineHeight: 1.2,
              color: "text.secondary",
            }}
          >
            {count}
          </Box>
        )}
      </Box>
      <Collapse in={open}>
        <Box sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>{children}</Box>
      </Collapse>
    </Box>
  );
}
