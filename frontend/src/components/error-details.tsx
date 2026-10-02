import Close from "@mui/icons-material/Close";
import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import type { AlertColor } from "@mui/material/Alert";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Collapse from "@mui/material/Collapse";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
import { type Ref, useState } from "react";
import { useTranslation } from "react-i18next";
import { CopyButton } from "@/components/copy-button";

export interface ErrorDetailsInfo {
  message: string;
  code?: string;
  status?: number;
  category?: string;
  /** Sanitized backend cause (e.g. pipeline reason). Never secrets. */
  details?: string;
  raw?: unknown;
}

function formatRaw(raw: unknown): string {
  if (raw === undefined || raw === null) return "";
  if (typeof raw === "string") return raw;
  try {
    return JSON.stringify(raw, null, 2);
  } catch {
    return String(raw);
  }
}

/**
 * Alert with a text-only "details" toggle: clicking the notification (or
 * the button) expands a code block with the backend cause (status, code,
 * category, details) and a copy button. Exactly one pictogram — the
 * severity icon — never an icon pair.
 */
export function ErrorDetails({
  title,
  details,
  severity = "error",
  onClose,
  ref,
}: {
  title: string;
  details?: ErrorDetailsInfo | null;
  severity?: AlertColor;
  onClose?: () => void;
  // Forwarded to the underlying Alert so transitions measuring the DOM
  // node (e.g. Snackbar's Grow calling reflow(node)) never see null.
  ref?: Ref<HTMLDivElement>;
}) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const body = details?.details || formatRaw(details?.raw);
  const hasDetails = Boolean(details && (details.code ?? details.status ?? details.category ?? body));
  const meta = details
    ? [details.status !== undefined && `HTTP ${details.status}`, details.code, details.category]
        .filter(Boolean)
        .join(" · ")
    : "";

  return (
    <Alert
      ref={ref}
      severity={severity}
      variant="filled"
      sx={{
        width: "100%",
        alignItems: "flex-start",
        cursor: hasDetails ? "pointer" : undefined,
        // Shared axis: the severity glyph box is 15px padding-top + 22px
        // glyph = 37px tall, so the title row matches it and glyph, title,
        // toggle and close all sit on one center line that never moves.
        "& .MuiAlert-icon": { paddingTop: "15px", paddingBottom: 0 },
        "& .MuiAlert-action": { paddingTop: 0 },
      }}
      onClick={() => {
        if (hasDetails) setExpanded((v) => !v);
      }}
    >
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          gap: 1,
          minHeight: 37,
        }}
      >
        <Box sx={{ lineHeight: 1.5, flex: 1 }}>{title}</Box>
        {hasDetails && (
          <Button
            size="small"
            onClick={(e) => {
              e.stopPropagation();
              setExpanded((v) => !v);
            }}
            endIcon={expanded ? <ExpandLess fontSize="small" /> : <ExpandMore fontSize="small" />}
            sx={{
              color: "inherit",
              fontWeight: 700,
              textTransform: "none",
              whiteSpace: "nowrap",
              opacity: 1,
              flexShrink: 0,
            }}
          >
            {expanded ? t("errors:hide_details") : t("errors:show_details")}
          </Button>
        )}
        {onClose && (
          <IconButton
            aria-label={t("common:close", "Close")}
            size="small"
            color="inherit"
            onClick={(e) => {
              e.stopPropagation();
              onClose();
            }}
            sx={{ flexShrink: 0 }}
          >
            <Close fontSize="small" />
          </IconButton>
        )}
      </Box>
      {hasDetails && details && (
        <Collapse in={expanded}>
          <Box
            sx={{ mt: 1, maxWidth: 440, borderRadius: 1, bgcolor: "rgba(0, 0, 0, 0.28)", p: 1.5 }}
            onClick={(e) => e.stopPropagation()}
          >
            <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 1, pl: 0.5 }}>
              <Typography variant="caption" component="div" sx={{ fontWeight: 700, letterSpacing: 0.4 }}>
                {meta || t("errors:show_details")}
              </Typography>
              {body && <CopyButton text={body} />}
            </Box>
            {body && (
              <Typography
                variant="caption"
                component="pre"
                sx={{
                  m: 0,
                  mt: 0.5,
                  whiteSpace: "pre-wrap",
                  wordBreak: "break-word",
                  fontFamily: "monospace",
                }}
              >
                {body}
              </Typography>
            )}
          </Box>
        </Collapse>
      )}
    </Alert>
  );
}
