import ContentCopy from "@mui/icons-material/ContentCopy";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import { useState } from "react";
import { useTranslation } from "react-i18next";

export function CopyButton({ text }: { text: string }) {
  const { t } = useTranslation("common");
  const [copied, setCopied] = useState(false);

  return (
    <Tooltip title={copied ? t("copied") : t("copy_to_clipboard")}>
      <IconButton
        size="small"
        onClick={async () => {
          try {
            await navigator.clipboard.writeText(text);
          } catch {
            /* clipboard not available */
          }
          setCopied(true);
          setTimeout(() => setCopied(false), 2000);
        }}
      >
        <ContentCopy fontSize="small" />
      </IconButton>
    </Tooltip>
  );
}
