import ContentCopy from "@mui/icons-material/ContentCopy";
import LinkIcon from "@mui/icons-material/Link";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import { useTranslation } from "react-i18next";

export function StreamUrlCard({ streamUrl }: { streamUrl: string }) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    try {
      navigator.clipboard.writeText(streamUrl);
    } catch {
      /* clipboard not available */
    }
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Card variant="outlined" sx={{ borderRadius: 3 }}>
      <CardContent sx={{ p: 4, "&:last-child": { pb: 4 } }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 2 }}>
          <LinkIcon color="primary" fontSize="small" />
          <Typography variant="body2" sx={{ fontWeight: 500, flex: 1 }}>
            {t("stations:stream_url")}
          </Typography>
          <Tooltip title={copied ? t("common:copied") : t("common:copy_to_clipboard")}>
            <IconButton onClick={handleCopy} color="primary" sx={{ p: 0, minWidth: 0, minHeight: 0, lineHeight: 1 }}>
              <ContentCopy fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
        <Typography
          variant="body2"
          component="a"
          href={streamUrl}
          target="_blank"
          rel="noopener noreferrer"
          sx={{
            color: "primary.main",
            textDecoration: "underline",
            wordBreak: "break-all",
            display: "block",
          }}
        >
          {streamUrl}
        </Typography>
      </CardContent>
    </Card>
  );
}
