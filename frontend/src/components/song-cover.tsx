import MusicNote from "@mui/icons-material/MusicNote";
import Box from "@mui/material/Box";
import { useState } from "react";
import { useTranslation } from "react-i18next";

export function SongCover({
  songId,
  hasCover,
  size = 40,
  autoDj,
}: {
  songId: string;
  hasCover: boolean;
  size?: number;
  autoDj?: boolean;
}) {
  const { t } = useTranslation("schedule");
  const [imgError, setImgError] = useState(false);
  const showPlaceholder = !hasCover || imgError;
  const img = (
    <Box
      component="img"
      src={`/api/songs/${songId}/cover`}
      alt=""
      onError={() => setImgError(true)}
      sx={{
        width: size,
        height: size,
        borderRadius: 0.75,
        objectFit: "cover",
        display: "block",
        flexShrink: 0,
      }}
    />
  );
  const placeholder = (
    <Box
      sx={{
        width: size,
        height: size,
        borderRadius: 0.75,
        bgcolor: "action.hover",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        flexShrink: 0,
      }}
    >
      <MusicNote fontSize="small" color="disabled" />
    </Box>
  );
  const content = showPlaceholder ? placeholder : img;
  if (autoDj) {
    return (
      <Box sx={{ position: "relative", flexShrink: 0, width: size, height: size }}>
        {content}
        <Box
          sx={{
            position: "absolute",
            bottom: -2,
            right: -4,
            bgcolor: "primary.main",
            color: "primary.contrastText",
            fontSize: 9,
            fontWeight: 700,
            px: 0.75,
            py: 0.25,
            borderRadius: 1,
            lineHeight: 1,
            boxShadow: 1,
          }}
        >
          {t("auto_dj")}
        </Box>
      </Box>
    );
  }
  return content;
}
