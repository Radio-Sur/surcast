import Check from "@mui/icons-material/Check";
import DarkMode from "@mui/icons-material/DarkMode";
import LightMode from "@mui/icons-material/LightMode";
import Palette from "@mui/icons-material/Palette";
import SettingsBrightness from "@mui/icons-material/SettingsBrightness";
import Box from "@mui/material/Box";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { type AccentKey, accentOptions, useTheme } from "@/providers/theme-provider";

const accentSwatches: Record<AccentKey, string> = {
  blue: "#5B7FFF",
  green: "#66BB6A",
  purple: "#AB47BC",
  orange: "#FF8A65",
  rose: "#E57373",
};

const accentLabelKeys: Record<
  AccentKey,
  "accent_electric" | "accent_emerald" | "accent_cosmic" | "accent_tangerine" | "accent_crimson"
> = {
  blue: "accent_electric",
  green: "accent_emerald",
  purple: "accent_cosmic",
  orange: "accent_tangerine",
  rose: "accent_crimson",
};

export function ThemeToggle() {
  const { t } = useTranslation("settings");
  const { mode, accent, setMode, setAccent } = useTheme();

  const themeModes = [
    { mode: "light" as const, icon: LightMode, label: t("light") },
    { mode: "dark" as const, icon: DarkMode, label: t("dark") },
    { mode: "system" as const, icon: SettingsBrightness, label: t("system") },
  ];

  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);

  const ModeIcon = themeModes.find((m) => m.mode === mode)?.icon ?? SettingsBrightness;

  return (
    <>
      <Tooltip title={t("theme_settings")}>
        <IconButton
          onClick={(e) => setAnchorEl(e.currentTarget)}
          size="small"
          sx={{
            border: 1,
            borderColor: "divider",
            borderRadius: 2,
            p: 1,
          }}
        >
          <ModeIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Menu
        anchorEl={anchorEl}
        open={!!anchorEl}
        onClose={() => setAnchorEl(null)}
        slotProps={{
          paper: {
            sx: { width: 280, borderRadius: 2, mt: 1, px: 2 },
          },
        }}
      >
        <Box sx={{ px: 2, py: 1.5, display: "flex", alignItems: "center", gap: 1 }}>
          <Palette fontSize="small" color="primary" />
          <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
            {t("theme")}
          </Typography>
        </Box>
        <Divider />
        <Box sx={{ py: 0.5 }}>
          {themeModes.map(({ mode: m, icon: Icon, label }) => (
            <MenuItem
              key={m}
              onClick={() => setMode(m)}
              disableRipple
              sx={{
                borderRadius: 3,
                px: 2,
                py: 1,
                my: 0.25,
                bgcolor: mode === m ? "action.selected" : "transparent",
              }}
            >
              <ListItemIcon>
                <Icon fontSize="small" color={mode === m ? "primary" : "inherit"} />
              </ListItemIcon>
              <ListItemText>{label}</ListItemText>
              {mode === m && <Check fontSize="small" color="primary" />}
            </MenuItem>
          ))}
        </Box>
        <Divider />
        <Box sx={{ px: 2, py: 1.5 }}>
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{ mb: 1.5, display: "block", fontWeight: 600, letterSpacing: "0.05em", textTransform: "uppercase" }}
          >
            {t("accent_color")}
          </Typography>
          <Box sx={{ display: "flex", gap: 1.5, justifyContent: "center" }}>
            {(Object.keys(accentOptions) as AccentKey[]).map((key) => (
              <Tooltip key={key} title={t(accentLabelKeys[key])} arrow>
                <Box
                  onClick={() => {
                    setAccent(key);
                    setAnchorEl(null);
                  }}
                  sx={{
                    width: 32,
                    height: 32,
                    borderRadius: "50%",
                    bgcolor: accentSwatches[key],
                    border: 3,
                    borderColor: accent === key ? accentSwatches[key] : "transparent",
                    outline: accent === key ? `2px solid ${accentSwatches[key]}44` : "none",
                    outlineOffset: 2,
                    cursor: "pointer",
                    transition: "all 0.2s ease-in-out",
                    transform: accent === key ? "scale(1.15)" : "scale(1)",
                    "&:hover": { transform: "scale(1.2)" },
                  }}
                />
              </Tooltip>
            ))}
          </Box>
        </Box>
      </Menu>
    </>
  );
}
