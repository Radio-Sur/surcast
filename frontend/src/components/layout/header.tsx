import Check from "@mui/icons-material/Check";
import Avatar from "@mui/material/Avatar";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
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
import { useNavigate } from "react-router-dom";
import { ThemeToggle } from "@/components/theme-toggle";
import { useAuth } from "@/hooks/use-auth";

const LANGUAGES = [
  { code: "en", label: "English", flag: "🇬🇧" },
  { code: "pl", label: "Polski", flag: "🇵🇱" },
];

export function Header() {
  const { t, i18n } = useTranslation("nav");
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const [langAnchorEl, setLangAnchorEl] = useState<HTMLElement | null>(null);

  const initials =
    user?.name
      ?.split(" ")
      .map((n) => n[0])
      .join("")
      .toUpperCase()
      .slice(0, 2) ?? "U";

  const handleLangChange = (code: string) => {
    i18n.changeLanguage(code);
    setLangAnchorEl(null);
  };

  const handleLogout = () => {
    setAnchorEl(null);
    logout();
    navigate("/login");
  };

  return (
    <Box
      component="header"
      sx={{
        px: 4,
        py: 2,
        display: "flex",
        alignItems: "center",
        justifyContent: "flex-end",
        gap: 1,
        bgcolor: "transparent",
      }}
    >
      <Tooltip title={i18n.language === "pl" ? "Zmień język" : "Change language"}>
        <Button
          onClick={(e) => setLangAnchorEl(e.currentTarget)}
          size="small"
          variant="outlined"
          sx={{
            borderRadius: 20,
            px: 1.5,
            minWidth: 0,
            borderColor: "divider",
            textTransform: "none",
            color: "text.primary",
            gap: 1,
            fontSize: "0.8rem",
            fontWeight: 600,
          }}
        >
          {LANGUAGES.find((l) => l.code === i18n.language)?.flag}
          <Box sx={{ ml: 0.5 }}>{i18n.language.toUpperCase()}</Box>
        </Button>
      </Tooltip>
      <Menu
        anchorEl={langAnchorEl}
        open={!!langAnchorEl}
        onClose={() => setLangAnchorEl(null)}
        slotProps={{ paper: { sx: { borderRadius: 2, mt: 1, px: 1, minWidth: 180 } } }}
      >
        <Box sx={{ px: 2, py: 1 }}>
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{ fontWeight: 600, letterSpacing: "0.05em", textTransform: "uppercase" }}
          >
            {t("language")}
          </Typography>
        </Box>
        <Divider />
        <Box sx={{ py: 0.5 }}>
          {LANGUAGES.map((lang) => (
            <MenuItem
              key={lang.code}
              onClick={() => handleLangChange(lang.code)}
              disableRipple
              sx={{ borderRadius: 2, px: 2, py: 1, my: 0.25 }}
            >
              <ListItemIcon sx={{ minWidth: 36, fontSize: "1.1rem" }}>{lang.flag}</ListItemIcon>
              <ListItemText>{lang.label}</ListItemText>
              {i18n.language === lang.code && <Check fontSize="small" color="primary" />}
            </MenuItem>
          ))}
        </Box>
      </Menu>
      <ThemeToggle />
      <IconButton
        onClick={(e) => setAnchorEl(e.currentTarget)}
        size="small"
        sx={{
          borderRadius: 3,
          px: 1.5,
          py: 0.5,
          gap: 1,
          "&:hover": { bgcolor: "action.hover" },
        }}
      >
        <Typography variant="body2" sx={{ fontWeight: 600 }}>
          {user?.name}
        </Typography>
        <Avatar
          sx={{
            width: 30,
            height: 30,
            fontSize: 12,
            fontWeight: 700,
            bgcolor: "primary.main",
            color: "primary.contrastText",
          }}
        >
          {initials}
        </Avatar>
      </IconButton>
      <Menu
        anchorEl={anchorEl}
        open={!!anchorEl}
        onClose={() => setAnchorEl(null)}
        slotProps={{
          paper: {
            sx: { borderRadius: 2, mt: 1, minWidth: 240, px: 2 },
          },
        }}
      >
        <Box sx={{ px: 2, py: 1.5 }}>
          <Typography variant="body2" sx={{ fontWeight: 700 }}>
            {user?.name}
          </Typography>
          <Typography variant="caption" color="text.secondary">
            @{user?.username}
          </Typography>
        </Box>
        <Divider />
        <MenuItem onClick={handleLogout} sx={{ borderRadius: 2, my: 0.5, px: 2, py: 1 }}>
          {t("log_out")}
        </MenuItem>
      </Menu>
    </Box>
  );
}
