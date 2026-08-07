import Dashboard from "@mui/icons-material/Dashboard";
import LibraryMusic from "@mui/icons-material/LibraryMusic";
import People from "@mui/icons-material/People";
import PlaylistPlay from "@mui/icons-material/PlaylistPlay";
import Radio from "@mui/icons-material/Radio";
import Settings from "@mui/icons-material/Settings";
import VpnKey from "@mui/icons-material/VpnKey";
import Box from "@mui/material/Box";
import Drawer from "@mui/material/Drawer";
import List from "@mui/material/List";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import { useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "@/hooks/use-auth";

const DRAWER_WIDTH = 260;

export function Sidebar() {
  const { t } = useTranslation("nav");
  const navigate = useNavigate();
  const location = useLocation();
  const { user } = useAuth();

  const navItems = [
    { to: "/", icon: Dashboard, label: t("dashboard") },
    { to: "/stations", icon: Radio, label: t("stations") },
    { to: "/songs", icon: LibraryMusic, label: t("music") },
    { to: "/playlists", icon: PlaylistPlay, label: t("playlists") },
    { to: "/api-keys", icon: VpnKey, label: t("api_keys") },
    { to: "/users", icon: People, label: t("users") },
  ];
  const isAdmin = user?.role === "admin";

  return (
    <Drawer
      variant="permanent"
      sx={{
        width: DRAWER_WIDTH,
        flexShrink: 0,
        "& .MuiDrawer-paper": {
          width: DRAWER_WIDTH,
          boxSizing: "border-box",
          borderRight: 1,
          borderColor: "divider",
        },
      }}
    >
      <Box sx={{ p: 3.5, display: "flex", alignItems: "center", gap: 1.5 }}>
        <Box
          component="img"
          src="/sur_logo.png"
          alt={t("brand_full")}
          sx={{ width: 52, height: 52, borderRadius: 1.5 }}
        />
        <Typography variant="h4" sx={{ fontWeight: 800, letterSpacing: "-0.03em", lineHeight: 1.2 }}>
          <Box component="span" sx={{ color: "primary.main" }}>
            {t("brand_sur")}
          </Box>
          {t("brand_cast")}
        </Typography>
      </Box>

      <List sx={{ px: 1.5, mt: 1 }}>
        {navItems.map(({ to, icon: Icon, label }) => {
          const active = to === "/" ? location.pathname === "/" : location.pathname.startsWith(to);
          return (
            <ListItemButton
              key={to}
              onClick={() => navigate(to)}
              selected={active}
              aria-current={active ? "page" : undefined}
              sx={{
                borderRadius: 3,
                mb: 0.5,
                py: 1.25,
                "&:hover": {
                  bgcolor: "action.hover",
                },
              }}
            >
              <ListItemIcon sx={{ minWidth: 40 }}>
                <Icon fontSize="small" />
              </ListItemIcon>
              <ListItemText
                primary={label}
                slotProps={{
                  primary: { sx: { fontSize: 14, fontWeight: 500 } },
                }}
              />
            </ListItemButton>
          );
        })}
        {isAdmin && (
          <>
            <Typography
              variant="caption"
              sx={{
                px: 1,
                pt: 2,
                pb: 0.5,
                display: "block",
                color: "text.disabled",
                fontWeight: 600,
                letterSpacing: 0.5,
              }}
            >
              {t("admin")}
            </Typography>
            <ListItemButton
              onClick={() => navigate("/admin/icecast")}
              selected={location.pathname.startsWith("/admin/icecast")}
              sx={{ borderRadius: 3, mb: 0.5, py: 1.25 }}
            >
              <ListItemIcon sx={{ minWidth: 40 }}>
                <Settings fontSize="small" />
              </ListItemIcon>
              <ListItemText primary={t("icecast")} slotProps={{ primary: { sx: { fontSize: 14, fontWeight: 500 } } }} />
            </ListItemButton>
          </>
        )}
      </List>
    </Drawer>
  );
}
