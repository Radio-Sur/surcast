import Dashboard from "@mui/icons-material/Dashboard";
import Folder from "@mui/icons-material/Folder";
import LibraryMusic from "@mui/icons-material/LibraryMusic";
import People from "@mui/icons-material/People";
import PlaylistPlay from "@mui/icons-material/PlaylistPlay";
import Radio from "@mui/icons-material/Radio";
import Settings from "@mui/icons-material/Settings";
import VpnKey from "@mui/icons-material/VpnKey";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import Drawer from "@mui/material/Drawer";
import List from "@mui/material/List";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Typography from "@mui/material/Typography";
import { useTranslation } from "react-i18next";
import { useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "@/hooks/use-auth";
import { useSnackbar } from "@/providers/snackbar-provider";

const DRAWER_WIDTH = 260;

export function Sidebar() {
  const { t } = useTranslation("nav");
  const navigate = useNavigate();
  const location = useLocation();
  const { user } = useAuth();
  const { showError } = useSnackbar();
  const isDemo = user?.role === "demo";

  const navItems = [
    { to: "/", icon: Dashboard, label: t("dashboard") },
    { to: "/stations", icon: Radio, label: t("stations") },
    { to: "/songs", icon: LibraryMusic, label: t("music") },
    { to: "/files", icon: Folder, label: t("files") },
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
      <Box sx={{ p: isDemo ? 2 : 3.5, display: "flex", alignItems: "center", gap: isDemo ? 1 : 1.5 }}>
        <Box
          component="img"
          src="/sur_logo.png"
          alt={t("brand_full")}
          sx={{ width: isDemo ? 36 : 52, height: isDemo ? 36 : 52, borderRadius: 1.5 }}
        />
        <Typography
          variant="h4"
          sx={{ fontWeight: 800, letterSpacing: "-0.03em", lineHeight: 1.2, ...(isDemo && { fontSize: 26 }) }}
        >
          <Box component="span" sx={{ color: "primary.main" }}>
            {t("brand_sur")}
          </Box>
          {t("brand_cast")}
        </Typography>
        {user?.role === "demo" && <Chip label="DEMO" size="small" color="warning" variant="outlined" />}
      </Box>

      <List sx={{ px: 1.5, mt: 1 }}>
        {navItems.map(({ to, icon: Icon, label }) => {
          const active = to === "/" ? location.pathname === "/" : location.pathname.startsWith(to);
          return (
            <ListItemButton
              key={to}
              onClick={() => {
                if (isDemo && (to === "/users" || to === "/api-keys")) {
                  showError({ title: t("demo_unavailable") });
                  return;
                }
                navigate(to);
              }}
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
        {(isAdmin || isDemo) && (
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
