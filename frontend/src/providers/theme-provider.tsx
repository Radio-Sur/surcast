import CssBaseline from "@mui/material/CssBaseline";
import { createTheme, ThemeProvider as MuiThemeProvider } from "@mui/material/styles";
import useMediaQuery from "@mui/material/useMediaQuery";
import { createContext, type ReactNode, useContext, useEffect, useMemo, useState } from "react";
import { STORAGE_KEYS } from "@/lib/constants";

function hexToRgb(hex: string) {
  const c = hex.replace("#", "");
  return {
    r: parseInt(c.substring(0, 2), 16),
    g: parseInt(c.substring(2, 4), 16),
    b: parseInt(c.substring(4, 6), 16),
  };
}

function tintBg(hex: string, lighten: number): string {
  const { r, g, b } = hexToRgb(hex);
  const tr = Math.round(r + (255 - r) * lighten);
  const tg = Math.round(g + (255 - g) * lighten);
  const tb = Math.round(b + (255 - b) * lighten);
  return `#${tr.toString(16).padStart(2, "0")}${tg.toString(16).padStart(2, "0")}${tb.toString(16).padStart(2, "0")}`;
}

function shadeBg(hex: string, darken: number): string {
  const { r, g, b } = hexToRgb(hex);
  return `rgb(${Math.round(r * darken)}, ${Math.round(g * darken)}, ${Math.round(b * darken)})`;
}

const accentM3 = {
  blue: {
    light: { primary: "#5B7FFF", secondary: "#4DB6AC", tertiary: "#FF8A65" },
    dark: { primary: "#8FA3FF", secondary: "#80CBC4", tertiary: "#FFAB91" },
  },
  green: {
    light: { primary: "#66BB6A", secondary: "#5C9EFF", tertiary: "#FFB74D" },
    dark: { primary: "#81C784", secondary: "#90CAF9", tertiary: "#FFCC80" },
  },
  purple: {
    light: { primary: "#AB47BC", secondary: "#F06292", tertiary: "#4DD0E1" },
    dark: { primary: "#CE93D8", secondary: "#F48FB1", tertiary: "#80DEEA" },
  },
  orange: {
    light: { primary: "#FF8A65", secondary: "#CE93D8", tertiary: "#81C784" },
    dark: { primary: "#FFAB91", secondary: "#DDB892", tertiary: "#A5D6A7" },
  },
  rose: {
    light: { primary: "#E57373", secondary: "#9575CD", tertiary: "#FFB74D" },
    dark: { primary: "#EF9A9A", secondary: "#B39DDB", tertiary: "#FFCC80" },
  },
} as const;

export const accentOptions = {
  blue: { label: "Electric" },
  green: { label: "Emerald" },
  purple: { label: "Cosmic" },
  orange: { label: "Tangerine" },
  rose: { label: "Crimson" },
} as const;

export type AccentKey = keyof typeof accentOptions;

type ThemeMode = "light" | "dark" | "system";

interface ThemeContextType {
  mode: ThemeMode;
  accent: AccentKey;
  resolvedTheme: "light" | "dark";
  setMode: (mode: ThemeMode) => void;
  setAccent: (accent: AccentKey) => void;
}

const ThemeContext = createContext<ThemeContextType | null>(null);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const prefersDark = useMediaQuery("(prefers-color-scheme: dark)");

  const [mode, setMode] = useState<ThemeMode>(() => {
    const stored = localStorage.getItem(STORAGE_KEYS.THEME);
    if (stored === "dark" || stored === "light" || stored === "system") return stored;
    return "system";
  });

  const [accent, setAccent] = useState<AccentKey>(() => {
    const stored = localStorage.getItem(STORAGE_KEYS.ACCENT);
    return stored && stored in accentOptions ? (stored as AccentKey) : "blue";
  });

  const resolvedTheme = mode === "system" ? (prefersDark ? "dark" : "light") : mode;

  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.THEME, mode);
  }, [mode]);
  useEffect(() => {
    localStorage.setItem(STORAGE_KEYS.ACCENT, accent);
  }, [accent]);

  const muiTheme = useMemo(() => {
    const colors = accentM3[accent];
    const scheme = colors[resolvedTheme];
    const { r, g, b } = hexToRgb(scheme.primary);

    const bgLight = tintBg(scheme.primary, 0.94);
    const paperLight = tintBg(scheme.primary, 0.97);
    const bgDark = shadeBg(scheme.primary, 0.08);
    const paperDark = shadeBg(scheme.primary, 0.12);

    return createTheme({
      palette: {
        mode: resolvedTheme,
        primary: { main: scheme.primary },
        secondary: { main: scheme.secondary },
        ...(resolvedTheme === "light"
          ? {
              background: { default: bgLight, paper: paperLight },
              divider: `rgba(${r}, ${g}, ${b}, 0.12)`,
              text: { primary: "#1a1a2e", secondary: `rgba(${r}, ${g}, ${b}, 0.65)` },
            }
          : {
              background: { default: bgDark, paper: paperDark },
              divider: `rgba(${r}, ${g}, ${b}, 0.15)`,
              text: { primary: "#e8e8f0", secondary: "rgba(255, 255, 255, 0.6)" },
            }),
      },
      shape: { borderRadius: 16 },
      typography: {
        fontFamily: '"Roboto","Helvetica","Arial",sans-serif',
        h4: { fontWeight: 700, letterSpacing: "-0.02em" },
        h5: { fontWeight: 700, letterSpacing: "-0.01em" },
        h6: { fontWeight: 600 },
        body1: { lineHeight: 1.6 },
      },
      components: {
        MuiCssBaseline: {
          styleOverrides: {
            body: {
              backgroundImage: `radial-gradient(ellipse at 50% 0%, rgba(${r}, ${g}, ${b}, 0.04) 0%, transparent 60%)`,
              backgroundAttachment: "fixed",
            },
          },
        },
        MuiButton: {
          styleOverrides: {
            root: {
              textTransform: "none",
              borderRadius: 28,
              fontWeight: 600,
              padding: "8px 24px",
              boxShadow: "none",
            },
            contained: {
              boxShadow: "none",
              "&:hover": { boxShadow: "none" },
            },
            outlined: {
              borderColor: `rgba(${r}, ${g}, ${b}, 0.3)`,
              color: scheme.primary,
              "&:hover": {
                borderColor: scheme.primary,
                bgcolor: `rgba(${r}, ${g}, ${b}, 0.08)`,
              },
            },
            text: {
              color: scheme.primary,
              "&:hover": { bgcolor: `rgba(${r}, ${g}, ${b}, 0.08)` },
            },
          },
        },
        MuiCard: {
          styleOverrides: {
            root: {
              borderRadius: 20,
              backgroundImage: "none",
              border: `1px solid rgba(${r}, ${g}, ${b}, 0.08)`,
              boxShadow: "none",
            },
          },
        },
        MuiPaper: {
          styleOverrides: {
            root: {
              backgroundImage: "none",
            },
          },
        },
        MuiDialog: {
          styleOverrides: {
            paper: { borderRadius: 24, border: `1px solid rgba(${r}, ${g}, ${b}, 0.1)` },
          },
        },
        MuiTextField: {
          styleOverrides: {
            root: {
              "& .MuiOutlinedInput-root": {
                borderRadius: 14,
                transition: "all 0.2s ease-in-out",
                bgcolor: resolvedTheme === "light" ? "rgba(255,255,255,0.8)" : "rgba(0,0,0,0.2)",
                "& .MuiOutlinedInput-notchedOutline": {
                  borderColor: `rgba(${r}, ${g}, ${b}, 0.2)`,
                },
                "&:hover .MuiOutlinedInput-notchedOutline": {
                  borderColor: `rgba(${r}, ${g}, ${b}, 0.4)`,
                },
                "&.Mui-focused": {
                  "& .MuiOutlinedInput-notchedOutline": {
                    borderColor: scheme.primary,
                    borderWidth: 2,
                  },
                },
              },
              "& .MuiInputLabel-root.Mui-focused": {
                color: scheme.primary,
              },
            },
          },
        },
        MuiChip: {
          styleOverrides: {
            root: { borderRadius: 8, fontWeight: 500 },
            filled: { bgcolor: `rgba(${r}, ${g}, ${b}, 0.1)`, color: scheme.primary },
          },
        },
        MuiTableRow: {
          styleOverrides: {
            root: {
              "&:last-child td": { borderBottom: "none" },
              transition: "background-color 0.15s ease-in-out",
              "&:hover": { bgcolor: `rgba(${r}, ${g}, ${b}, 0.04)` },
            },
          },
        },
        MuiTableCell: {
          styleOverrides: {
            root: {
              padding: "14px 20px",
            },
            head: {
              fontWeight: 600,
              color: `rgba(${r}, ${g}, ${b}, 0.8)`,
            },
          },
        },
        MuiSwitch: {
          styleOverrides: {
            root: {
              "& .MuiSwitch-switchBase.Mui-checked": {
                color: scheme.primary,
                "&:hover": { bgcolor: `rgba(${r}, ${g}, ${b}, 0.08)` },
              },
              "& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track": {
                bgcolor: scheme.primary,
                opacity: 0.4,
              },
            },
          },
        },
        MuiAlert: {
          styleOverrides: {
            root: { borderRadius: 12 },
          },
        },
        MuiTooltip: {
          styleOverrides: {
            tooltip: { borderRadius: 8, fontWeight: 500 },
          },
        },
        MuiTab: {
          styleOverrides: {
            root: {
              textTransform: "none",
              fontWeight: 600,
              "&.Mui-selected": { color: scheme.primary },
            },
          },
        },
        MuiTabs: {
          styleOverrides: {
            indicator: { bgcolor: scheme.primary },
          },
        },
      },
    });
  }, [accent, resolvedTheme]);

  return (
    <ThemeContext.Provider value={{ mode, accent, resolvedTheme, setMode, setAccent }}>
      <MuiThemeProvider theme={muiTheme}>
        <CssBaseline />
        {children}
      </MuiThemeProvider>
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used within ThemeProvider");
  return ctx;
}
