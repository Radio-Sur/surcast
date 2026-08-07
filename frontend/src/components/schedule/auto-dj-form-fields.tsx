import {
  Box,
  FormControl,
  FormControlLabel,
  InputLabel,
  MenuItem,
  Select,
  Slider,
  Switch,
  Typography,
} from "@mui/material";
import { useTranslation } from "react-i18next";

export function AutoDJFormFields({
  autoDjMode,
  autoDjAvoidRepeat,
  autoDjMinGap,
  autoDjSongsAhead,
  readOnly,
  onChange,
}: {
  autoDjMode: string | null;
  autoDjAvoidRepeat: boolean | null;
  autoDjMinGap: number | null;
  autoDjSongsAhead: number | null;
  readOnly: boolean;
  onChange: (field: string, value: boolean | string | number | null | undefined) => void;
}) {
  const { t } = useTranslation("schedule");
  return (
    <>
      <FormControl size="small">
        <InputLabel>{t("auto_dj_mode")}</InputLabel>
        <Select
          value={autoDjMode || ""}
          label={t("auto_dj_mode")}
          onChange={(e) => onChange("auto_dj_mode", e.target.value || null)}
          disabled={readOnly}
        >
          <MenuItem value="">{t("auto_dj_mode_disabled")}</MenuItem>
          <MenuItem value="random">{t("auto_dj_mode_random")}</MenuItem>
          <MenuItem value="sequential">{t("auto_dj_mode_sequential")}</MenuItem>
          <MenuItem value="reverse">{t("auto_dj_mode_reverse")}</MenuItem>
        </Select>
      </FormControl>
      {autoDjMode && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <FormControlLabel
            control={
              <Switch
                size="small"
                checked={autoDjAvoidRepeat ?? true}
                onChange={(e) => onChange("auto_dj_avoid_repeat", e.target.checked)}
                disabled={readOnly}
              />
            }
            label={t("auto_dj_avoid_repeat")}
          />
          <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            <Typography variant="caption">{t("auto_dj_min_gap")}</Typography>
            <Slider
              size="small"
              value={autoDjMinGap ?? 3}
              min={0}
              max={10}
              step={1}
              onChange={(_, v) => onChange("auto_dj_min_gap", v as number)}
              sx={{ flex: 1 }}
              disabled={readOnly}
            />
            <Typography variant="caption" sx={{ minWidth: 20 }}>
              {autoDjMinGap ?? 3}
            </Typography>
          </Box>
          <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            <Typography variant="caption">{t("auto_dj_songs_ahead")}</Typography>
            <Slider
              size="small"
              value={autoDjSongsAhead ?? 5}
              min={1}
              max={20}
              step={1}
              onChange={(_, v) => onChange("auto_dj_songs_ahead", v as number)}
              sx={{ flex: 1 }}
              disabled={readOnly}
            />
            <Typography variant="caption" sx={{ minWidth: 20 }}>
              {autoDjSongsAhead ?? 5}
            </Typography>
          </Box>
        </Box>
      )}
    </>
  );
}
