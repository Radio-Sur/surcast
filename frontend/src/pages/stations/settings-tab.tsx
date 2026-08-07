import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Slider from "@mui/material/Slider";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import Typography from "@mui/material/Typography";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { BufferingInfoTable } from "./buffering-info-table";
import { StreamUrlCard } from "./stream-url-card";

export type TransitionMode = "crossfade" | "autocue" | "off";

export function SettingsTab({
  prebufferBytes,
  playedLimit,
  defaultFadeMs,
  transitionMode,
  autocueFadeMaxMs,
  streamUrl,
  updateStation,
  updatePending,
}: {
  prebufferBytes: number;
  playedLimit: number;
  defaultFadeMs: number;
  transitionMode: TransitionMode;
  autocueFadeMaxMs: number;
  streamUrl: string;
  updateStation: (data: {
    prebuffer_bytes?: number;
    played_limit?: number;
    default_fade_ms?: number;
    transition_mode?: TransitionMode;
    autocue_fade_max_ms?: number;
  }) => void;
  updatePending: boolean;
}) {
  const { t } = useTranslation();
  const [prebufferVal, setPrebufferVal] = useState(prebufferBytes);
  const [playedLimitVal, setPlayedLimitVal] = useState(playedLimit);
  const [fadeVal, setFadeVal] = useState(defaultFadeMs);
  const [modeVal, setModeVal] = useState<TransitionMode>(transitionMode);
  const [autocueFadeVal, setAutocueFadeVal] = useState(autocueFadeMaxMs);
  useEffect(() => {
    setPrebufferVal(prebufferBytes);
  }, [prebufferBytes]);

  useEffect(() => {
    setPlayedLimitVal(playedLimit);
  }, [playedLimit]);

  useEffect(() => {
    setFadeVal(defaultFadeMs);
  }, [defaultFadeMs]);

  useEffect(() => {
    setModeVal(transitionMode);
  }, [transitionMode]);

  useEffect(() => {
    setAutocueFadeVal(autocueFadeMaxMs);
  }, [autocueFadeMaxMs]);

  const handleSave = () => {
    const payload: {
      prebuffer_bytes?: number;
      played_limit?: number;
      default_fade_ms?: number;
      transition_mode?: TransitionMode;
      autocue_fade_max_ms?: number;
    } = {
      prebuffer_bytes: prebufferVal,
      played_limit: playedLimitVal,
      default_fade_ms: modeVal === "crossfade" ? fadeVal : 0,
      transition_mode: modeVal,
    };
    if (modeVal === "autocue") {
      payload.autocue_fade_max_ms = autocueFadeVal;
    }
    updateStation(payload);
  };

  const showFadeSlider = modeVal === "crossfade";
  const showAutocueFadeSlider = modeVal === "autocue";

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <StreamUrlCard streamUrl={streamUrl} />

      <Card variant="outlined" sx={{ borderRadius: 3 }}>
        <CardContent sx={{ p: 4, "&:last-child": { pb: 4 } }}>
          <Typography variant="h6" sx={{ mb: 3 }}>
            {t("stations:stream_settings")}
          </Typography>

          <Box sx={{ maxWidth: 400 }}>
            <Typography gutterBottom>
              {t("stations:prebuffer")}:{" "}
              <strong>{t("stations:prebuffer_value", { value: prebufferVal.toLocaleString() })}</strong>
            </Typography>
            <Slider
              value={prebufferVal}
              onChange={(_, v) => setPrebufferVal(v as number)}
              min={1024}
              max={131072}
              step={1024}
              valueLabelDisplay="auto"
              valueLabelFormat={(v) => t("stations:prebuffer_kb", { value: (v / 1024).toFixed(0) })}
            />
            <Box sx={{ display: "flex", justifyContent: "space-between", mt: 1 }}>
              <Typography variant="caption" color="text.secondary">
                {t("stations:prebuffer_1kb")}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {t("stations:prebuffer_128kb")}
              </Typography>
            </Box>

            <BufferingInfoTable prebufferBytes={prebufferVal} />
          </Box>

          <Box sx={{ mt: 3 }}>
            <Typography gutterBottom>
              {t("stations:played_history")}:{" "}
              <strong>
                {playedLimitVal === 0
                  ? t("stations:played_limit_unlimited")
                  : t("stations:played_limit_value", { count: playedLimitVal })}
              </strong>
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: "block" }}>
              {t("stations:prebuffer_helper")}
            </Typography>
            <Slider
              value={playedLimitVal}
              onChange={(_, v) => setPlayedLimitVal(v as number)}
              min={0}
              max={500}
              step={10}
              valueLabelDisplay="auto"
              valueLabelFormat={(v) => (v === 0 ? "\u221E" : `${v}`)}
            />
            <Box sx={{ display: "flex", justifyContent: "space-between", mt: 1 }}>
              <Typography variant="caption" color="text.secondary">
                {t("stations:played_limit_0")}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {t("stations:played_limit_500")}
              </Typography>
            </Box>
          </Box>

          <Box sx={{ mt: 3 }}>
            <Typography gutterBottom>{t("stations:transition_mode")}</Typography>
            <ToggleButtonGroup
              exclusive
              value={modeVal}
              onChange={(_, v: TransitionMode | null) => {
                if (v) setModeVal(v);
              }}
              size="small"
            >
              <ToggleButton value="crossfade">{t("stations:transition_crossfade")}</ToggleButton>
              <ToggleButton value="autocue">{t("stations:transition_autocue")}</ToggleButton>
              <ToggleButton value="off">{t("common:off")}</ToggleButton>
            </ToggleButtonGroup>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: "block" }}>
              {t("stations:transition_mode_helper")}
            </Typography>
          </Box>

          {showFadeSlider && (
            <Box sx={{ mt: 3 }}>
              <Typography gutterBottom>
                {t("stations:crossfade")}:{" "}
                <strong>
                  {fadeVal === 0
                    ? t("common:off")
                    : t("stations:crossfade_value", { value: (fadeVal / 1000).toFixed(1) })}
                </strong>
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: "block" }}>
                {t("stations:crossfade_helper")}
              </Typography>
              <Slider
                value={fadeVal}
                onChange={(_, v) => setFadeVal(v as number)}
                min={0}
                max={15000}
                step={500}
                valueLabelDisplay="auto"
                valueLabelFormat={(v) =>
                  v === 0 ? t("common:off") : t("stations:crossfade_label", { value: (v / 1000).toFixed(1) })
                }
              />
              <Box sx={{ display: "flex", justifyContent: "space-between", mt: 1 }}>
                <Typography variant="caption" color="text.secondary">
                  {t("common:off")}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {t("stations:crossfade_15s")}
                </Typography>
              </Box>
            </Box>
          )}

          {showAutocueFadeSlider && (
            <Box sx={{ mt: 3 }}>
              <Typography gutterBottom>
                {t("stations:autocue_fade")}:{" "}
                <strong>
                  {autocueFadeVal === 0
                    ? t("common:off")
                    : t("stations:crossfade_value", { value: (autocueFadeVal / 1000).toFixed(1) })}
                </strong>
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ mb: 1, display: "block" }}>
                {t("stations:autocue_fade_helper")}
              </Typography>
              <Slider
                value={autocueFadeVal}
                onChange={(_, v) => setAutocueFadeVal(v as number)}
                min={0}
                max={15000}
                step={500}
                valueLabelDisplay="auto"
                valueLabelFormat={(v) =>
                  v === 0 ? t("common:off") : t("stations:crossfade_label", { value: (v / 1000).toFixed(1) })
                }
              />
              <Box sx={{ display: "flex", justifyContent: "space-between", mt: 1 }}>
                <Typography variant="caption" color="text.secondary">
                  {t("common:off")}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {t("stations:crossfade_15s")}
                </Typography>
              </Box>
            </Box>
          )}

          <Box sx={{ mt: 2, display: "flex", gap: 1 }}>
            <Button
              variant="contained"
              disabled={
                (prebufferVal === prebufferBytes &&
                  playedLimitVal === playedLimit &&
                  fadeVal === defaultFadeMs &&
                  modeVal === transitionMode &&
                  autocueFadeVal === autocueFadeMaxMs) ||
                updatePending
              }
              onClick={handleSave}
            >
              {updatePending ? t("common:saving") : t("common:save")}
            </Button>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
