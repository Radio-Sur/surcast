import {
  Box,
  Checkbox,
  FormControl,
  FormControlLabel,
  FormGroup,
  FormLabel,
  Radio,
  RadioGroup,
  TextField,
  Typography,
} from "@mui/material";
import { useTranslation } from "react-i18next";
import type { RecurrenceType } from "@/types";

interface RecurrencePickerProps {
  value: RecurrenceType;
  interval: number | null;
  days: number[] | null;
  endDate: string | null;
  count: number | null;
  onChange: (
    field: "recurrence_type" | "recurrence_interval" | "recurrence_days" | "recurrence_end_date" | "recurrence_count",
    value: RecurrenceType | number | number[] | string | null,
  ) => void;
}

const DAY_SHORT_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"] as const;

export function RecurrencePicker({ value, interval, days, endDate, count, onChange }: RecurrencePickerProps) {
  const { t } = useTranslation();
  return (
    <Box sx={{ mt: 2 }}>
      <FormControl component="fieldset">
        <FormLabel component="legend" sx={{ fontSize: "0.85rem", mb: 1 }}>
          {t("schedule:repeat")}
        </FormLabel>
        <RadioGroup value={value} onChange={(e) => onChange("recurrence_type", e.target.value as RecurrenceType)}>
          <FormControlLabel value="none" control={<Radio size="small" />} label={t("schedule:repeat_none")} />
          <FormControlLabel value="daily" control={<Radio size="small" />} label={t("schedule:repeat_daily")} />
          <FormControlLabel
            value="every_n_days"
            control={<Radio size="small" />}
            label={
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                {t("schedule:repeat_every")}
                <TextField
                  size="small"
                  type="number"
                  value={interval ?? 1}
                  onChange={(e) => onChange("recurrence_interval", parseInt(e.target.value, 10) || 1)}
                  onClick={(e) => e.stopPropagation()}
                  sx={{ width: 60 }}
                  slotProps={{ htmlInput: { min: 1, style: { textAlign: "center" } } }}
                />
                {t("schedule:repeat_days")}
              </Box>
            }
          />
          <FormControlLabel value="weekly" control={<Radio size="small" />} label={t("schedule:repeat_weekly")} />
          <FormControlLabel value="biweekly" control={<Radio size="small" />} label={t("schedule:repeat_biweekly")} />
          <FormControlLabel value="monthly" control={<Radio size="small" />} label={t("schedule:repeat_monthly")} />
          <FormControlLabel value="custom_days" control={<Radio size="small" />} label={t("schedule:repeat_custom")} />
        </RadioGroup>
      </FormControl>

      {value === "custom_days" && (
        <Box sx={{ ml: 3, mt: 1 }}>
          <FormGroup row>
            {DAY_SHORT_KEYS.map((key, idx) => (
              <FormControlLabel
                key={idx}
                control={
                  <Checkbox
                    size="small"
                    checked={days?.includes(idx) ?? false}
                    onChange={(e) => {
                      const current = days ?? [];
                      const next = e.target.checked ? [...current, idx].sort() : current.filter((d) => d !== idx);
                      onChange("recurrence_days", next);
                    }}
                  />
                }
                label={t(`schedule:day_${key}`)}
              />
            ))}
          </FormGroup>
        </Box>
      )}

      <Box sx={{ display: "flex", gap: 2, mt: 1.5, alignItems: "center" }}>
        <Typography variant="caption" color="text.secondary">
          {t("schedule:repeat_end")}
        </Typography>
        <FormControlLabel
          control={
            <Radio
              size="small"
              checked={!endDate && !count}
              onChange={() => {
                onChange("recurrence_end_date", null);
                onChange("recurrence_count", null);
              }}
            />
          }
          label={<Typography variant="caption">{t("schedule:repeat_end_never")}</Typography>}
        />
        <FormControlLabel
          control={<Radio size="small" checked={!!count} onChange={() => onChange("recurrence_count", count || 10)} />}
          label={
            <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
              <Typography variant="caption">{t("schedule:repeat_end_after")}</Typography>
              <TextField
                size="small"
                type="number"
                value={count ?? 10}
                onChange={(e) => onChange("recurrence_count", parseInt(e.target.value, 10) || 1)}
                onClick={(e) => e.stopPropagation()}
                sx={{ width: 60 }}
                slotProps={{ htmlInput: { min: 1, style: { textAlign: "center", fontSize: "0.8rem" } } }}
              />
              <Typography variant="caption">{t("schedule:repeat_end_occurrences")}</Typography>
            </Box>
          }
        />
        <FormControlLabel
          control={
            <Radio size="small" checked={!!endDate} onChange={() => onChange("recurrence_end_date", endDate || "")} />
          }
          label={
            <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
              <Typography variant="caption">{t("schedule:repeat_end_on_date")}</Typography>
              <TextField
                size="small"
                type="date"
                value={endDate || ""}
                onChange={(e) => onChange("recurrence_end_date", e.target.value || null)}
                onClick={(e) => e.stopPropagation()}
                sx={{ width: 150 }}
                slotProps={{ htmlInput: { style: { fontSize: "0.8rem" } } }}
              />
            </Box>
          }
        />
      </Box>
    </Box>
  );
}
