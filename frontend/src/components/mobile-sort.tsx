import ArrowDownward from "@mui/icons-material/ArrowDownward";
import ArrowUpward from "@mui/icons-material/ArrowUpward";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";
import MenuItem from "@mui/material/MenuItem";
import TextField from "@mui/material/TextField";
import { useTranslation } from "react-i18next";

export function MobileSort<K extends string>({
  value,
  order,
  options,
  onSort,
}: {
  value: K;
  order: "asc" | "desc";
  options: { value: K; label: string }[];
  onSort: (key: K) => void;
}) {
  const { t } = useTranslation("common");
  return (
    <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
      <TextField
        select
        size="small"
        label={t("sort_by")}
        value={value}
        onChange={(e) => onSort(e.target.value as K)}
        sx={{ flex: 1, minWidth: 0 }}
      >
        {options.map((option) => (
          <MenuItem key={option.value} value={option.value}>
            {option.label}
          </MenuItem>
        ))}
      </TextField>
      <IconButton aria-label={t(order === "asc" ? "sort_descending" : "sort_ascending")} onClick={() => onSort(value)}>
        {order === "asc" ? <ArrowUpward /> : <ArrowDownward />}
      </IconButton>
    </Box>
  );
}
