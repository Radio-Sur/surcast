import Chip from "@mui/material/Chip";

export function RoleChip({ roleName }: { roleName: string }) {
  const color = roleName === "admin" ? "error" : "warning";
  const label = roleName.charAt(0).toUpperCase() + roleName.slice(1);
  return <Chip label={label} color={color} size="small" variant="outlined" />;
}
