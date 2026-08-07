import Chip from "@mui/material/Chip";

export function RoleChip({ roleName }: { roleName: string }) {
  const color = roleName === "admin" ? "error" : roleName === "manager" ? "warning" : "default";
  return <Chip label={roleName} color={color} size="small" variant="outlined" />;
}
