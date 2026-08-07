import MoreVert from "@mui/icons-material/MoreVert";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import Paper from "@mui/material/Paper";
import Skeleton from "@mui/material/Skeleton";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { RoleChip } from "@/components/role-chip";
import { useAuth } from "@/hooks/use-auth";
import { useDeleteUser, useUpdateUser, useUsers } from "@/hooks/use-users";
import { useSnackbar } from "@/providers/snackbar-provider";

export function UsersPage() {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const { data: users, isLoading, isError, error } = useUsers();
  const updateUser = useUpdateUser();
  const deleteUser = useDeleteUser();
  const { user: currentUser } = useAuth();
  const isAdmin = currentUser?.role === "admin";

  const [menuAnchor, setMenuAnchor] = useState<null | HTMLElement>(null);
  const [menuUser, setMenuUser] = useState<string | null>(null);
  const [editUser, setEditUser] = useState<string | null>(null);
  const [editName, setEditName] = useState("");
  const [editRole, setEditRole] = useState("");
  const [deleteId, setDeleteId] = useState<string | null>(null);

  const handleMenuOpen = (e: React.MouseEvent<HTMLElement>, userId: string) => {
    setMenuAnchor(e.currentTarget);
    setMenuUser(userId);
  };

  const handleMenuClose = () => {
    setMenuAnchor(null);
    setMenuUser(null);
  };

  const handleEditOpen = (userId: string) => {
    const u = users?.find((u) => u.id === userId);
    if (u) {
      setEditName(u.name);
      setEditRole(u.role);
      setEditUser(userId);
    }
    handleMenuClose();
  };

  const handleEditSave = async () => {
    if (!editUser) return;
    try {
      await updateUser.mutateAsync({ id: editUser, data: { name: editName, role: editRole } });
      setEditUser(null);
    } catch (err) {
      console.error("Failed to update user", err);
      showSnackbar("Failed to update user", "error");
    }
  };

  const handleDelete = async () => {
    if (deleteId) {
      try {
        await deleteUser.mutateAsync(deleteId);
        setDeleteId(null);
      } catch (err) {
        console.error("Failed to delete user", err);
        showSnackbar("Failed to delete user", "error");
      }
    }
  };

  if (isLoading) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Skeleton variant="text" width={200} height={40} />
        <Skeleton variant="rounded" height={300} />
      </Box>
    );
  }

  if (isError) {
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <Alert severity="error">{error instanceof Error ? error.message : "Failed to load users"}</Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <Box>
        <Typography variant="h4">{t("users:title")}</Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
          {t("users:subtitle")}
        </Typography>
      </Box>

      <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 3 }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 600, pl: 6 }}>{t("users:table_username")}</TableCell>
              <TableCell sx={{ fontWeight: 600 }}>{t("users:table_name")}</TableCell>
              <TableCell sx={{ fontWeight: 600 }}>{t("users:table_role")}</TableCell>
              <TableCell sx={{ fontWeight: 600 }}>{t("users:table_created")}</TableCell>
              {isAdmin && <TableCell sx={{ fontWeight: 600, width: 60, pr: 6 }} />}
            </TableRow>
          </TableHead>
          <TableBody>
            {users && users.length > 0 ? (
              users.map((user) => (
                <TableRow key={user.id} hover>
                  <TableCell sx={{ fontWeight: 500, pl: 6 }}>{user.username}</TableCell>
                  <TableCell>{user.name}</TableCell>
                  <TableCell>
                    <RoleChip roleName={user.role} />
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" color="text.secondary">
                      {new Date(user.created_at).toLocaleDateString()}
                    </Typography>
                  </TableCell>
                  {isAdmin && (
                    <TableCell sx={{ pr: 6 }}>
                      <IconButton size="small" onClick={(e) => handleMenuOpen(e, user.id)}>
                        <MoreVert fontSize="small" />
                      </IconButton>
                    </TableCell>
                  )}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={isAdmin ? 5 : 4}
                  align="center"
                  sx={{ py: 6, color: "text.secondary", pl: 6, pr: 6 }}
                >
                  {t("users:empty")}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>

      <Menu anchorEl={menuAnchor} open={!!menuAnchor} onClose={handleMenuClose}>
        <MenuItem onClick={() => menuUser && handleEditOpen(menuUser)}>{t("users:edit")}</MenuItem>
        <MenuItem
          onClick={() => {
            setDeleteId(menuUser);
            handleMenuClose();
          }}
          sx={{ color: "error.main" }}
        >
          {t("users:delete")}
        </MenuItem>
      </Menu>

      <Dialog open={!!editUser} onClose={() => setEditUser(null)}>
        <DialogTitle>{t("users:edit_title")}</DialogTitle>
        <DialogContent>
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2, mt: 1 }}>
            <TextField
              label={t("users:name_label")}
              value={editName}
              onChange={(e) => setEditName(e.target.value)}
              fullWidth
            />
            <TextField
              label={t("users:role_label")}
              value={editRole}
              onChange={(e) => setEditRole(e.target.value)}
              select
              fullWidth
              slotProps={{ select: { native: true } }}
            >
              <option value="admin">{t("users:role_admin")}</option>
              <option value="manager">{t("users:role_manager")}</option>
              <option value="viewer">{t("users:role_viewer")}</option>
            </TextField>
          </Box>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setEditUser(null)}>{t("common:cancel")}</Button>
          <Button onClick={handleEditSave} variant="contained" disabled={updateUser.isPending}>
            {updateUser.isPending ? t("common:saving") : t("common:save")}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={!!deleteId} onClose={() => setDeleteId(null)}>
        <DialogTitle>{t("users:delete_title")}</DialogTitle>
        <DialogContent>
          <DialogContentText>{t("users:delete_message")}</DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setDeleteId(null)}>{t("common:cancel")}</Button>
          <Button onClick={handleDelete} variant="contained" color="error" disabled={deleteUser.isPending}>
            {deleteUser.isPending ? t("common:deleting") : t("common:delete")}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
