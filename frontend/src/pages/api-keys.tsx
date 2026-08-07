import Add from "@mui/icons-material/Add";
import ContentCopy from "@mui/icons-material/ContentCopy";
import Delete from "@mui/icons-material/Delete";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import Paper from "@mui/material/Paper";
import Skeleton from "@mui/material/Skeleton";
import Switch from "@mui/material/Switch";
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
import { useApiKeys, useCreateApiKey, useDeleteApiKey, useUpdateApiKey } from "@/hooks/use-api-keys";
import { useSnackbar } from "@/providers/snackbar-provider";

export function ApiKeysPage() {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const { data: keys, isLoading, isError, error } = useApiKeys();
  const createKey = useCreateApiKey();
  const updateKey = useUpdateApiKey();
  const deleteKey = useDeleteApiKey();
  const [createOpen, setCreateOpen] = useState(false);
  const [newName, setNewName] = useState("");
  const [expiresAt, setExpiresAt] = useState("");
  const [createdKey, setCreatedKey] = useState<string | null>(null);
  const [deleteId, setDeleteId] = useState<string | null>(null);

  const handleCreate = async () => {
    if (!newName.trim()) return;
    try {
      const result = await createKey.mutateAsync({
        name: newName.trim(),
        expires_at: expiresAt ? new Date(expiresAt).toISOString() : undefined,
      });
      setCreatedKey(result.key);
      setNewName("");
      setExpiresAt("");
      setCreateOpen(false);
    } catch (err) {
      console.error("Failed to create API key", err);
      showSnackbar("Failed to create API key", "error");
    }
  };

  const handleDelete = async () => {
    if (deleteId) {
      try {
        await deleteKey.mutateAsync(deleteId);
        setDeleteId(null);
      } catch (err) {
        console.error("Failed to delete API key", err);
        showSnackbar("Failed to delete API key", "error");
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
        <Alert severity="error">{error instanceof Error ? error.message : "Failed to load API keys"}</Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <Box>
          <Typography variant="h4">{t("api-keys:title")}</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            {t("api-keys:subtitle")}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<Add />} onClick={() => setCreateOpen(true)}>
          {t("api-keys:new")}
        </Button>
      </Box>

      {createdKey && (
        <Alert
          severity="success"
          onClose={() => setCreatedKey(null)}
          sx={{ alignItems: "flex-start", "& .MuiAlert-message": { wordBreak: "break-all" } }}
          action={
            <Button size="small" onClick={() => navigator.clipboard.writeText(createdKey)} startIcon={<ContentCopy />}>
              {t("common:copy")}
            </Button>
          }
        >
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
            {t("api-keys:created_alert")}
          </Typography>
          <Typography
            variant="body2"
            sx={{
              fontFamily: "monospace",
              bgcolor: "action.hover",
              px: 1,
              py: 0.5,
              borderRadius: 1,
            }}
          >
            {createdKey}
          </Typography>
        </Alert>
      )}

      <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 3 }}>
        <Table sx={{ tableLayout: "fixed" }}>
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 600, pl: 6, width: "28%" }}>{t("api-keys:table_name")}</TableCell>
              <TableCell sx={{ fontWeight: 600, width: "18%" }}>{t("api-keys:table_key")}</TableCell>
              <TableCell sx={{ fontWeight: 600, pl: 1, width: "17%" }}>{t("api-keys:table_status")}</TableCell>
              <TableCell sx={{ fontWeight: 600, width: "14%" }}>{t("api-keys:table_last_used")}</TableCell>
              <TableCell sx={{ fontWeight: 600, width: "14%" }}>{t("api-keys:table_expires")}</TableCell>
              <TableCell sx={{ fontWeight: 600, pr: 6, width: 60 }} />
            </TableRow>
          </TableHead>
          <TableBody>
            {keys && keys.length > 0 ? (
              keys.map((key) => (
                <TableRow key={key.id} hover>
                  <TableCell
                    sx={{
                      fontWeight: 500,
                      pl: 6,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                    }}
                  >
                    {key.name}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" sx={{ fontFamily: "monospace" }}>
                      {key.key_prefix}...
                    </Typography>
                  </TableCell>
                  <TableCell sx={{ pl: 1 }}>
                    <Box
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 1,
                        whiteSpace: "nowrap",
                      }}
                    >
                      {key.expires_at && new Date(key.expires_at) <= new Date() ? (
                        <>
                          <Box sx={{ width: 40 }} />
                          <Typography variant="body2" color="text.secondary">
                            {t("api-keys:status_expired")}
                          </Typography>
                        </>
                      ) : (
                        <>
                          <Switch
                            size="small"
                            checked={key.is_active}
                            onChange={() =>
                              updateKey.mutate({
                                id: key.id,
                                data: { is_active: !key.is_active },
                              })
                            }
                          />
                          <Typography variant="body2">
                            {key.is_active ? t("api-keys:status_active") : t("api-keys:status_inactive")}
                          </Typography>
                        </>
                      )}
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" color="text.secondary">
                      {key.last_used_at ? new Date(key.last_used_at).toLocaleDateString() : t("api-keys:never")}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" color="text.secondary">
                      {key.expires_at ? new Date(key.expires_at).toLocaleDateString() : t("api-keys:never")}
                    </Typography>
                  </TableCell>
                  <TableCell sx={{ pr: 6 }}>
                    <IconButton size="small" onClick={() => setDeleteId(key.id)} color="error">
                      <Delete fontSize="small" />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 6, color: "text.secondary", pl: 6, pr: 6 }}>
                  {t("api-keys:empty")}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>

      <Dialog
        open={createOpen}
        onClose={() => {
          setCreateOpen(false);
          setNewName("");
          setExpiresAt("");
        }}
      >
        <DialogTitle>{t("api-keys:create_title")}</DialogTitle>
        <DialogContent>
          <DialogContentText sx={{ mb: 2 }}>{t("api-keys:create_body")}</DialogContentText>
          <TextField
            autoFocus
            label={t("api-keys:name_label")}
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            placeholder={t("api-keys:name_placeholder")}
            fullWidth
            sx={{ mb: 2 }}
            onKeyDown={(e) => e.key === "Enter" && handleCreate()}
          />
          <TextField
            label={t("api-keys:expires_label")}
            type="date"
            value={expiresAt}
            onChange={(e) => setExpiresAt(e.target.value)}
            fullWidth
            helperText={t("api-keys:expires_helper")}
            slotProps={{
              inputLabel: { shrink: true },
              htmlInput: { min: new Date().toISOString().split("T")[0] },
            }}
          />
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button
            onClick={() => {
              setCreateOpen(false);
              setNewName("");
              setExpiresAt("");
            }}
          >
            {t("common:cancel")}
          </Button>
          <Button onClick={handleCreate} variant="contained" disabled={!newName.trim() || createKey.isPending}>
            {createKey.isPending ? t("common:creating") : t("common:create")}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={!!deleteId} onClose={() => setDeleteId(null)}>
        <DialogTitle>{t("api-keys:delete_title")}</DialogTitle>
        <DialogContent>
          <DialogContentText>{t("api-keys:delete_message")}</DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setDeleteId(null)}>{t("common:cancel")}</Button>
          <Button onClick={handleDelete} variant="contained" color="error" disabled={deleteKey.isPending}>
            {deleteKey.isPending ? t("common:deleting") : t("common:delete")}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
