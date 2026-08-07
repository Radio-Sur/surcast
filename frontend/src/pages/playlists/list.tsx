import Add from "@mui/icons-material/Add";
import Delete from "@mui/icons-material/Delete";
import PlaylistPlay from "@mui/icons-material/PlaylistPlay";
import Visibility from "@mui/icons-material/Visibility";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
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
import { useNavigate } from "react-router-dom";
import { useCreatePlaylist, useDeletePlaylist, usePlaylists } from "@/hooks/use-playlists";
import { useSnackbar } from "@/providers/snackbar-provider";

export function PlaylistsListPage() {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const { data: playlists, isLoading, isError, error } = usePlaylists();
  const createPlaylist = useCreatePlaylist();
  const deletePlaylist = useDeletePlaylist();
  const navigate = useNavigate();
  const [createOpen, setCreateOpen] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [deleteId, setDeleteId] = useState<string | null>(null);

  const playlistToDelete = deleteId ? playlists?.find((p) => p.id === deleteId) : null;

  const handleCreate = async () => {
    if (!newName.trim()) return;
    try {
      await createPlaylist.mutateAsync({ name: newName.trim(), description: newDesc.trim() || undefined });
      setCreateOpen(false);
      setNewName("");
      setNewDesc("");
    } catch (err) {
      console.error("Failed to create playlist", err);
      showSnackbar("Failed to create playlist", "error");
    }
  };

  const handleDelete = async () => {
    if (deleteId) {
      try {
        await deletePlaylist.mutateAsync(deleteId);
        setDeleteId(null);
      } catch (err) {
        console.error("Failed to delete playlist", err);
        showSnackbar("Failed to delete playlist", "error");
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
        <Alert severity="error">{error instanceof Error ? error.message : "Failed to load playlists"}</Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <Box>
          <Typography variant="h4" sx={{ fontWeight: 700 }}>
            {t("playlists:title")}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            {t("playlists:subtitle")}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<Add />} onClick={() => setCreateOpen(true)}>
          {t("playlists:new")}
        </Button>
      </Box>

      {!playlists || playlists.length === 0 ? (
        <Paper variant="outlined" sx={{ p: 6, textAlign: "center", borderRadius: 3 }}>
          <PlaylistPlay sx={{ fontSize: 48, mb: 1, opacity: 0.3 }} />
          <Typography>{t("playlists:empty")}</Typography>
          <Typography variant="body2" color="text.secondary">
            {t("playlists:empty_hint")}
          </Typography>
        </Paper>
      ) : (
        <TableContainer
          component={Paper}
          variant="outlined"
          sx={{ borderRadius: 3, "& .MuiTableRow-root:hover": { bgcolor: "action.hover" } }}
        >
          <Table>
            <TableHead>
              <TableRow>
                <TableCell sx={{ pl: 4 }}>{t("playlists:table_name")}</TableCell>
                <TableCell>{t("playlists:table_description")}</TableCell>
                <TableCell align="center">{t("playlists:table_songs")}</TableCell>
                <TableCell align="right" sx={{ pr: 4 }}>
                  {t("playlists:table_actions")}
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {playlists.map((p) => (
                <TableRow
                  key={p.id}
                  tabIndex={0}
                  sx={{ cursor: "pointer", "&:hover": { bgcolor: "action.hover" } }}
                  onClick={() => navigate(`/playlists/${p.slug || p.id}`)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      navigate(`/playlists/${p.slug || p.id}`);
                    }
                  }}
                >
                  <TableCell sx={{ pl: 4, fontWeight: 600 }}>{p.name}</TableCell>
                  <TableCell
                    sx={{
                      color: "text.secondary",
                      maxWidth: 300,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {p.description || "—"}
                  </TableCell>
                  <TableCell align="center">{p.song_count}</TableCell>
                  <TableCell align="right" sx={{ pr: 4 }}>
                    <IconButton
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        navigate(`/playlists/${p.slug || p.id}`);
                      }}
                    >
                      <Visibility fontSize="small" />
                    </IconButton>
                    <IconButton
                      size="small"
                      color="error"
                      onClick={(e) => {
                        e.stopPropagation();
                        setDeleteId(p.id);
                      }}
                    >
                      <Delete fontSize="small" />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>{t("playlists:create_title")}</DialogTitle>
        <DialogContent sx={{ display: "flex", flexDirection: "column", gap: 2, pt: "16px !important" }}>
          <TextField
            label={t("playlists:name_label")}
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            fullWidth
            autoFocus
          />
          <TextField
            label={t("playlists:description_label")}
            value={newDesc}
            onChange={(e) => setNewDesc(e.target.value)}
            fullWidth
            multiline
            rows={2}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>{t("common:cancel")}</Button>
          <Button variant="contained" onClick={handleCreate} disabled={!newName.trim() || createPlaylist.isPending}>
            {createPlaylist.isPending ? t("common:creating") : t("common:create")}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={!!deleteId} onClose={() => setDeleteId(null)}>
        <DialogTitle>{t("playlists:delete_title")}</DialogTitle>
        <DialogContent>
          <Typography>{t("playlists:delete_message", { name: playlistToDelete?.name })}</Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteId(null)}>{t("common:cancel")}</Button>
          <Button color="error" variant="contained" onClick={handleDelete} disabled={deletePlaylist.isPending}>
            {deletePlaylist.isPending ? t("common:deleting") : t("common:delete")}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
