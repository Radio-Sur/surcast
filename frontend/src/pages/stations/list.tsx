import Add from "@mui/icons-material/Add";
import Delete from "@mui/icons-material/Delete";
import Edit from "@mui/icons-material/Edit";
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
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Typography from "@mui/material/Typography";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { CreateStationDialog } from "@/components/create-station-dialog";
import { useDeleteStation, useStations } from "@/hooks/use-stations";
import { useSnackbar } from "@/providers/snackbar-provider";

export function StationsListPage() {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const { data: stations, isLoading, isError, error } = useStations();
  const deleteStation = useDeleteStation();
  const navigate = useNavigate();
  const [deleteId, setDeleteId] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);

  const stationToDelete = deleteId ? stations?.find((s) => s.id === deleteId) : null;

  const handleDelete = async () => {
    if (deleteId) {
      try {
        await deleteStation.mutateAsync(deleteId);
        setDeleteId(null);
      } catch (err) {
        console.error("Failed to delete station", err);
        showSnackbar("Failed to delete station", "error");
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
        <Alert severity="error">{error instanceof Error ? error.message : "Failed to load stations"}</Alert>
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
          <Typography variant="h4">{t("stations:title")}</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            {t("stations:subtitle")}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<Add />} onClick={() => setCreateOpen(true)}>
          {t("stations:add")}
        </Button>
      </Box>

      <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 3 }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell sx={{ fontWeight: 600, pl: 6 }}>{t("stations:table_name")}</TableCell>
              <TableCell sx={{ fontWeight: 600 }}>{t("stations:table_description")}</TableCell>
              <TableCell sx={{ fontWeight: 600 }}>{t("stations:table_stream_url")}</TableCell>
              <TableCell sx={{ fontWeight: 600, width: 100, pr: 6 }}>{t("stations:table_actions")}</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {stations && stations.length > 0 ? (
              stations.map((station) => (
                <TableRow
                  key={station.id}
                  hover
                  tabIndex={0}
                  sx={{ cursor: "pointer" }}
                  onClick={() => navigate(`/stations/${station.slug || station.id}`)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      navigate(`/stations/${station.slug || station.id}`);
                    }
                  }}
                >
                  <TableCell sx={{ fontWeight: 500, pl: 6 }}>{station.name}</TableCell>
                  <TableCell
                    sx={{
                      color: "text.secondary",
                      maxWidth: 300,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {station.description || "\u2014"}
                  </TableCell>
                  <TableCell sx={{ color: "text.secondary" }}>{station.stream_url || "\u2014"}</TableCell>
                  <TableCell sx={{ pr: 6 }}>
                    <Box sx={{ display: "flex", gap: 0.5 }}>
                      <IconButton
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          navigate(`/stations/${station.slug || station.id}/edit`);
                        }}
                      >
                        <Edit fontSize="small" />
                      </IconButton>
                      <IconButton
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          setDeleteId(station.id);
                        }}
                        color="error"
                      >
                        <Delete fontSize="small" />
                      </IconButton>
                    </Box>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 6, color: "text.secondary", pl: 6, pr: 6 }}>
                  {t("stations:empty")}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>

      <CreateStationDialog open={createOpen} onClose={() => setCreateOpen(false)} />

      <Dialog open={!!deleteId} onClose={() => setDeleteId(null)} slotProps={{ paper: { sx: { borderRadius: 3 } } }}>
        <DialogTitle>{t("stations:delete_title")}</DialogTitle>
        <DialogContent>
          <DialogContentText>{t("stations:delete_message", { name: stationToDelete?.name ?? "" })}</DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setDeleteId(null)}>{t("common:cancel")}</Button>
          <Button onClick={handleDelete} variant="contained" color="error" disabled={deleteStation.isPending}>
            {deleteStation.isPending ? t("common:deleting") : t("common:delete")}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
