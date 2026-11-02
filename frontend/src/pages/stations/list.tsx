import Add from "@mui/icons-material/Add";
import Delete from "@mui/icons-material/Delete";
import Edit from "@mui/icons-material/Edit";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
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
import { EditStationDialog } from "@/components/edit-station-dialog";
import { ErrorDetails } from "@/components/error-details";
import { MobileList } from "@/components/mobile-list";
import { PageHeader } from "@/components/page-header";
import { RowActionsMenu } from "@/components/row-actions-menu";
import { useAuth } from "@/hooks/use-auth";
import { useIsMobile } from "@/hooks/use-is-mobile";
import { useDeleteStation, useStations } from "@/hooks/use-stations";
import { isHttpError, serverMessage } from "@/lib/is-http-error";
import { reportHttpError } from "@/lib/report-error";
import { useSnackbar } from "@/providers/snackbar-provider";
import type { Station } from "@/types";

export function StationsListPage() {
  const isDemo = useAuth().user?.role === "demo";
  const { t } = useTranslation();
  const { showError } = useSnackbar();
  const { data: stations, isLoading, isError, error } = useStations();
  const deleteStation = useDeleteStation();
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const [deleteId, setDeleteId] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [editingStation, setEditingStation] = useState<Station | null>(null);

  const stationToDelete = deleteId ? stations?.find((s) => s.id === deleteId) : null;

  const handleDelete = async () => {
    if (deleteId) {
      try {
        await deleteStation.mutateAsync(deleteId);
        setDeleteId(null);
      } catch (err) {
        reportHttpError(showError, err, "Failed to delete station");
      }
    }
  };

  const openStation = (station: Station) => navigate(`/stations/${station.slug || station.id}`);

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
        <Alert severity="error">
          {(() => {
            const info = isHttpError(error);
            const server = serverMessage(error);
            return (
              <ErrorDetails
                title={server ?? "Failed to load stations"}
                details={{ ...info, message: server ?? "Failed to load stations" }}
              />
            );
          })()}
        </Alert>
      </Box>
    );
  }

  const empty = (
    <Paper variant="outlined" sx={{ py: 6, px: 2, textAlign: "center", color: "text.secondary", borderRadius: 3 }}>
      {t("stations:empty")}
    </Paper>
  );

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <PageHeader
        title={t("stations:title")}
        subtitle={t("stations:subtitle")}
        actions={
          <Button variant="contained" startIcon={<Add />} onClick={() => setCreateOpen(true)}>
            {t("stations:add")}
          </Button>
        }
      />

      {isMobile ? (
        !stations || stations.length === 0 ? (
          empty
        ) : (
          <MobileList>
            {stations.map((station) => (
              <Card
                key={station.id}
                variant="outlined"
                tabIndex={0}
                sx={{ borderRadius: 3, cursor: "pointer" }}
                onClick={() => openStation(station)}
                onKeyDown={(e) => {
                  if (e.target !== e.currentTarget) return;
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    openStation(station);
                  }
                }}
              >
                <CardContent sx={{ p: 2, "&:last-child": { pb: 2 }, display: "flex", alignItems: "center", gap: 1 }}>
                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    <Typography variant="subtitle1" noWrap sx={{ fontWeight: 600 }}>
                      {station.name}
                    </Typography>
                    <Typography variant="body2" color="text.secondary" noWrap>
                      {station.description || "\u2014"}
                    </Typography>
                    <Typography variant="caption" color="text.secondary" noWrap sx={{ display: "block" }}>
                      {station.stream_url || "\u2014"}
                    </Typography>
                  </Box>
                  <RowActionsMenu
                    actions={[
                      {
                        label: t("common:edit"),
                        icon: <Edit fontSize="small" />,
                        disabled: isDemo,
                        onClick: () => setEditingStation(station),
                      },
                      {
                        label: t("common:delete"),
                        icon: <Delete fontSize="small" />,
                        destructive: true,
                        onClick: () => setDeleteId(station.id),
                      },
                    ]}
                  />
                </CardContent>
              </Card>
            ))}
          </MobileList>
        )
      ) : (
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
                    onClick={() => openStation(station)}
                    onKeyDown={(e) => {
                      if (e.target !== e.currentTarget) return;
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        openStation(station);
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
                          aria-label={t("common:edit")}
                          disabled={isDemo}
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            setEditingStation(station);
                          }}
                        >
                          <Edit fontSize="small" />
                        </IconButton>
                        <IconButton
                          aria-label={t("common:delete")}
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
      )}

      <CreateStationDialog open={createOpen} onClose={() => setCreateOpen(false)} />
      <EditStationDialog open={!!editingStation} station={editingStation} onClose={() => setEditingStation(null)} />

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
