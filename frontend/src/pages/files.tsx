import {
  DndContext,
  type DragEndEvent,
  type DraggableAttributes,
  type DraggableSyntheticListeners,
  PointerSensor,
  useDraggable,
  useDroppable,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import CreateNewFolder from "@mui/icons-material/CreateNewFolder";
import Delete from "@mui/icons-material/Delete";
import DriveFileMove from "@mui/icons-material/DriveFileMove";
import FilterAlt from "@mui/icons-material/FilterAlt";
import FolderOpen from "@mui/icons-material/FolderOpen";
import InfoOutlined from "@mui/icons-material/InfoOutlined";
import InsertDriveFile from "@mui/icons-material/InsertDriveFile";
import MoreVert from "@mui/icons-material/MoreVert";
import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import Link from "@mui/material/Link";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import Paper from "@mui/material/Paper";
import Skeleton from "@mui/material/Skeleton";
import Stack from "@mui/material/Stack";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TableSortLabel from "@mui/material/TableSortLabel";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { FolderCoverStack } from "@/components/folder-cover-stack";
import { GripDots } from "@/components/queue";
import { SongCover } from "@/components/song-cover";
import { SongDetailsDialog } from "@/components/song-details-dialog";
import {
  useCreateFolder,
  useDeleteFile,
  useDeleteFolder,
  useDirectory,
  useFolders,
  useMoveEntry,
} from "@/hooks/use-files";
import { usePurgeMissingSongs, useSongs } from "@/hooks/use-songs";
import { formatBytes } from "@/lib/format-bytes";
import { reportHttpError } from "@/lib/report-error";
import { useSnackbar } from "@/providers/snackbar-provider";
import type { FileEntry, FileSortKey, FolderEntry, SortOrder } from "@/types";

const TABLE_SX = {
  borderRadius: 3,
  "& .MuiTableCell-root:first-of-type": { pl: 3 },
  "& .MuiTableCell-root:last-of-type": { pr: 3 },
};

function baseName(path: string) {
  return path.split("/").pop() ?? path;
}

function parentPath(path: string) {
  return path.includes("/") ? path.slice(0, path.lastIndexOf("/")) : "";
}

function formatDate(value: number | null) {
  return value ? new Date(value).toLocaleString() : "—";
}

function fileType(name: string) {
  const dot = name.lastIndexOf(".");
  return dot > 0 ? name.slice(dot + 1).toUpperCase() : "—";
}

function DragHandle({
  setNodeRef,
  attributes,
  listeners,
  onClick,
}: {
  setNodeRef: (node: HTMLElement | null) => void;
  attributes: DraggableAttributes;
  listeners: DraggableSyntheticListeners;
  onClick: (event: React.MouseEvent) => void;
}) {
  return (
    <IconButton
      ref={setNodeRef}
      {...attributes}
      {...listeners}
      onClick={onClick}
      size="small"
      sx={{ cursor: "grab", touchAction: "none", color: "text.disabled" }}
    >
      <GripDots />
    </IconButton>
  );
}

function FolderRow({
  entry,
  onOpen,
  onMove,
  onDelete,
}: {
  entry: FolderEntry;
  onOpen: () => void;
  onMove: () => void;
  onDelete: () => void;
}) {
  const { t } = useTranslation("files");
  const [menuAnchor, setMenuAnchor] = useState<HTMLElement | null>(null);
  const { attributes, listeners, setNodeRef: setDragRef, isDragging } = useDraggable({ id: entry.path });
  const { setNodeRef: setDropRef, isOver } = useDroppable({ id: `folder:${entry.path}` });

  return (
    <TableRow
      ref={setDropRef}
      hover
      sx={{
        cursor: "pointer",
        opacity: isDragging ? 0.5 : 1,
        bgcolor: isOver ? "action.selected" : undefined,
      }}
      onClick={onOpen}
    >
      <TableCell sx={{ width: 44 }}>
        <DragHandle
          setNodeRef={setDragRef}
          attributes={attributes}
          listeners={listeners}
          onClick={(e) => e.stopPropagation()}
        />
      </TableCell>
      <TableCell sx={{ width: 56 }}>
        <FolderCoverStack covers={entry.covers} />
      </TableCell>
      <TableCell sx={{ fontWeight: 500 }}>{entry.name}</TableCell>
      <TableCell />
      <TableCell />
      <TableCell>
        <Typography variant="body2" color="text.secondary">
          {t("folder")}
        </Typography>
      </TableCell>
      <TableCell align="right" onClick={(e) => e.stopPropagation()}>
        <IconButton size="small" onClick={(e) => setMenuAnchor(e.currentTarget)}>
          <MoreVert fontSize="small" />
        </IconButton>
        <Menu anchorEl={menuAnchor} open={!!menuAnchor} onClose={() => setMenuAnchor(null)}>
          <MenuItem
            onClick={() => {
              setMenuAnchor(null);
              onOpen();
            }}
          >
            <FolderOpen fontSize="small" sx={{ mr: 1 }} />
            {t("open")}
          </MenuItem>
          <MenuItem
            onClick={() => {
              setMenuAnchor(null);
              onMove();
            }}
          >
            <DriveFileMove fontSize="small" sx={{ mr: 1 }} />
            {t("move")}
          </MenuItem>
          <MenuItem
            onClick={() => {
              setMenuAnchor(null);
              onDelete();
            }}
            sx={{ color: "error.main" }}
          >
            <Delete fontSize="small" sx={{ mr: 1 }} />
            {t("delete")}
          </MenuItem>
        </Menu>
      </TableCell>
    </TableRow>
  );
}

function FileRow({
  entry,
  onMove,
  onDelete,
  onDetails,
}: {
  entry: FileEntry;
  onMove: () => void;
  onDelete: () => void;
  onDetails: () => void;
}) {
  const { t } = useTranslation("files");
  const { t: ts } = useTranslation("songs");
  const [menuAnchor, setMenuAnchor] = useState<HTMLElement | null>(null);
  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({ id: entry.path });

  return (
    <TableRow hover sx={{ opacity: isDragging ? 0.5 : 1 }}>
      <TableCell sx={{ width: 44 }}>
        <DragHandle setNodeRef={setNodeRef} attributes={attributes} listeners={listeners} onClick={() => {}} />
      </TableCell>
      <TableCell sx={{ width: 56 }}>
        {entry.song ? (
          <SongCover songId={entry.song.id} hasCover={entry.song.has_cover} />
        ) : (
          <InsertDriveFile color="disabled" />
        )}
      </TableCell>
      <TableCell>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1, minWidth: 0 }}>
          <Box sx={{ minWidth: 0 }}>
            <Box
              component="span"
              sx={{ display: "block", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
            >
              {entry.song?.title || entry.name}
            </Box>
            {entry.song && (
              <Typography variant="caption" color="text.secondary" noWrap>
                {entry.song.artist || ts("table_artist")}
              </Typography>
            )}
          </Box>
          {entry.song?.missing_since && <Chip size="small" color="error" variant="outlined" label={t("missing")} />}
          {!entry.song && entry.is_audio && <Chip size="small" variant="outlined" label={t("not_in_library")} />}
        </Box>
      </TableCell>
      <TableCell>
        <Typography variant="body2" color="text.secondary">
          {formatBytes(entry.size)}
        </Typography>
      </TableCell>
      <TableCell>
        <Typography variant="body2" color="text.secondary">
          {formatDate(entry.modified_at)}
        </Typography>
      </TableCell>
      <TableCell>
        <Typography variant="body2" color="text.secondary">
          {fileType(entry.name)}
        </Typography>
      </TableCell>
      <TableCell align="right">
        <IconButton size="small" onClick={(e) => setMenuAnchor(e.currentTarget)}>
          <MoreVert fontSize="small" />
        </IconButton>
        <Menu anchorEl={menuAnchor} open={!!menuAnchor} onClose={() => setMenuAnchor(null)}>
          {entry.song && (
            <MenuItem
              onClick={() => {
                setMenuAnchor(null);
                onDetails();
              }}
            >
              <InfoOutlined fontSize="small" sx={{ mr: 1 }} />
              {t("details")}
            </MenuItem>
          )}
          <MenuItem
            onClick={() => {
              setMenuAnchor(null);
              onMove();
            }}
          >
            <DriveFileMove fontSize="small" sx={{ mr: 1 }} />
            {t("move")}
          </MenuItem>
          <MenuItem
            onClick={() => {
              setMenuAnchor(null);
              onDelete();
            }}
            sx={{ color: "error.main" }}
          >
            <Delete fontSize="small" sx={{ mr: 1 }} />
            {t("delete")}
          </MenuItem>
        </Menu>
      </TableCell>
    </TableRow>
  );
}

export function FilesPage() {
  const { t } = useTranslation("files");
  const { t: ts } = useTranslation("songs");
  const { t: tc } = useTranslation("common");
  const { showSnackbar, showError } = useSnackbar();

  const [path, setPath] = useState("");
  const [sort, setSort] = useState<FileSortKey>("name");
  const [order, setOrder] = useState<SortOrder>("asc");
  const [missingMode, setMissingMode] = useState(false);

  const [newFolderOpen, setNewFolderOpen] = useState(false);
  const [folderName, setFolderName] = useState("");
  const [moveFrom, setMoveFrom] = useState<string | null>(null);
  const [moveDest, setMoveDest] = useState("");
  const [deleteTarget, setDeleteTarget] = useState<{ kind: "file" | "folder"; path: string; name: string } | null>(
    null,
  );
  const [detailsSongId, setDetailsSongId] = useState<string | null>(null);
  const [purgeOpen, setPurgeOpen] = useState(false);

  const { data: listing, isLoading, isError, error } = useDirectory(path, sort, order);
  const { data: folders } = useFolders();
  const { data: allSongs } = useSongs();
  const createFolder = useCreateFolder();
  const deleteFolder = useDeleteFolder();
  const deleteFile = useDeleteFile();
  const moveEntry = useMoveEntry();
  const purgeMissing = usePurgeMissingSongs();

  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 5 } }));

  const missingSongs = useMemo(() => (allSongs ?? []).filter((song) => song.missing_since), [allSongs]);

  const crumbs = useMemo(() => {
    const parts = path ? path.split("/") : [];
    let acc = "";
    return parts.map((part) => {
      acc = acc ? `${acc}/${part}` : part;
      return { name: part, path: acc };
    });
  }, [path]);

  const handleSort = (key: FileSortKey) => {
    if (sort === key) setOrder((prev) => (prev === "asc" ? "desc" : "asc"));
    else {
      setSort(key);
      setOrder("asc");
    }
  };

  const handleDragEnd = (event: DragEndEvent) => {
    if (!event.over) return;
    const from = String(event.active.id);
    const overId = String(event.over.id);
    if (!overId.startsWith("folder:")) return;
    const dest = overId.slice("folder:".length);

    const to = dest ? `${dest}/${baseName(from)}` : baseName(from);
    if (to === from || (from && dest.startsWith(`${from}/`))) return;
    moveEntry.mutate(
      { from, to },
      {
        onSuccess: () => showSnackbar(t("moved"), "success"),
        onError: (err) => reportHttpError(showError, err, t("load_error")),
      },
    );
  };

  const handleCreateFolder = async () => {
    const name = folderName.trim();
    if (!name) return;
    const target = path ? `${path}/${name}` : name;
    try {
      await createFolder.mutateAsync(target);
      setNewFolderOpen(false);
      setFolderName("");
      showSnackbar(t("folder_created"), "success");
    } catch (err) {
      reportHttpError(showError, err, t("load_error"));
    }
  };

  const handleMove = async () => {
    if (!moveFrom) return;
    const to = moveDest ? `${moveDest}/${baseName(moveFrom)}` : baseName(moveFrom);
    try {
      await moveEntry.mutateAsync({ from: moveFrom, to });
      setMoveFrom(null);
      showSnackbar(t("moved"), "success");
    } catch (err) {
      reportHttpError(showError, err, t("load_error"));
    }
  };

  const handleDelete = async () => {
    if (!deleteTarget) return;
    try {
      if (deleteTarget.kind === "file") await deleteFile.mutateAsync(deleteTarget.path);
      else await deleteFolder.mutateAsync(deleteTarget.path);
      setDeleteTarget(null);
      showSnackbar(t("deleted"), "success");
    } catch (err) {
      reportHttpError(showError, err, t("load_error"));
    }
  };

  const handlePurge = async () => {
    try {
      await purgeMissing.mutateAsync();
      setPurgeOpen(false);
      showSnackbar(t("purged", { count: missingSongs.length }), "success");
    } catch (err) {
      reportHttpError(showError, err, t("load_error"));
    }
  };

  const moveOptions = (folders ?? []).filter(
    (folder) => folder !== moveFrom && !(moveFrom && folder.startsWith(`${moveFrom}/`)),
  );

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 2 }}>
        <Box>
          <Typography variant="h4">{t("title")}</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            {t("subtitle")}
          </Typography>
        </Box>
        <Stack direction="row" spacing={1} sx={{ alignItems: "center" }}>
          <Chip
            icon={<FilterAlt />}
            label={t("show_missing")}
            onClick={() => setMissingMode((value) => !value)}
            color={missingMode ? "primary" : "default"}
            variant={missingMode ? "filled" : "outlined"}
          />
          {!missingMode && (
            <Button variant="contained" startIcon={<CreateNewFolder />} onClick={() => setNewFolderOpen(true)}>
              {t("new_folder")}
            </Button>
          )}
        </Stack>
      </Box>

      {missingMode ? (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <Typography variant="subtitle1">
              {t("missing_mode")} <Chip size="small" label={missingSongs.length} sx={{ ml: 1 }} />
            </Typography>
            <Button
              variant="outlined"
              color="error"
              startIcon={<Delete />}
              disabled={missingSongs.length === 0}
              onClick={() => setPurgeOpen(true)}
            >
              {t("delete_all_missing")}
            </Button>
          </Box>
          <TableContainer component={Paper} variant="outlined" sx={{ ...TABLE_SX }}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ fontWeight: 600 }}>{ts("table_title")}</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>{ts("table_artist")}</TableCell>
                  <TableCell sx={{ fontWeight: 600 }}>{ts("details_path")}</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {missingSongs.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={3} align="center" sx={{ py: 6, color: "text.secondary" }}>
                      {t("empty_missing")}
                    </TableCell>
                  </TableRow>
                ) : (
                  missingSongs.map((song) => (
                    <TableRow key={song.id} hover>
                      <TableCell>{song.title}</TableCell>
                      <TableCell>{song.artist || "—"}</TableCell>
                      <TableCell>
                        <Typography variant="body2" color="text.secondary" sx={{ wordBreak: "break-all" }}>
                          {song.file_path}
                        </Typography>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      ) : (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <Breadcrumbs>
            <Link
              component="button"
              underline="hover"
              onClick={() => setPath("")}
              color={path ? "inherit" : "text.primary"}
            >
              {t("root")}
            </Link>
            {crumbs.map((crumb, index) =>
              index === crumbs.length - 1 ? (
                <Typography key={crumb.path} color="text.primary">
                  {crumb.name}
                </Typography>
              ) : (
                <Link key={crumb.path} component="button" underline="hover" onClick={() => setPath(crumb.path)}>
                  {crumb.name}
                </Link>
              ),
            )}
          </Breadcrumbs>

          {isError ? (
            <Typography color="error">{error instanceof Error ? error.message : t("load_error")}</Typography>
          ) : isLoading || !listing ? (
            <Skeleton variant="rounded" height={300} />
          ) : (
            <DndContext sensors={sensors} onDragEnd={handleDragEnd}>
              <TableContainer component={Paper} variant="outlined" sx={{ ...TABLE_SX }}>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ width: 44 }} />
                      <TableCell sx={{ width: 56 }} />
                      <TableCell sortDirection={sort === "name" ? order : false}>
                        <TableSortLabel active={sort === "name"} direction={order} onClick={() => handleSort("name")}>
                          {t("name")}
                        </TableSortLabel>
                      </TableCell>
                      <TableCell sortDirection={sort === "size" ? order : false}>
                        <TableSortLabel active={sort === "size"} direction={order} onClick={() => handleSort("size")}>
                          {t("size")}
                        </TableSortLabel>
                      </TableCell>
                      <TableCell sortDirection={sort === "modified" ? order : false}>
                        <TableSortLabel
                          active={sort === "modified"}
                          direction={order}
                          onClick={() => handleSort("modified")}
                        >
                          {t("modified")}
                        </TableSortLabel>
                      </TableCell>
                      <TableCell sortDirection={sort === "type" ? order : false}>
                        <TableSortLabel active={sort === "type"} direction={order} onClick={() => handleSort("type")}>
                          {t("type")}
                        </TableSortLabel>
                      </TableCell>
                      <TableCell sx={{ fontWeight: 600 }} align="right">
                        {t("action")}
                      </TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {listing.folders.length === 0 && listing.files.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={7} align="center" sx={{ py: 6, color: "text.secondary" }}>
                          {t("empty")}
                        </TableCell>
                      </TableRow>
                    ) : (
                      <>
                        {listing.folders.map((folder) => (
                          <FolderRow
                            key={folder.path}
                            entry={folder}
                            onOpen={() => setPath(folder.path)}
                            onMove={() => {
                              setMoveFrom(folder.path);
                              setMoveDest(parentPath(folder.path));
                            }}
                            onDelete={() => setDeleteTarget({ kind: "folder", path: folder.path, name: folder.name })}
                          />
                        ))}
                        {listing.files.map((file) => (
                          <FileRow
                            key={file.path}
                            entry={file}
                            onMove={() => {
                              setMoveFrom(file.path);
                              setMoveDest(parentPath(file.path));
                            }}
                            onDelete={() => setDeleteTarget({ kind: "file", path: file.path, name: file.name })}
                            onDetails={() => file.song && setDetailsSongId(file.song.id)}
                          />
                        ))}
                      </>
                    )}
                  </TableBody>
                </Table>
              </TableContainer>
            </DndContext>
          )}
        </Box>
      )}

      <Dialog
        open={newFolderOpen}
        onClose={() => setNewFolderOpen(false)}
        slotProps={{ paper: { sx: { borderRadius: 3 } } }}
      >
        <DialogTitle sx={{ px: 3, pt: 3 }}>{t("new_folder")}</DialogTitle>
        <DialogContent sx={{ px: 3, pt: 2 }}>
          <TextField
            autoFocus
            fullWidth
            label={t("folder_name")}
            value={folderName}
            onChange={(e) => setFolderName(e.target.value)}
            sx={{ mt: 1 }}
          />
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 3 }}>
          <Button onClick={() => setNewFolderOpen(false)}>{tc("cancel")}</Button>
          <Button
            variant="contained"
            onClick={handleCreateFolder}
            disabled={!folderName.trim() || createFolder.isPending}
          >
            {t("create")}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={!!moveFrom} onClose={() => setMoveFrom(null)} slotProps={{ paper: { sx: { borderRadius: 3 } } }}>
        <DialogTitle sx={{ px: 3, pt: 3 }}>{t("move_title", { name: moveFrom ? baseName(moveFrom) : "" })}</DialogTitle>
        <DialogContent sx={{ px: 3, pt: 2 }}>
          <TextField
            select
            fullWidth
            label={t("destination")}
            value={moveDest}
            onChange={(e) => setMoveDest(e.target.value)}
            sx={{ mt: 1, minWidth: 280 }}
          >
            <MenuItem value="">{t("move_root")}</MenuItem>
            {moveOptions.map((folder) => (
              <MenuItem key={folder} value={folder}>
                {folder}
              </MenuItem>
            ))}
          </TextField>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 3 }}>
          <Button onClick={() => setMoveFrom(null)}>{tc("cancel")}</Button>
          <Button variant="contained" onClick={handleMove} disabled={moveEntry.isPending}>
            {t("move")}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={!!deleteTarget}
        onClose={() => setDeleteTarget(null)}
        slotProps={{ paper: { sx: { borderRadius: 3 } } }}
      >
        <DialogTitle sx={{ px: 3, pt: 3 }}>
          {deleteTarget?.kind === "folder" ? t("delete_folder_title") : t("delete_file_title")}
        </DialogTitle>
        <DialogContent sx={{ px: 3, pt: 2 }}>
          <DialogContentText>
            {deleteTarget?.kind === "folder"
              ? t("delete_folder_message", { name: deleteTarget.name })
              : t("delete_file_message", { name: deleteTarget?.name ?? "" })}
          </DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 3 }}>
          <Button onClick={() => setDeleteTarget(null)}>{tc("cancel")}</Button>
          <Button
            variant="contained"
            color="error"
            onClick={handleDelete}
            disabled={deleteFile.isPending || deleteFolder.isPending}
          >
            {t("delete")}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={purgeOpen} onClose={() => setPurgeOpen(false)} slotProps={{ paper: { sx: { borderRadius: 3 } } }}>
        <DialogTitle sx={{ px: 3, pt: 3 }}>{ts("delete_missing_title")}</DialogTitle>
        <DialogContent sx={{ px: 3, pt: 2 }}>
          <DialogContentText>{ts("delete_missing_message", { count: missingSongs.length })}</DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 3 }}>
          <Button onClick={() => setPurgeOpen(false)}>{tc("cancel")}</Button>
          <Button variant="contained" color="error" onClick={handlePurge} disabled={purgeMissing.isPending}>
            {t("delete")}
          </Button>
        </DialogActions>
      </Dialog>

      <SongDetailsDialog songId={detailsSongId} onClose={() => setDetailsSongId(null)} />
    </Box>
  );
}
