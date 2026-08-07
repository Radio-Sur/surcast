import MusicNote from "@mui/icons-material/MusicNote";
import UploadFile from "@mui/icons-material/UploadFile";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormGroup from "@mui/material/FormGroup";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { Station } from "@/types";

export function UploadSongDialog({
  open,
  stations,
  uploadSongPending,
  uploadZipPending,
  onUploadSingle,
  onUploadZip,
  onClose,
}: {
  open: boolean;
  stations: Station[] | undefined;
  uploadSongPending: boolean;
  uploadZipPending: boolean;
  onUploadSingle: (data: {
    file: File;
    title?: string;
    artist?: string;
    album?: string;
    assignToAll: boolean;
    stationIds: string[];
  }) => void;
  onUploadZip: (data: { file: File; assignToAll: boolean; stationIds: string[] }) => void;
  onClose: () => void;
}) {
  const [uploadTab, setUploadTab] = useState(0);
  const [singleFile, setSingleFile] = useState<File | null>(null);
  const [singleTitle, setSingleTitle] = useState("");
  const [singleArtist, setSingleArtist] = useState("");
  const [singleAlbum, setSingleAlbum] = useState("");
  const [zipFile, setZipFile] = useState<File | null>(null);
  const [assignToAll, setAssignToAll] = useState(false);
  const [selectedStationIds, setSelectedStationIds] = useState<Set<string>>(new Set());
  const { t } = useTranslation();
  const [dragOver, setDragOver] = useState(false);

  const singleFileRef = useRef<HTMLInputElement>(null);
  const zipFileRef = useRef<HTMLInputElement>(null);

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  };
  const handleDragLeave = () => setDragOver(false);
  const handleDropSingle = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file?.type.startsWith("audio/")) setSingleFile(file);
  };
  const handleDropZip = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file?.name.endsWith(".zip")) setZipFile(file);
  };

  const toggleStation = (id: string) => {
    setSelectedStationIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const resetForm = () => {
    setSingleFile(null);
    setSingleTitle("");
    setSingleArtist("");
    setSingleAlbum("");
    setZipFile(null);
    setUploadTab(0);
    setAssignToAll(false);
    setSelectedStationIds(new Set());
  };

  const handleClose = () => {
    resetForm();
    onClose();
  };

  const handleUploadSingle = () => {
    if (!singleFile) return;
    onUploadSingle({
      file: singleFile,
      title: singleTitle || undefined,
      artist: singleArtist || undefined,
      album: singleAlbum || undefined,
      assignToAll,
      stationIds: assignToAll ? [] : Array.from(selectedStationIds),
    });
    handleClose();
  };

  const handleUploadZip = () => {
    if (!zipFile) return;
    onUploadZip({
      file: zipFile,
      assignToAll,
      stationIds: assignToAll ? [] : Array.from(selectedStationIds),
    });
    handleClose();
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
      slotProps={{ paper: { sx: { borderRadius: 3 } } }}
    >
      <DialogTitle sx={{ px: 3, pt: 3, pb: 0 }}>{t("songs:upload_title")}</DialogTitle>
      <DialogContent sx={{ px: 3, pt: 3, pb: 2 }}>
        <Tabs value={uploadTab} onChange={(_, v) => setUploadTab(v)} sx={{ mb: 3, mt: 1 }}>
          <Tab label={t("songs:tab_single")} />
          <Tab label={t("songs:tab_zip")} />
        </Tabs>

        {uploadTab === 0 ? (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
            <input
              ref={singleFileRef}
              type="file"
              accept="audio/*"
              hidden
              onChange={(e) => setSingleFile(e.target.files?.[0] ?? null)}
            />
            <Box
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDropSingle}
              onClick={() => singleFileRef.current?.click()}
              sx={{
                height: 120,
                border: "2px dashed",
                borderColor: dragOver ? "primary.main" : "divider",
                borderRadius: 3,
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                gap: 1,
                cursor: "pointer",
                bgcolor: dragOver ? "action.hover" : "transparent",
                transition: "all 0.2s",
                color: "text.secondary",
              }}
            >
              <MusicNote fontSize="large" color={singleFile ? "primary" : "inherit"} />
              <Typography variant="body2">{singleFile ? singleFile.name : t("songs:drop_audio")}</Typography>
            </Box>
            <TextField
              label={t("songs:title_label")}
              value={singleTitle}
              onChange={(e) => setSingleTitle(e.target.value)}
              placeholder={t("songs:leave_empty")}
            />
            <TextField
              label={t("songs:artist_label")}
              value={singleArtist}
              onChange={(e) => setSingleArtist(e.target.value)}
            />
            <TextField
              label={t("songs:album_label")}
              value={singleAlbum}
              onChange={(e) => setSingleAlbum(e.target.value)}
            />
          </Box>
        ) : (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
            <input
              ref={zipFileRef}
              type="file"
              accept=".zip"
              hidden
              onChange={(e) => setZipFile(e.target.files?.[0] ?? null)}
            />
            <Box
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDropZip}
              onClick={() => zipFileRef.current?.click()}
              sx={{
                height: 120,
                border: "2px dashed",
                borderColor: dragOver ? "primary.main" : "divider",
                borderRadius: 3,
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                gap: 1,
                cursor: "pointer",
                bgcolor: dragOver ? "action.hover" : "transparent",
                transition: "all 0.2s",
                color: "text.secondary",
              }}
            >
              <UploadFile fontSize="large" color={zipFile ? "primary" : "inherit"} />
              <Typography variant="body2">{zipFile ? zipFile.name : t("songs:drop_zip")}</Typography>
            </Box>
            <Typography variant="caption" color="text.secondary">
              {t("songs:supported_formats")}
            </Typography>
          </Box>
        )}

        <Box sx={{ mt: 3 }}>
          <FormControlLabel
            control={<Checkbox checked={assignToAll} onChange={(e) => setAssignToAll(e.target.checked)} />}
            label={t("songs:assign_to_all")}
          />
          {!assignToAll && stations && stations.length > 0 && (
            <FormGroup sx={{ ml: 2, mt: 1 }}>
              {stations.map((s) => (
                <FormControlLabel
                  key={s.id}
                  control={
                    <Checkbox
                      size="small"
                      checked={selectedStationIds.has(s.id)}
                      onChange={() => toggleStation(s.id)}
                    />
                  }
                  label={s.name}
                />
              ))}
            </FormGroup>
          )}
          {!assignToAll && stations && stations.length === 0 && (
            <DialogContentText sx={{ ml: 2 }}>{t("songs:no_stations")}</DialogContentText>
          )}
        </Box>
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button onClick={handleClose}>{t("common:cancel")}</Button>
        <Button
          onClick={uploadTab === 0 ? handleUploadSingle : handleUploadZip}
          variant="contained"
          disabled={uploadTab === 0 ? !singleFile || uploadSongPending : !zipFile || uploadZipPending}
        >
          {uploadTab === 0
            ? uploadSongPending
              ? t("common:uploading")
              : t("songs:upload_button")
            : uploadZipPending
              ? t("common:uploading")
              : t("songs:upload_archive_button")}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
