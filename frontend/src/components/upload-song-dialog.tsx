import Close from "@mui/icons-material/Close";
import MusicNote from "@mui/icons-material/MusicNote";
import UploadFile from "@mui/icons-material/UploadFile";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import CircularProgress from "@mui/material/CircularProgress";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormGroup from "@mui/material/FormGroup";
import IconButton from "@mui/material/IconButton";
import LinearProgress from "@mui/material/LinearProgress";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { uploadsService } from "@/lib/services/uploads";
import { useSnackbar } from "@/providers/snackbar-provider";
import type { Station, UploadJobStatus } from "@/types";

export interface UploadFinished {
  created: number;
  failed: number;
}

const AUDIO_EXTS = new Set(["mp3", "wav", "ogg", "flac", "aac", "m4a", "wma", "opus"]);

function fileKey(f: File) {
  return `${f.name}|${f.size}|${f.lastModified}`;
}

function isAudioLike(f: File) {
  return f.type.startsWith("audio/") || AUDIO_EXTS.has((f.name.split(".").pop() ?? "").toLowerCase());
}

export function UploadSongDialog({
  open,
  stations,
  onFinished,
  onClose,
}: {
  open: boolean;
  stations: Station[] | undefined;
  onFinished: (result: UploadFinished) => void;
  onClose: () => void;
}) {
  const [files, setFiles] = useState<File[]>([]);
  const [zipFile, setZipFile] = useState<File | null>(null);
  const [singleTitle, setSingleTitle] = useState("");
  const [singleArtist, setSingleArtist] = useState("");
  const [singleAlbum, setSingleAlbum] = useState("");
  const [assignToAll, setAssignToAll] = useState(false);
  const [selectedStationIds, setSelectedStationIds] = useState<Set<string>>(new Set());
  const [dragOver, setDragOver] = useState(false);
  const [starting, setStarting] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<UploadJobStatus | null>(null);
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();

  const fileInputRef = useRef<HTMLInputElement>(null);
  const zipInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    setStarting(false);
    setJobId(null);
    setJobStatus(null);
  }, [open]);

  const resetFormRef = useRef<() => void>(() => {});
  const onFinishedRef = useRef(onFinished);
  const onCloseRef = useRef(onClose);
  const showSnackbarRef = useRef(showSnackbar);
  const tRef = useRef(t);

  useEffect(() => {
    if (!jobId) return;
    let cancelled = false;

    const tick = async () => {
      try {
        const status = await uploadsService.job(jobId);
        if (cancelled) return;
        setJobStatus(status);
        if (status.status === "done") {
          setStarting(false);
          setJobId(null);
          resetFormRef.current();
          onFinishedRef.current({ created: status.processed, failed: status.failed });
          onCloseRef.current();
        } else if (status.status === "error") {
          setStarting(false);
          setJobId(null);
          showSnackbarRef.current(status.error || tRef.current("songs:upload_failed"), "error");
        }
      } catch (err) {
        if (!cancelled) {
          console.error("Failed to poll upload job", err);
        }
      }
    };

    void tick();
    const interval = setInterval(tick, 700);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [jobId]);

  const appendFiles = (incoming: File[]) => {
    setFiles((prev) => {
      const byKey = new Map(prev.map((f) => [fileKey(f), f]));
      for (const f of incoming) byKey.set(fileKey(f), f);
      return Array.from(byKey.values());
    });
  };

  const removeFile = (key: string) => {
    setFiles((prev) => prev.filter((f) => fileKey(f) !== key));
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  };
  const handleDragLeave = () => setDragOver(false);

  const handlePick = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files) return;
    const incoming = Array.from(e.target.files);
    handleIncoming(incoming);
    e.target.value = "";
  };

  const handleIncoming = (incoming: File[]) => {
    const audio = incoming.filter((f) => isAudioLike(f) && !f.name.toLowerCase().endsWith(".zip"));
    if (audio.length > 0) appendFiles(audio);
    const zip = incoming.find((f) => f.name.toLowerCase().endsWith(".zip"));
    if (zip) setZipFile(zip);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    handleIncoming(Array.from(e.dataTransfer.files));
  };

  const handlePickZip = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file?.name.toLowerCase().endsWith(".zip")) setZipFile(file);
    e.target.value = "";
  };

  const handleDropZip = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const zip = Array.from(e.dataTransfer.files).find((f) => f.name.toLowerCase().endsWith(".zip"));
    if (zip) setZipFile(zip);
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
    setFiles([]);
    setZipFile(null);
    setSingleTitle("");
    setSingleArtist("");
    setSingleAlbum("");
    setAssignToAll(false);
    setSelectedStationIds(new Set());
    setDragOver(false);
  };

  resetFormRef.current = resetForm;
  onFinishedRef.current = onFinished;
  onCloseRef.current = onClose;
  showSnackbarRef.current = showSnackbar;
  tRef.current = t;

  const handleClose = () => {
    resetForm();
    onClose();
  };

  const working = starting || jobId !== null;

  const percent = jobStatus ? (jobStatus.total > 0 ? Math.round((jobStatus.processed / jobStatus.total) * 100) : 0) : 0;

  const audioCount = files.length;
  const trackCount = audioCount + (zipFile ? 1 : 0);
  const singleEntry = audioCount === 1 && !zipFile;

  const handleUpload = async () => {
    if (trackCount === 0) return;

    const formData = new FormData();
    for (const f of files) {
      formData.append("file", f);
    }
    if (zipFile) {
      formData.append("file", zipFile);
    }
    if (singleEntry) {
      if (singleTitle) formData.append("title", singleTitle);
      if (singleArtist) formData.append("artist", singleArtist);
      if (singleAlbum) formData.append("album", singleAlbum);
    }
    if (assignToAll) {
      formData.append("assign_to_all", "true");
    } else if (selectedStationIds.size > 0) {
      formData.append("station_ids", JSON.stringify(Array.from(selectedStationIds)));
    }

    setStarting(true);
    try {
      const created = await uploadsService.createJob(formData);
      setJobId(created.job_id);
    } catch (err) {
      console.error("Failed to start upload", err);
      setStarting(false);
      showSnackbar(t("songs:upload_failed"), "error");
    }
  };

  const uploadLabel = () => {
    if (working) return t("common:uploading");
    if (trackCount === 0) return t("songs:upload_button");
    if (trackCount === 1) return t("songs:upload_single");
    return t("songs:upload_many", { count: trackCount });
  };

  return (
    <Dialog
      open={open}
      onClose={working ? undefined : handleClose}
      maxWidth="sm"
      fullWidth
      slotProps={{ paper: { sx: { borderRadius: 3 } } }}
    >
      <DialogTitle sx={{ px: 3, pt: 3, pb: 0 }}>{t("songs:upload_title")}</DialogTitle>
      <DialogContent sx={{ px: 3, pt: 3, pb: 2 }}>
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <input ref={fileInputRef} type="file" accept="audio/*,.zip" multiple hidden onChange={handlePick} />

          <Box
            data-testid="dropzone"
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
            sx={{
              minHeight: 120,
              border: "2px dashed",
              borderColor: dragOver ? "primary.main" : "divider",
              borderRadius: 3,
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              gap: 1,
              p: 2,
              cursor: "pointer",
              bgcolor: dragOver ? "action.hover" : "transparent",
              transition: "all 0.2s",
              color: "text.secondary",
            }}
          >
            <MusicNote fontSize="large" color={files.length > 0 ? "primary" : "inherit"} />
            <Typography variant="body2">{trackCount === 0 ? t("songs:drop_files") : t("songs:drop_more")}</Typography>
          </Box>
          <Typography variant="caption" color="text.secondary">
            {t("songs:many_files_hint")}
          </Typography>

          <Box sx={{ mt: 1 }}>
            <Typography variant="subtitle2" sx={{ mb: 1 }}>
              {t("songs:zip_section_label")}
            </Typography>
            <input ref={zipInputRef} type="file" accept=".zip" hidden onChange={handlePickZip} />
            <Box
              data-testid="zip-dropzone"
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDropZip}
              onClick={() => zipInputRef.current?.click()}
              sx={{
                minHeight: 80,
                border: "2px dashed",
                borderColor: dragOver ? "primary.main" : "divider",
                borderRadius: 3,
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                gap: 1,
                p: 2,
                cursor: "pointer",
                bgcolor: dragOver ? "action.hover" : "transparent",
                transition: "all 0.2s",
                color: "text.secondary",
              }}
            >
              <UploadFile fontSize="large" color={zipFile ? "primary" : "inherit"} />
              <Typography variant="body2">{t("songs:drop_zip")}</Typography>
            </Box>
            <Typography variant="caption" color="text.secondary">
              {t("songs:zip_section_hint")}
            </Typography>
          </Box>

          {trackCount > 0 && (
            <Box sx={{ mt: 1 }}>
              <Typography variant="subtitle2" sx={{ mb: 1 }}>
                {trackCount === 1 ? t("songs:will_add_single") : t("songs:will_add", { count: trackCount })}
              </Typography>
              <List dense disablePadding sx={{ maxHeight: 200, overflowY: "auto" }}>
                {files.map((f) => (
                  <ListItem
                    key={fileKey(f)}
                    sx={{ px: 1 }}
                    secondaryAction={
                      <IconButton edge="end" aria-label={t("songs:remove_file")} onClick={() => removeFile(fileKey(f))}>
                        <Close fontSize="small" />
                      </IconButton>
                    }
                  >
                    <ListItemIcon sx={{ minWidth: 32 }}>
                      <MusicNote fontSize="small" color="primary" />
                    </ListItemIcon>
                    <ListItemText primary={f.name} slotProps={{ primary: { variant: "body2", noWrap: true } }} />
                  </ListItem>
                ))}
                {zipFile && (
                  <ListItem
                    sx={{ px: 1 }}
                    secondaryAction={
                      <IconButton edge="end" aria-label={t("songs:remove_file")} onClick={() => setZipFile(null)}>
                        <Close fontSize="small" />
                      </IconButton>
                    }
                  >
                    <ListItemIcon sx={{ minWidth: 32 }}>
                      <UploadFile fontSize="small" color="primary" />
                    </ListItemIcon>
                    <ListItemText
                      primary={t("songs:zip_row", { name: zipFile.name })}
                      slotProps={{ primary: { variant: "body2", noWrap: true } }}
                    />
                  </ListItem>
                )}
              </List>
            </Box>
          )}

          {singleEntry && (
            <>
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
            </>
          )}
        </Box>

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

        {working && (
          <Box sx={{ mt: 3 }}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
              <CircularProgress size={20} />
              <Typography
                variant="body2"
                color="text.secondary"
                sx={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
              >
                {jobStatus
                  ? `${t("songs:processing_file")} ${jobStatus.processed + 1}/${jobStatus.total}${jobStatus.current_file ? ` — ${jobStatus.current_file}` : ""}`
                  : t("common:uploading")}
              </Typography>
            </Box>
            <LinearProgress variant="determinate" value={percent} />
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: "block" }}>
              {percent}%
            </Typography>
          </Box>
        )}
      </DialogContent>
      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button onClick={handleClose} disabled={working}>
          {t("common:cancel")}
        </Button>
        <Button
          onClick={handleUpload}
          variant="contained"
          disabled={trackCount === 0 || working}
          startIcon={working ? <CircularProgress size={16} color="inherit" /> : undefined}
        >
          {uploadLabel()}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
