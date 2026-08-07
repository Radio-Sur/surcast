import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import CircularProgress from "@mui/material/CircularProgress";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Navigate, useNavigate } from "react-router-dom";
import { useAuth } from "@/hooks/use-auth";
import { httpClient } from "@/lib/api";
import { isHttpError } from "@/lib/is-http-error";

export function SetupPage() {
  const { t } = useTranslation();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [success, setSuccess] = useState(false);
  const navigate = useNavigate();
  const { setupComplete, isLoading, refreshSetupStatus } = useAuth();
  const formRef = useRef<HTMLFormElement>(null);
  const pwRef = useRef<HTMLInputElement>(null);

  if (isLoading || setupComplete === null) {
    return (
      <Box
        sx={{
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          bgcolor: "background.default",
        }}
      >
        <CircularProgress />
      </Box>
    );
  }

  if (setupComplete) {
    return <Navigate to="/login" replace />;
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setSubmitting(true);

    try {
      await httpClient.post("/setup/init", { username, password });
      await refreshSetupStatus();
      setSuccess(true);
    } catch (err: unknown) {
      setError(isHttpError(err)?.message || t("errors:setup"));
    } finally {
      setSubmitting(false);
    }
  };

  const logo = (
    <Box sx={{ display: "flex", alignItems: "center", gap: 1, justifyContent: "center", mt: -2, mb: 0.5 }}>
      <Box
        component="img"
        src="/sur_logo.png"
        alt={t("nav:brand_full")}
        sx={{ width: 52, height: 52, borderRadius: 1.5 }}
      />
      <Typography variant="h4" sx={{ fontWeight: 800, letterSpacing: "-0.03em", lineHeight: 1.2, mt: 0.5 }}>
        <Box component="span" sx={{ color: "primary.main" }}>
          {t("nav:brand_sur")}
        </Box>
        {t("nav:brand_cast")}
      </Typography>
    </Box>
  );

  if (success) {
    return (
      <Box
        sx={{
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          bgcolor: "background.default",
        }}
      >
        <Card sx={{ width: "100%", maxWidth: 420, mx: 2 }}>
          <CardContent sx={{ p: 4, textAlign: "center" }}>
            {logo}
            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
              {t("auth:setup_success_title")}
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
              {t("auth:setup_success_message")}
            </Typography>
            <Button
              onClick={() => {
                navigate("/login");
              }}
              variant="contained"
              size="large"
              fullWidth
            >
              {t("auth:setup_go_to_sign_in")}
            </Button>
          </CardContent>
        </Card>
      </Box>
    );
  }

  return (
    <Box
      sx={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        bgcolor: "background.default",
      }}
    >
      <Card sx={{ width: "100%", maxWidth: 420, mx: 2 }}>
        <CardContent sx={{ p: 4 }}>
          {logo}
          <Typography variant="body2" color="text.secondary" align="center" sx={{ mb: 3 }}>
            {t("auth:setup_title")}
          </Typography>

          <Box
            ref={formRef}
            component="form"
            onSubmit={handleSubmit}
            sx={{ display: "flex", flexDirection: "column", gap: 2.5 }}
          >
            {error && <Alert severity="error">{error}</Alert>}

            <TextField
              label={t("auth:username")}
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder={t("auth:setup_username_placeholder")}
              required
              fullWidth
              autoFocus
              autoComplete="username"
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  pwRef.current?.focus();
                }
              }}
            />
            <TextField
              inputRef={pwRef}
              label={t("auth:password")}
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder={t("auth:setup_password_placeholder")}
              required
              fullWidth
              autoComplete="new-password"
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  formRef.current?.requestSubmit();
                }
              }}
            />
            <Button
              type="submit"
              variant="contained"
              size="large"
              fullWidth
              disabled={submitting}
              sx={{ position: "relative" }}
            >
              {submitting ? <CircularProgress size={22} sx={{ color: "inherit" }} /> : t("auth:setup_create_button")}
            </Button>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
