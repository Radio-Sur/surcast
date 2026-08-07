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
import { isHttpError } from "@/lib/is-http-error";

export function LoginPage() {
  const { t } = useTranslation();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const navigate = useNavigate();
  const { login, setupComplete, isLoading } = useAuth();
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

  if (!setupComplete) {
    return <Navigate to="/setup" replace />;
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setSubmitting(true);

    try {
      await login(username, password);
      navigate("/");
    } catch (err: unknown) {
      setError(isHttpError(err)?.message || t("errors:login"));
    } finally {
      setSubmitting(false);
    }
  };

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
          <Typography variant="body2" color="text.secondary" align="center" sx={{ mb: 3 }}>
            {t("auth:title")}
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
              placeholder={t("auth:username_placeholder")}
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
              placeholder={t("auth:password_placeholder")}
              required
              fullWidth
              autoComplete="current-password"
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
              {submitting ? <CircularProgress size={22} sx={{ color: "inherit" }} /> : t("auth:sign_in")}
            </Button>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
}
