import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import { Component, type ErrorInfo, type ReactNode } from "react";
import { useTranslation } from "react-i18next";

interface Props {
  children: ReactNode;
  onError?: (error: Error, info: ErrorInfo) => void;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

class ErrorBoundaryImpl extends Component<
  Props & { labels: { title: string; message: string; retry: string; reload: string } },
  State
> {
  state: State = { hasError: false, error: null };

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("ErrorBoundary caught:", error, info);
    this.props.onError?.(error, info);
  }

  handleRetry = () => {
    this.setState({ hasError: false, error: null });
  };

  render() {
    if (this.state.hasError) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "60vh",
            gap: 2,
            px: 2,
          }}
        >
          <Typography variant="h5" sx={{ fontWeight: 600 }}>
            {this.props.labels.title}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ maxWidth: 400, textAlign: "center" }}>
            {this.props.labels.message}
          </Typography>
          <Box sx={{ display: "flex", gap: 1 }}>
            <Button variant="contained" onClick={this.handleRetry}>
              {this.props.labels.retry}
            </Button>
            <Button variant="outlined" onClick={() => window.location.reload()}>
              {this.props.labels.reload}
            </Button>
          </Box>
        </Box>
      );
    }
    return this.props.children;
  }
}

export function ErrorBoundary(props: Props) {
  const { t } = useTranslation("errors");
  return (
    <ErrorBoundaryImpl
      {...props}
      labels={{ title: t("boundary_title"), message: t("codes.UNKNOWN"), retry: t("retry"), reload: t("reload") }}
    />
  );
}
