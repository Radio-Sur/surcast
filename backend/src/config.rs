use std::{env, path::PathBuf};

use crate::util::normalize_lexically;

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub jwt_secret: String,
    pub jwt_access_expiry: i64,
    pub jwt_refresh_expiry: i64,
    pub server_host: String,
    pub server_port: u16,
    pub lastfm_api_key: Option<String>,
    pub upload_dir: String,
    pub icecast_public_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
            jwt_secret: env::var("JWT_SECRET").expect("JWT_SECRET must be set"),
            jwt_access_expiry: env::var("JWT_ACCESS_EXPIRY")
                .unwrap_or_else(|_| "900".to_string())
                .parse()
                .expect("JWT_ACCESS_EXPIRY must be a number"),
            jwt_refresh_expiry: env::var("JWT_REFRESH_EXPIRY")
                .unwrap_or_else(|_| "604800".to_string())
                .parse()
                .expect("JWT_REFRESH_EXPIRY must be a number"),
            server_host: env::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            server_port: env::var("SERVER_PORT")
                .unwrap_or_else(|_| "3001".to_string())
                .parse()
                .expect("SERVER_PORT must be a number"),
            lastfm_api_key: env::var("LASTFM_API_KEY").ok(),
            upload_dir: absolute_upload_dir(env::var("UPLOAD_DIR").unwrap_or_else(|_| "./uploads".to_string())),
            icecast_public_url: env::var("ICECAST_PUBLIC_URL").unwrap_or_default(),
        }
    }
}

fn absolute_upload_dir(path: String) -> String {
    let path = PathBuf::from(path);
    let absolute = if path.is_absolute() {
        normalize_lexically(&path)
    } else {
        let joined = env::current_dir()
            .expect("failed to resolve UPLOAD_DIR from the current directory")
            .join(path);
        normalize_lexically(&joined)
    };
    absolute.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;

    fn cleanup_env() {
        env::remove_var("DATABASE_URL");
        env::remove_var("JWT_SECRET");
        env::remove_var("JWT_ACCESS_EXPIRY");
        env::remove_var("JWT_REFRESH_EXPIRY");
        env::remove_var("SERVER_HOST");
        env::remove_var("SERVER_PORT");
        env::remove_var("LASTFM_API_KEY");
        env::remove_var("UPLOAD_DIR");
        env::remove_var("ICECAST_PUBLIC_URL");
    }

    #[test]
    #[serial]
    fn test_config_from_env_required_vars() {
        cleanup_env();
        env::set_var("DATABASE_URL", "postgres://test:test@localhost/test");
        env::set_var("JWT_SECRET", "test-secret-thirtytwo-chars-minimum!!");

        let config = Config::from_env();
        assert_eq!(config.database_url, "postgres://test:test@localhost/test");
        assert_eq!(config.jwt_secret, "test-secret-thirtytwo-chars-minimum!!");
        assert_eq!(config.jwt_access_expiry, 900);
        assert_eq!(config.jwt_refresh_expiry, 604800);
        assert_eq!(config.server_host, "0.0.0.0");
        assert_eq!(config.server_port, 3001);
        assert!(config.lastfm_api_key.is_none());
        assert!(std::path::Path::new(&config.upload_dir).is_absolute());
        assert!(std::path::Path::new(&config.upload_dir).ends_with("uploads"));

        cleanup_env();
    }

    #[test]
    #[serial]
    fn test_config_from_env_custom_values() {
        cleanup_env();
        env::set_var("DATABASE_URL", "postgres://custom:pass@host/custom");
        env::set_var("JWT_SECRET", "custom-secret-thirtytwo-chars-min!!");
        env::set_var("JWT_ACCESS_EXPIRY", "3600");
        env::set_var("JWT_REFRESH_EXPIRY", "86400");
        env::set_var("SERVER_HOST", "127.0.0.1");
        env::set_var("SERVER_PORT", "8080");
        env::set_var("LASTFM_API_KEY", "lastfm-key-123");
        env::set_var("UPLOAD_DIR", "/custom/uploads");

        let config = Config::from_env();
        assert_eq!(config.database_url, "postgres://custom:pass@host/custom");
        assert_eq!(config.jwt_secret, "custom-secret-thirtytwo-chars-min!!");
        assert_eq!(config.jwt_access_expiry, 3600);
        assert_eq!(config.jwt_refresh_expiry, 86400);
        assert_eq!(config.server_host, "127.0.0.1");
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.lastfm_api_key, Some("lastfm-key-123".to_string()));
        assert_eq!(config.upload_dir, "/custom/uploads");

        cleanup_env();
    }

    #[test]
    #[serial]
    fn test_config_resolves_relative_upload_dir_to_absolute_path() {
        cleanup_env();
        env::set_var("DATABASE_URL", "postgres://test:test@localhost/test");
        env::set_var("JWT_SECRET", "test-secret-thirtytwo-chars-minimum!!");
        env::set_var("UPLOAD_DIR", "./../uploads");

        let config = Config::from_env();
        let p = std::path::Path::new(&config.upload_dir);
        assert!(p.is_absolute());
        // must be lexically normalized - no ./ or ../ segments
        let s = config.upload_dir.clone();
        assert!(!s.contains("/./"), "upload_dir not normalized: {s}");
        assert!(!s.contains("/../"), "upload_dir not normalized: {s}");
        assert!(!s.ends_with("/."), "upload_dir not normalized: {s}");

        cleanup_env();
    }
}
