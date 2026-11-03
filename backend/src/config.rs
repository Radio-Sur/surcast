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
    /// Directory holding the music library. API uploads and files dropped
    /// in manually (e.g. over FTP) live here under their original names;
    /// the lazy scanner imports them into the database.
    pub music_dir: String,
    pub icecast_public_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self::from_vars(|name| env::var(name))
    }

    fn from_vars(var: impl Fn(&str) -> Result<String, env::VarError>) -> Self {
        let jwt_secret = var("JWT_SECRET").expect("JWT_SECRET must be set");
        assert!(
            jwt_secret.len() >= 32 && jwt_secret != "change-me-to-a-random-secret",
            "JWT_SECRET must be a random secret of at least 32 bytes (got {}). \
             If .env is correct, check for an exported JWT_SECRET overriding it.",
            jwt_secret.len()
        );
        Self {
            database_url: var("DATABASE_URL").expect("DATABASE_URL must be set"),
            jwt_secret,
            jwt_access_expiry: var("JWT_ACCESS_EXPIRY")
                .unwrap_or_else(|_| "900".to_string())
                .parse()
                .expect("JWT_ACCESS_EXPIRY must be a number"),
            jwt_refresh_expiry: var("JWT_REFRESH_EXPIRY")
                .unwrap_or_else(|_| "604800".to_string())
                .parse()
                .expect("JWT_REFRESH_EXPIRY must be a number"),
            server_host: var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            server_port: var("SERVER_PORT")
                .unwrap_or_else(|_| "3001".to_string())
                .parse()
                .expect("SERVER_PORT must be a number"),
            lastfm_api_key: var("LASTFM_API_KEY").ok(),
            music_dir: absolute_dir(var("MUSIC_DIR").unwrap_or_else(|_| "./music".to_string())),
            icecast_public_url: var("ICECAST_PUBLIC_URL").unwrap_or_default(),
        }
    }
}

fn absolute_dir(path: String) -> String {
    let path = PathBuf::from(path);
    let absolute = if path.is_absolute() {
        normalize_lexically(&path)
    } else {
        let joined = env::current_dir()
            .expect("failed to resolve MUSIC_DIR from the current directory")
            .join(path);
        normalize_lexically(&joined)
    };
    absolute.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(values: &[(&str, &str)]) -> Config {
        Config::from_vars(|name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
                .ok_or(env::VarError::NotPresent)
        })
    }

    #[test]
    fn test_config_from_env_required_vars() {
        let config = config(&[
            ("DATABASE_URL", "postgres://test:test@localhost/test"),
            ("JWT_SECRET", "test-secret-thirtytwo-chars-minimum!!"),
        ]);
        assert_eq!(config.database_url, "postgres://test:test@localhost/test");
        assert_eq!(config.jwt_secret, "test-secret-thirtytwo-chars-minimum!!");
        assert_eq!(config.jwt_access_expiry, 900);
        assert_eq!(config.jwt_refresh_expiry, 604800);
        assert_eq!(config.server_host, "0.0.0.0");
        assert_eq!(config.server_port, 3001);
        assert!(config.lastfm_api_key.is_none());
        // No MUSIC_DIR set: derives to ./music.
        assert!(std::path::Path::new(&config.music_dir).is_absolute());
        assert!(std::path::Path::new(&config.music_dir).ends_with("music"));
    }

    #[test]
    fn test_config_music_dir_explicit_value() {
        let config = config(&[
            ("DATABASE_URL", "postgres://test:test@localhost/test"),
            ("JWT_SECRET", "test-secret-thirtytwo-chars-minimum!!"),
            ("MUSIC_DIR", "/srv/music"),
        ]);
        assert_eq!(config.music_dir, "/srv/music");
    }

    #[test]
    fn test_config_from_env_custom_values() {
        let config = config(&[
            ("DATABASE_URL", "postgres://custom:pass@host/custom"),
            ("JWT_SECRET", "custom-secret-thirtytwo-chars-min!!"),
            ("JWT_ACCESS_EXPIRY", "3600"),
            ("JWT_REFRESH_EXPIRY", "86400"),
            ("SERVER_HOST", "127.0.0.1"),
            ("SERVER_PORT", "8080"),
            ("LASTFM_API_KEY", "lastfm-key-123"),
            ("MUSIC_DIR", "/custom/music"),
        ]);
        assert_eq!(config.database_url, "postgres://custom:pass@host/custom");
        assert_eq!(config.jwt_secret, "custom-secret-thirtytwo-chars-min!!");
        assert_eq!(config.jwt_access_expiry, 3600);
        assert_eq!(config.jwt_refresh_expiry, 86400);
        assert_eq!(config.server_host, "127.0.0.1");
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.lastfm_api_key, Some("lastfm-key-123".to_string()));
        assert_eq!(config.music_dir, "/custom/music");
    }

    #[test]
    fn test_config_resolves_relative_music_dir_to_absolute_path() {
        let config = config(&[
            ("DATABASE_URL", "postgres://test:test@localhost/test"),
            ("JWT_SECRET", "test-secret-thirtytwo-chars-minimum!!"),
            ("MUSIC_DIR", "./../music"),
        ]);
        let p = std::path::Path::new(&config.music_dir);
        assert!(p.is_absolute());
        // must be lexically normalized - no ./ or ../ segments
        let s = config.music_dir.clone();
        assert!(!s.contains("/./"), "music_dir not normalized: {s}");
        assert!(!s.contains("/../"), "music_dir not normalized: {s}");
        assert!(!s.ends_with("/."), "music_dir not normalized: {s}");
    }

    #[test]
    fn rejects_short_and_example_signing_secrets() {
        for secret in ["", "short", "change-me-to-a-random-secret"] {
            assert!(std::panic::catch_unwind(|| config(&[("DATABASE_URL", "unused"), ("JWT_SECRET", secret)])).is_err());
        }
    }
}
