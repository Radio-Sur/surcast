pub mod handlers;
pub mod models;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct IcecastManager {
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    binary: Arc<std::sync::Mutex<Option<String>>>,
    port: Arc<std::sync::Mutex<Option<i32>>>,
    base_dir: PathBuf,
}

impl IcecastManager {
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            child: Arc::new(Mutex::new(None)),
            binary: Arc::new(std::sync::Mutex::new(find_icecast())),
            port: Arc::new(std::sync::Mutex::new(None)),
            base_dir,
        }
    }

    fn dir(&self) -> &Path {
        &self.base_dir
    }

    fn config_path(&self) -> PathBuf {
        self.dir().join("icecast.xml")
    }

    fn icecast_log(&self) -> PathBuf {
        self.dir().join("icecast-server.log")
    }

    fn binary_path(&self) -> Result<String, String> {
        let guard = self.binary.lock().unwrap_or_else(|e| e.into_inner());
        guard.clone().ok_or_else(|| "icecast binary not found. Install it via: nix develop / brew install icecast / apt install icecast / docker compose up -d icecast".into())
    }

    fn prepare_directories(&self) -> Result<(), String> {
        std::fs::create_dir_all(self.dir())
            .and_then(|_| std::fs::create_dir_all(self.dir().join("web")))
            .and_then(|_| std::fs::create_dir_all(self.dir().join("admin")))
            .map_err(|e| format!("Failed to create Icecast dirs: {e}"))?;
        std::fs::write(self.dir().join("mime.types"), MIME_TYPES).ok();
        Ok(())
    }

    fn copy_share_files(&self) {
        if let Ok(binary) = self.binary_path() {
            let binary_path = std::path::Path::new(&binary);
            if let Some(prefix) = binary_path.parent().and_then(|p| p.parent()) {
                let share_admin = prefix.join("share").join("icecast").join("admin");
                let share_web = prefix.join("share").join("icecast").join("web");
                let target_admin = self.dir().join("admin");
                let target_web = self.dir().join("web");
                copy_dir_recursive(&share_admin, &target_admin);
                copy_dir_recursive(&share_web, &target_web);
            }
        }
    }

    fn write_config(
        &self,
        port: i32,
        source_password: &str,
        admin_user: &str,
        admin_password: &str,
        drop_privileges: bool,
    ) -> Result<(), String> {
        let config = generate_config(self.dir(), port, source_password, admin_user, admin_password, drop_privileges);
        let config_path = self.config_path();
        std::fs::write(&config_path, &config).map_err(|e| format!("Failed to write config: {e}"))?;
        std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600)).ok();
        Ok(())
    }

    async fn spawn_process(&self, binary: &str) -> Result<tokio::process::Child, String> {
        let log = std::fs::File::create(self.icecast_log()).map_err(|e| format!("Failed to create log file: {e}"))?;

        let mut child = Command::new(binary)
            .arg("-c")
            .arg(self.config_path())
            .stdout(log.try_clone().map_err(|e| format!("{e}"))?)
            .stderr(log)
            .spawn()
            .map_err(|e| format!("Failed to start icecast: {e}"))?;

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        match child.try_wait() {
            Ok(Some(status)) if !status.success() => {
                let err = std::fs::read_to_string(self.icecast_log()).unwrap_or_default();
                return Err(format!("Icecast exited with {status}\nLog:\n{err}"));
            }
            _ => {}
        }

        Ok(child)
    }

    pub async fn start(&self, port: i32, source_password: &str, admin_user: &str, admin_password: &str) -> Result<String, String> {
        let mut guard = self.child.lock().await;
        if guard.is_some() {
            return Err("Icecast already running".into());
        }

        let binary = self.binary_path()?;
        tracing::info!(?binary, port, dir = %self.dir().display(), "Starting Icecast");

        kill_zombie_icecast(&self.config_path()).await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if port_is_listening(port as u16).await {
            return Err(format!("Port {port} is already in use by another process"));
        }

        self.prepare_directories()?;
        self.copy_share_files();

        *self.port.lock().unwrap_or_else(|e| e.into_inner()) = Some(port);
        let root = running_as_root();
        if root {
            tracing::info!("Running as root, dropping Icecast privileges to icecast2");
            chown_icecast_dir(self.dir());
        }
        self.write_config(port, source_password, admin_user, admin_password, root)?;

        let child = self.spawn_process(&binary).await?;
        *guard = Some(child);
        Ok(format!("Icecast started on port {port}"))
    }

    pub async fn stop(&self) -> Result<String, String> {
        let mut guard = self.child.lock().await;
        *self.port.lock().unwrap_or_else(|e| e.into_inner()) = None;
        match guard.take() {
            Some(mut child) => {
                child.kill().await.map_err(|e| format!("Failed to kill icecast: {e}"))?;
                child.wait().await.ok();
                Ok("Icecast stopped".into())
            }
            None => Err("Icecast is not running".into()),
        }
    }

    pub async fn restart(&self, port: i32, source_password: &str, admin_user: &str, admin_password: &str) -> Result<String, String> {
        let _ = self.stop().await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        self.start(port, source_password, admin_user, admin_password).await
    }

    pub async fn is_running(&self) -> bool {
        let mut guard = self.child.lock().await;
        if let Some(child) = guard.as_mut() {
            let exited = child.try_wait().ok().flatten().is_some();
            if exited {
                *guard = None;
            } else {
                return true;
            }
        }
        false
    }

    /// Check if the icecast process is running, with a fallback port probe
    /// if no child process handle is available (e.g. after backend restart).
    pub async fn is_running_on_port(&self, port: u16) -> bool {
        if self.is_running().await {
            return true;
        }
        port_is_listening(port).await
    }
}

fn generate_config(dir: &Path, port: i32, source_password: &str, admin_user: &str, admin_password: &str, drop_privileges: bool) -> String {
    let logdir = dir.display();
    let security = if drop_privileges {
        r#"
    <security>
        <chroot>0</chroot>
        <changeowner>
            <user>icecast2</user>
            <group>icecast2</group>
        </changeowner>
    </security>"#
            .to_string()
    } else {
        String::new()
    };
    format!(
        r#"<icecast>
    <limits>
        <clients>100</clients>
        <sources>10</sources>
        <queue-size>524288</queue-size>
        <client-timeout>30</client-timeout>
        <header-timeout>15</header-timeout>
        <source-timeout>10</source-timeout>
        <burst-on-connect>1</burst-on-connect>
        <burst-size>65535</burst-size>
    </limits>
    <authentication>
        <source-password>{source_password}</source-password>
        <admin-user>{admin_user}</admin-user>
        <admin-password>{admin_password}</admin-password>
    </authentication>
    <hostname>localhost</hostname>
    <listen-socket>
        <port>{port}</port>
        <bind-address>0.0.0.0</bind-address>
    </listen-socket>
    <http-headers>
        <header name="Access-Control-Allow-Origin" value="*"/>
    </http-headers>
    <paths>
        <logdir>{logdir}</logdir>
        <webroot>{logdir}/web</webroot>
        <adminroot>{logdir}/admin</adminroot>
        <mime-types>{logdir}/mime.types</mime-types>
    </paths>
    <fileserve>1</fileserve>{security}
</icecast>"#
    )
}

const MIME_TYPES: &str = r#"text/html html htm
text/css css
text/plain txt
image/png png
image/x-icon ico
image/svg+xml svg
image/gif gif
image/jpeg jpeg jpg
application/javascript js
application/json json
application/xml xml
application/octet-stream bin
"#;

fn running_as_root() -> bool {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim() == "0")
        .unwrap_or(false)
}

fn chown_icecast_dir(dir: &Path) {
    let _ = std::process::Command::new("chown")
        .args(["-R", "icecast2:icecast2"])
        .arg(dir)
        .output();
}

fn find_icecast() -> Option<String> {
    for name in ["icecast", "icecast2"] {
        if let Ok(output) = std::process::Command::new("which").arg(name).output() {
            if output.status.success() {
                let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !path.is_empty() {
                    return Some(path);
                }
            }
        }
    }

    if let Ok(entries) = std::fs::read_dir("/nix/store") {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.to_string_lossy().contains("icecast") && p.join("bin/icecast").exists() {
                return Some(p.join("bin/icecast").to_string_lossy().to_string());
            }
        }
    }

    for path in [
        "/usr/bin/icecast",
        "/usr/bin/icecast2",
        "/usr/local/bin/icecast",
        "/usr/local/bin/icecast2",
        "/opt/homebrew/bin/icecast",
        "/opt/homebrew/bin/icecast2",
    ] {
        if std::path::Path::new(path).exists() {
            return Some(path.to_string());
        }
    }

    None
}

fn is_icecast_executable(path: &Path) -> bool {
    matches!(path.file_name().and_then(|name| name.to_str()), Some("icecast" | "icecast2"))
}

fn uses_config(cmdline: &[u8], cwd: &Path, config: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;

    let args: Vec<_> = cmdline.split(|byte| *byte == 0).collect();
    args.windows(2).any(|args| {
        if args[0] != b"-c" {
            return false;
        }
        let path = cwd.join(std::ffi::OsStr::from_bytes(args[1]));
        let path = path.canonicalize().unwrap_or(path);
        let config = config.canonicalize().unwrap_or_else(|_| config.to_path_buf());
        path == config
    })
}

/// Reclaim only a previous child using this manager's configuration. Other
/// stations, development servers and parallel integration tests are not ours.
async fn kill_zombie_icecast(config: &Path) {
    let config = std::env::current_dir().unwrap_or_default().join(config);
    if let Ok(proc) = std::fs::read_dir("/proc") {
        for entry in proc.flatten() {
            let pid_str = entry.file_name().to_string_lossy().to_string();
            if let Ok(pid) = pid_str.parse::<i32>() {
                if let Ok(exe) = std::fs::read_link(format!("/proc/{pid}/exe")) {
                    if is_icecast_executable(&exe) {
                        if let (Ok(cmdline), Ok(cwd)) = (
                            std::fs::read(format!("/proc/{pid}/cmdline")),
                            std::fs::read_link(format!("/proc/{pid}/cwd")),
                        ) {
                            if uses_config(&cmdline, &cwd, &config) {
                                tracing::warn!(pid, config = %config.display(), "Killing previous Icecast instance for this configuration");
                                let _ = std::process::Command::new("kill").arg("-9").arg(pid.to_string()).output();
                            }
                        }
                    }
                }
            }
        }
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    if let Ok(entries) = std::fs::read_dir(src) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name() else { continue };
            if path.is_dir() {
                let sub_dst = dst.join(name);
                let _ = std::fs::create_dir_all(&sub_dst);
                copy_dir_recursive(&path, &sub_dst);
            } else {
                let _ = std::fs::copy(&path, dst.join(name));
            }
        }
    }
}

async fn port_is_listening(port: u16) -> bool {
    std::process::Command::new("ss")
        .args(["-ltn", "sport", "=", &format!(":{}", port)])
        .output()
        .ok()
        .map(|o| {
            let out = String::from_utf8_lossy(&o.stdout);
            out.lines().skip(1).any(|line| !line.trim().is_empty())
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_config_contains_values() {
        let dir = std::path::Path::new("/tmp/icecast");
        let config = generate_config(dir, 8000, "sourcepass", "adminuser", "adminpass", false);
        assert!(config.contains("8000"));
        assert!(config.contains("sourcepass"));
        assert!(config.contains("adminuser"));
        assert!(config.contains("adminpass"));
        assert!(config.contains("<icecast>"));
        assert!(config.contains("</icecast>"));
        assert!(!config.contains("changeowner"));
    }

    #[test]
    fn test_generate_config_privilege_drop() {
        let dir = std::path::Path::new("/tmp/icecast");
        let root_config = generate_config(dir, 8000, "spw", "au", "ap", true);
        assert!(root_config.contains("changeowner"));
        assert!(root_config.contains("<user>icecast2</user>"));
        assert!(root_config.contains("<group>icecast2</group>"));
        let non_root_config = generate_config(dir, 8000, "spw", "au", "ap", false);
        assert!(!non_root_config.contains("changeowner"));
    }

    #[test]
    fn test_generate_config_different_port() {
        let dir = std::path::Path::new("/tmp/icecast");
        let config = generate_config(dir, 9000, "pw", "au", "ap", false);
        assert!(config.contains("9000"));
        assert!(!config.contains("8000"));
    }

    #[tokio::test]
    async fn test_port_is_listening_returns_true_for_open_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(port_is_listening(port).await);
    }

    #[tokio::test]
    async fn start_does_not_kill_another_process_using_the_requested_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let dir = tempfile::tempdir().unwrap();
        let manager = IcecastManager::new(dir.path().to_path_buf());
        // No real Icecast binary is needed: the occupied-port check runs
        // before spawning anything.
        *manager.binary.lock().unwrap() = Some("icecast".into());
        let error = manager.start(port.into(), "pw", "admin", "pw").await.unwrap_err();
        assert!(error.contains("already in use"));
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok());
    }

    #[test]
    fn cleanup_matches_only_the_same_icecast_configuration() {
        let cwd = Path::new("/workspace/backend");
        assert!(uses_config(
            b"icecast\0-c\0/workspace/.icecast/icecast.xml\0",
            cwd,
            Path::new("/workspace/.icecast/icecast.xml")
        ));
        assert!(uses_config(
            b"icecast\0-c\0.icecast/icecast.xml\0",
            cwd,
            Path::new("/workspace/backend/.icecast/icecast.xml")
        ));
        assert!(!uses_config(
            b"icecast\0-c\0/tmp/test/icecast.xml\0",
            cwd,
            Path::new("/workspace/.icecast/icecast.xml")
        ));
        assert!(!uses_config(b"icecast\0", cwd, Path::new("/workspace/.icecast/icecast.xml")));
    }

    #[test]
    fn test_icecast_manager_config_path() {
        let manager = IcecastManager::new(std::path::Path::new("/tmp/icecast-test").to_path_buf());
        let config = manager.config_path();
        assert!(config.ends_with("icecast.xml"));
    }

    #[test]
    fn test_icecast_executable_detection_excludes_test_binaries() {
        assert!(is_icecast_executable(Path::new("/nix/store/hash-icecast/bin/icecast")));
        assert!(is_icecast_executable(Path::new("/usr/bin/icecast2")));
        assert!(!is_icecast_executable(Path::new("/workspace/target/debug/api_icecast")));
        assert!(!is_icecast_executable(Path::new("/usr/bin/not-icecast")));
    }
}
