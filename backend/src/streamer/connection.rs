use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::streamer::backend::StreamBackend;

pub struct IcecastBackend;

#[async_trait]
impl StreamBackend for IcecastBackend {
    async fn connect(&self, mount: &str, db: &sqlx::PgPool) -> Result<TcpStream, String> {
        let (addr, source_password) = crate::icecast::models::get_connection_config(db)
            .await
            .map_err(|e| format!("Failed to read icecast config: {:?}", e))?;

        let mount = crate::util::url_encode(mount);
        let auth = format!("Basic {}", BASE64.encode(format!("source:{source_password}")));

        let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr))
            .await
            .map_err(|_| "Icecast connection timeout".to_string())?
            .map_err(|e| format!("Icecast connection failed: {e}"))?;

        let headers = format!(
            "SOURCE /{mount} HTTP/1.0\r\n\
             Host: {addr}\r\n\
             Content-Type: audio/mpeg\r\n\
             Authorization: {auth}\r\n\
             \r\n",
        );
        stream
            .write_all(headers.as_bytes())
            .await
            .map_err(|e| format!("Write headers failed: {e}"))?;

        let mut buf = [0; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .map_err(|_| "Icecast response timeout".to_string())?
            .map_err(|e| format!("Read response failed: {e}"))?;

        let resp = String::from_utf8_lossy(&buf[..n]);
        if !resp.contains("200 OK") {
            return Err(format!("Icecast rejected: {}", resp.lines().next().unwrap_or("unknown")));
        }

        Ok(stream)
    }

    fn name(&self) -> &'static str {
        "Icecast"
    }
}
