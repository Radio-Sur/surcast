use async_trait::async_trait;
use tokio::net::TcpStream;

#[async_trait]
pub trait StreamBackend: Send + Sync + 'static {
    async fn connect(&self, mount: &str, db: &sqlx::PgPool) -> Result<TcpStream, String>;
    fn name(&self) -> &'static str;
}
