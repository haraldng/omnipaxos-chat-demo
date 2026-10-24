use clap::Parser;
use omni_chat::{cli::Cli, server};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,omni_chat=info")),
        )
        .with_target(false)
        .init();
    server::run(Cli::parse().command).await
}
