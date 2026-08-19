use chatgpt2api_rust::{AppConfig, AppState, run};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = std::env::var("RUST_BIND").unwrap_or_else(|_| "127.0.0.1:8099".to_owned());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let state = AppState::new(AppConfig::from_env())?;
    run(listener, state).await?;
    Ok(())
}
