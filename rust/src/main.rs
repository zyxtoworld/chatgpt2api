use chatgpt2api_rust::{AppConfig, AppState, run};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState::new(AppConfig::from_env())?;
    let bind = std::env::var("RUST_BIND").unwrap_or_else(|_| {
        if std::env::var_os("RUST_PRODUCTION").is_some() {
            "0.0.0.0:80".to_owned()
        } else {
            "127.0.0.1:8099".to_owned()
        }
    });
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    run(listener, state).await?;
    Ok(())
}
