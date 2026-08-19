use std::{future::Future, time::Duration};

use axum::Router;

use super::AppState;

pub async fn run(listener: tokio::net::TcpListener, state: AppState) -> Result<(), std::io::Error> {
    serve_state_with_bounded_shutdown(listener, state, shutdown_signal(), Duration::from_secs(25))
        .await
}

pub(super) async fn serve_state_with_bounded_shutdown<F>(
    listener: tokio::net::TcpListener,
    state: AppState,
    shutdown: F,
    drain_timeout: Duration,
) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    let catalog = state.account_type_catalog.clone();
    serve_with_bounded_shutdown(
        listener,
        state.router(),
        async move {
            shutdown.await;
            catalog.shutdown().await;
        },
        drain_timeout,
    )
    .await
}

pub(super) async fn serve_with_bounded_shutdown<F>(
    listener: tokio::net::TcpListener,
    router: Router,
    shutdown: F,
    drain_timeout: Duration,
) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut drain = Box::pin(async move {
        shutdown.await;
        let _ = shutdown_tx.send(());
        tokio::time::sleep(drain_timeout).await;
    });
    let server = axum::serve(listener, router).with_graceful_shutdown(async {
        let _ = shutdown_rx.await;
    });
    let mut server = Box::pin(server.into_future());
    let result = tokio::select! {
        result = &mut server => result,
        _ = &mut drain => Ok(()),
    };
    result
}

pub(super) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = terminate.recv() => {},
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
