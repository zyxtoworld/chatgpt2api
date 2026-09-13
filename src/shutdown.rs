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
    let editable_workers = state.editable_workers.clone();
    let storage_backend = state.storage_backend.clone();
    let admission_state = state.clone();
    serve_with_bounded_shutdown_and_cleanup(
        listener,
        state.router(),
        shutdown,
        move || {
            admission_state.begin_http_shutdown();
            editable_workers.begin_shutdown();
            catalog.begin_shutdown();
            async move {
                tokio::join!(
                    editable_workers.finish_shutdown(),
                    catalog.finish_shutdown(),
                    async move {
                        if let Some(storage_backend) = storage_backend {
                            storage_backend.close().await;
                        }
                    }
                );
            }
        },
        drain_timeout,
    )
    .await
}

#[cfg(test)]
pub(super) async fn serve_with_bounded_shutdown<F>(
    listener: tokio::net::TcpListener,
    router: Router,
    shutdown: F,
    drain_timeout: Duration,
) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_bounded_shutdown_and_cleanup(listener, router, shutdown, || async {}, drain_timeout)
        .await
}

async fn serve_with_bounded_shutdown_and_cleanup<F, O, C>(
    listener: tokio::net::TcpListener,
    router: Router,
    shutdown: F,
    on_shutdown: O,
    drain_timeout: Duration,
) -> Result<(), std::io::Error>
where
    F: Future<Output = ()> + Send + 'static,
    O: FnOnce() -> C + Send + 'static,
    C: Future<Output = ()> + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, router).with_graceful_shutdown(async {
        let _ = shutdown_rx.await;
    });
    let mut server = Box::pin(server.into_future());
    let mut shutdown = Box::pin(shutdown);
    tokio::select! {
        biased;
        _ = &mut shutdown => {}
        result = &mut server => return result,
    }

    let deadline = tokio::time::Instant::now() + drain_timeout;
    let cleanup = begin_shutdown_before_graceful_signal(on_shutdown, || {
        let _ = shutdown_tx.send(());
    });
    let drain = async {
        let (server_result, ()) = tokio::join!(&mut server, cleanup);
        server_result
    };
    match tokio::time::timeout_at(deadline, drain).await {
        Ok(result) => result,
        Err(_) => Ok(()),
    }
}

fn begin_shutdown_before_graceful_signal<O, C, S>(on_shutdown: O, signal_graceful: S) -> C
where
    O: FnOnce() -> C,
    S: FnOnce(),
{
    let cleanup = on_shutdown();
    signal_graceful();
    cleanup
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::begin_shutdown_before_graceful_signal;

    #[test]
    fn owner_and_http_admission_begin_before_graceful_signal() {
        let began = Cell::new(false);
        let signaled = Cell::new(false);
        let cleanup = begin_shutdown_before_graceful_signal(
            || {
                began.set(true);
                "cleanup"
            },
            || {
                assert!(began.get(), "graceful signal preceded admission fences");
                signaled.set(true);
            },
        );
        assert!(signaled.get());
        assert_eq!(cleanup, "cleanup");
    }
}
