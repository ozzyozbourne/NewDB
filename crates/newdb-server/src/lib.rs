//! HTTP server over any [`Store`] (DESIGN.md section 8.3). Tested against
//! `newdb_testkit::Oracle`; the CLI runs it over `newdb_engine::Db`.
//!
//! Endpoints (the JSON shapes are documented in `crates/newdb-server/docs/api.md`):
//! - `POST /v1/traces`: OTLP/HTTP, protobuf or JSON by `content-type`, gzip accepted.
//!   Responds with an empty `ExportTraceServiceResponse` once the store acknowledges.
//! - `GET /v1/traces/{trace_id}?project=&payloads=true|false&stats=1`: the trace's spans
//!   as a nested tree built from `parent_span_id` (orphans at the top level). Payload
//!   bytes are spliced into the response as raw JSON, never parsed.
//! - `GET /v1/runs?project=&status=&run_type=&model=&session=&min_latency_ms=&min_tokens=&min_cost_usd=&since=&cursor=&limit=&stats=1`:
//!   newest first; `cursor` is the opaque key of the last run of the previous page.
//! - `GET /v1/search?project=&q=&limit=&stats=1`.
//! - `GET /healthz`.
//!
//! With `stats=1`, the response includes the `ReadStats` (and asks for timings).
//! Request bodies are capped at `ServerConfig::max_body_bytes`, before and after gzip.
//!
//! Store calls block (ingest waits for the group commit), so they run on tokio's blocking
//! pool, never on the I/O threads.

mod api;
mod query;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use newdb_otlp::Normalizer;
use newdb_types::{Result, Store};
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

pub struct ServerConfig {
    /// Use port 0 in tests to get a free port; read it from `ServerHandle::addr`.
    pub addr: SocketAddr,
    pub max_body_bytes: usize,
    pub normalizer: Arc<Normalizer>,
}

/// A running server. Dropping it shuts the server down.
pub struct ServerHandle {
    addr: SocketAddr,
    inner: Option<Running>,
}

struct Running {
    runtime: Runtime,
    stop: watch::Sender<bool>,
    accept: JoinHandle<()>,
}

impl Running {
    fn stop(self) {
        let _ = self.stop.send(true);
        let _ = self.runtime.block_on(self.accept);
    }
}

impl ServerHandle {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stops accepting, finishes in-flight requests, and returns.
    pub fn shutdown(mut self) {
        if let Some(r) = self.inner.take() {
            r.stop();
        }
    }

    /// Blocks until the server stops (for `newdb serve`). Only a fatal listener error
    /// stops it; to stop it on a signal, call [`ServerHandle::shutdown`] from the
    /// signal handler's thread instead of waiting here.
    pub fn wait(mut self) {
        if let Some(r) = self.inner.take() {
            let _ = r.runtime.block_on(r.accept);
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(r) = self.inner.take() {
            r.stop();
        }
    }
}

/// Idle keep-alive connections and slow clients are dropped after this long without a
/// complete request head.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Starts the server on its own tokio runtime and returns once it is listening.
pub fn start(store: Arc<dyn Store>, config: ServerConfig) -> Result<ServerHandle> {
    let threads = std::thread::available_parallelism().map_or(2, |n| n.get().clamp(1, 4));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(64)
        .thread_name("newdb-http")
        .enable_io()
        .enable_time()
        .build()?;
    // Bind here so a taken port is an error from `start`, not from a background task.
    let std_listener = std::net::TcpListener::bind(config.addr)?;
    std_listener.set_nonblocking(true)?;
    let addr = std_listener.local_addr()?;
    let listener = {
        let _guard = runtime.enter();
        tokio::net::TcpListener::from_std(std_listener)?
    };

    let app = Arc::new(api::App {
        store,
        normalizer: config.normalizer,
        max_body_bytes: config.max_body_bytes,
    });
    let (stop, stopped) = watch::channel(false);
    let accept = runtime.spawn(accept_loop(listener, app, stopped));
    Ok(ServerHandle {
        addr,
        inner: Some(Running {
            runtime,
            stop,
            accept,
        }),
    })
}

async fn accept_loop(
    listener: tokio::net::TcpListener,
    app: Arc<api::App>,
    mut stopped: watch::Receiver<bool>,
) {
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            _ = stopped.changed() => break,
            Some(_) = conns.join_next() => {}
            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok((s, _)) => s,
                    // Out of file descriptors and similar: back off instead of spinning.
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let app = app.clone();
                let mut stopped = stopped.clone();
                conns.spawn(async move {
                    let service = service_fn(move |req| {
                        let app = app.clone();
                        async move { Ok::<_, std::convert::Infallible>(app.handle(req).await) }
                    });
                    let conn = http1::Builder::new()
                        .timer(TokioTimer::new())
                        .header_read_timeout(HEADER_READ_TIMEOUT)
                        .serve_connection(TokioIo::new(stream), service);
                    tokio::pin!(conn);
                    tokio::select! {
                        _ = conn.as_mut() => {}
                        _ = stopped.changed() => {
                            conn.as_mut().graceful_shutdown();
                            let _ = conn.await;
                        }
                    }
                });
            }
        }
    }
    drop(listener);
    while conns.join_next().await.is_some() {}
}
