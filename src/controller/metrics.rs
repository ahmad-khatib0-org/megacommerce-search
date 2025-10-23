use std::{
  fmt::Write,
  io::ErrorKind,
  net::SocketAddr,
  time::{Duration, Instant},
};

use dashmap::DashMap;
use hyper::{
  body::Incoming,
  header::{HeaderValue, ALLOW, CONTENT_LENGTH, CONTENT_TYPE},
  server::conn::http1::Builder,
  service::service_fn,
  Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use megacommerce_shared::models::errors::{BoxedErr, ErrorType, InternalError};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};
use tokio::{
  net::TcpListener,
  spawn,
  sync::{oneshot::channel, watch::Receiver},
  time::timeout,
};
use tracing::{error, info, warn};

static REGISTRY: Lazy<Registry> = Lazy::new(|| Registry::new());

static INFLIGHT_GAUGE: Lazy<IntGauge> = Lazy::new(|| {
  let g = IntGauge::new("meili_worker_inflight", "Number of messages currently being processed")
    .expect("should create gauge");
  REGISTRY.register(Box::new(g.clone())).expect("should register gauge");
  g
});

static PROCESSED_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
  let c = IntCounter::new("meili_worker_processed", "Total successfully processed messages")
    .expect("should create counter");
  REGISTRY.register(Box::new(c.clone())).expect("should register counter");
  c
});

static FAILED_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
  let c = IntCounter::new("meili_worker_failed", "Total failed message processing attempts")
    .expect("should create counter");
  REGISTRY.register(Box::new(c.clone())).expect("should register counter");
  c
});

static MEILI_DURATION_HIST: Lazy<Histogram> = Lazy::new(|| {
  // micro-to-seconds buckets
  let opt =
    HistogramOpts::new("meili_worker_meili_duration_seconds", "Meili push duration in seconds")
      .buckets(vec![0.000_1, 0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]);

  let h = Histogram::with_opts(opt).expect("should create histogram");
  REGISTRY.register(Box::new(h.clone())).expect("should register histogram");
  h
});

// Per-endpoint retry counts and committed offsets stored in concurrent maps.
static MEILI_RETRIES: Lazy<DashMap<String, u64>> = Lazy::new(|| DashMap::new());
static COMMITTED_OFFSETS: Lazy<DashMap<(String, i32), i64>> = Lazy::new(|| DashMap::new());

//
// Cache for last rendered metrics payload
//
struct CachedMetrics {
  bytes: Vec<u8>,
  expires_at: Instant,
}

static METRICS_CACHE: Lazy<Mutex<Option<CachedMetrics>>> = Lazy::new(|| Mutex::new(None));

/// Call when a message processing starts.
pub fn inc_inflight() {
  INFLIGHT_GAUGE.inc();
}

/// Call when a message processing finishes successfully.
pub fn dec_inflight() {
  // IntGauge provides `dec()` but doesn't protect against negative in Prometheus,
  // so we guard to avoid going below zero.
  if INFLIGHT_GAUGE.get() > 0 {
    INFLIGHT_GAUGE.dec();
  } else {
    warn!("dec_inflight called but inflight is already zero");
  }
}

pub fn inc_processed() {
  PROCESSED_COUNTER.inc();
}

pub fn inc_failed(_reason: &str) {
  // reason intentionally ignored for now; increment counter
  FAILED_COUNTER.inc();
}

pub fn inc_meili_retries(endpoint: &str) {
  MEILI_RETRIES.entry(endpoint.to_string()).and_modify(|v| *v += 1).or_insert(1);
}

pub fn set_committed_offset(topic: &str, partition: i32, offset: i64) {
  COMMITTED_OFFSETS.insert((topic.to_string(), partition), offset);
}

pub fn observe_meili_duration_secs(seconds: f64) {
  if seconds.is_sign_negative() || !seconds.is_finite() {
    return;
  }
  MEILI_DURATION_HIST.observe(seconds);
}

/// Produce the bytes for the /metrics response, using prometheus encoder.
/// This also writes custom metrics (retries & offsets) into the registry as ephemeral gauges.
/// We don't mutate the global registry itself; instead we create local ephemeral gauges
/// and register them to a new temporary Registry for encoding. That keeps the global
/// metric types stable while allowing dynamic label series.
fn render_metrics_bytes() -> Vec<u8> {
  // Create a fresh registry to hold the base global registry metrics plus ephemeral metrics.
  // We will gather from the global REGISTRY + custom ephemeral metrics for retries and offsets.
  // Note: prometheus::Registry does not allow "merging" easily; but we can gather
  // the global registry and manually append text for ephemeral metrics, or simply register
  // ephemeral metrics on a small registry and gather both. Simpler and safer: encode the
  // global registry, then append text for ephemeral maps (with proper escaping).
  let encoder = TextEncoder::new();
  let metrics_families = REGISTRY.gather();
  let mut buffer = Vec::with_capacity(1024);
  encoder.encode(&metrics_families, &mut buffer).expect("should encode base metrics");

  // Append retries and offsets in Prometheus exposition format.
  // We will format them as gauge/counter metrics with labels.
  // Ensure label escaping for double quotes, backslash, and newline.
  fn escape_prom_label(v: &str, out: &mut String) {
    for ch in v.chars() {
      match ch {
        '\\' => out.push_str(r"\\"),
        '\n' => out.push_str(r"\n"),
        '"' => out.push_str(r#"\""#),
        c => out.push(c),
      }
    }
  }

  // We'll append to a String then push bytes.
  let mut extra = String::with_capacity(512);
  // meili retries per endpoint (counter-like)
  extra.push_str("# HELP meili_worker_meili_retries Meili retry count per endpoint\n");
  extra.push_str("# TYPE meili_worker_meili_retries counter\n");

  for r in MEILI_RETRIES.iter() {
    let endpoint = r.key();
    let count = *r.value();
    extra.push_str("meili_worker_meili_retries{endpoint=\"");
    escape_prom_label(endpoint, &mut extra);
    extra.push_str("\"} ");
    let _ = write!(&mut extra, "{}\n", count);
  }

  // committed offsets per topic+partition (gauge)
  extra.push_str("# HELP meili_worker_committed_offset Last committed offset per partition\n");
  extra.push_str("# TYPE meili_worker_committed_offset gauge\n");
  for kv in COMMITTED_OFFSETS.iter() {
    let (topic, partition) = kv.key();
    let off = *kv.value();
    extra.push_str("meili_worker_committed_offset{topic=\"");
    escape_prom_label(topic, &mut extra);
    extra.push_str("\",partition=\"");
    let _ = write!(&mut extra, "{}", partition);
    extra.push_str("\"} ");
    let _ = write!(&mut extra, "{}\n", off);
  }

  buffer.extend_from_slice(extra.as_bytes());
  buffer
}

/// Get cached metrics bytes if still valid, otherwise regenerate and cache.
fn get_cached_metrics_bytes(cache_ttl: Duration) -> Vec<u8> {
  let mut guard = METRICS_CACHE.lock();
  let now = Instant::now();
  if let Some(ref cached) = *guard {
    if cached.expires_at > now {
      return cached.bytes.clone();
    }
  }

  let bytes = render_metrics_bytes();
  let expires_at = now + cache_ttl;
  *guard = Some(CachedMetrics { expires_at, bytes: bytes.clone() });

  bytes
}

/// Start metrics server using hyper. Returns JoinHandle for the background server task.
///
/// `shutdown_rx` is a tokio::sync::watch::Receiver<()> that will be
/// signalled to trigger graceful shutdown.
///
/// `cache_ttl` controls how long a generated metrics payload is cached
/// (default 500ms recommended).
///
/// `handler_timeout` is the maximum time to spend servicing a single request.
pub async fn start_metrics_server(
  addr: SocketAddr,
  mut shutdown_rx: Receiver<()>,
  handler_timeout: Option<Duration>,
  cache_ttl: Option<u32>,
) -> Result<(), BoxedErr> {
  let cache_time = cache_ttl.unwrap_or(500);
  let handler_time = handler_timeout.unwrap_or(Duration::from_secs(10));
  let (tx, rx) = channel::<Result<(), (BoxedErr, String)>>();

  // spawn the actual server in background
  spawn(async move {
    info!("metrics endpoint starting for {}", addr);

    let listener = match TcpListener::bind(addr).await {
      Ok(l) => l,
      Err(err) => {
        let _ = tx.send(Err((Box::new(err), format!("Failed to bind metrics addr {}:", addr))));
        return;
      }
    };
    info!("metrics endpoint listening on {}", addr);

    // Convert cache_time (ms) into Duration for per-request usage
    let cache_duration = Duration::from_millis(cache_time as u64);
    let handler_duration = handler_time;

    // Accept loop with graceful shutdown via shutdown_rx.changed()
    loop {
      tokio::select! {
        _ = shutdown_rx.changed() => {
          info!("shutdown signal received for metrics server");
          // signal success to caller
          let _ = tx.send(Ok(()));
          break;
        }
        accept = listener.accept() => {
          let (stream, peer_addr) = match accept {
            Ok((s, p)) => (s, p),
            Err(e) => {
              error!("Metrics accept error: {}", e);
              continue;
            }
          };

          // clone small values for the connection task
          let cache_duration = cache_duration.clone();
          let handler_duration = handler_duration.clone();

          spawn(async move {
            let io = TokioIo::new(stream);

            // per-connection service (one service per connection)
            let service = service_fn(move |req| {
              let cache_duration = cache_duration.clone();
              async move {
                match timeout(handler_duration, handle_metrics_request(req, cache_duration)).await {
                  Ok(Ok(resp)) => Ok::<_, hyper::Error>(resp),
                  Ok(Err(e)) => {
                    error!("handler error: {}", e);
                    let mut res = Response::new("internal error".into());
                    *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(res)
                  }
                  Err(_) => {
                    let mut res = Response::new("request timeout".into());
                    *res.status_mut() = StatusCode::REQUEST_TIMEOUT;
                    Ok(res)
                  }
                }
              }
            });

            // Serve this single connection (http1) — await the future here
            if let Err(e) = Builder::new().serve_connection(io, service).await {
              error!("Error serving connection from {}: {:?}", peer_addr, e);
            }
          });
        }
      }
    }

    info!("metrics server at {} shutting down", addr);
  });

  let ie = |err: BoxedErr, msg: &str| {
    Box::new(InternalError {
      path: "search.controller.metrics".to_string(),
      err,
      err_type: ErrorType::Internal,
      temp: false,
      msg: msg.to_string(),
    })
  };

  // wait for the oneshot result from the spawned task
  match rx.await {
    Err(err) => {
      // sender dropped; treat this as internal error
      Err(ie(Box::new(err), "metrics server didn't send a success/failed signal"))
    }
    Ok(Err((boxed, msg))) => Err(ie(boxed, &msg)),
    Ok(Ok(())) => Ok(()),
  }
}

/// Core request handler: only allows GET/HEAD /metrics
async fn handle_metrics_request(
  req: Request<Incoming>,
  cache_ttl: Duration,
) -> Result<Response<String>, std::io::Error> {
  // Only accept GET or HEAD
  match *req.method() {
    Method::GET | Method::HEAD => {}
    _ => {
      let mut resp = Response::new("Method Not Allowed".to_string());
      *resp.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
      resp.headers_mut().insert(ALLOW, HeaderValue::from_static("GET, HEAD"));
      return Ok(resp);
    }
  }

  // Only path /metrics
  if req.uri().path() != "/metrics" {
    let mut resp = Response::new("Not Found".to_string());
    *resp.status_mut() = StatusCode::NOT_FOUND;
    return Ok(resp);
  }

  // Serve cached bytes if fresh, otherwise regenerate.
  let bytes = get_cached_metrics_bytes(cache_ttl);

  // HEAD should not include body per HTTP semantics but it's okay to return empty body.
  let body = if req.method() == Method::HEAD {
    String::new()
  } else {
    String::from_utf8(bytes).map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?
  };

  let content_length = HeaderValue::from_str(&body.len().to_string()).unwrap();
  let encoder = TextEncoder::new();
  let content_type = HeaderValue::from_str(encoder.format_type()).unwrap();

  let mut resp = Response::new(body);
  *resp.status_mut() = StatusCode::OK;

  resp.headers_mut().insert(CONTENT_TYPE, content_type);
  resp.headers_mut().insert(CONTENT_LENGTH, content_length);

  Ok(resp)
}
