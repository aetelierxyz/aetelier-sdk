//! `md_worker` — synchronised market-data collector with Parquet persistence.
//!
//! Connects to one or more exchange WebSocket feeds (per the TOML manifest),
//! decodes events, feeds them through a `MarketSynchronizer`, and emits
//! grid-aligned `MarketSnapshot`s to the configured output sinks.
//!
//! When built with the `parquet` feature (the default for the
//! `aetelier-md-worker` Docker image), this binary wires
//! [`aetelier_io::ParquetSnapshotFlusher`] into the per-worker sink set so
//! that `[[collect.output]] type = "parquet"` entries in the manifest
//! actually persist `orderbooks/`, `trades/`, `liquidations/`, `funding_rates/`,
//! `funding_settlements/` and `open_interests/` parquet files to disk. Without that wiring, the
//! parquet sink is silently dropped at sink-build time (see
//! `aetelier_connect::workers::output::build_sinks`).
//!
//! # Usage
//!
//! ```bash
//! md_worker --config configs/manifest.toml
//! RUST_LOG=info md_worker --config configs/manifest.toml
//! ```
//!
//! The worker runs until interrupted (Ctrl-C / SIGINT) or until the optional
//! `session.duration_hours` from the manifest expires.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use tokio::{sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;

use aetelier_connect::config::workers::market_worker_config::TransportKind;
use aetelier_connect::config::workers::{MarketWorkerConfig, MarketWorkerManifest};
use aetelier_connect::framework::FeedState;
use aetelier_connect::framework::feed::FeedSnapshot;
use aetelier_connect::workers::market_worker::MarketWorker;

#[cfg(feature = "parquet")]
use aetelier_io::ParquetSnapshotFlusher;

const UNSET_RUST_LOG_FILTER: &str = "info,rustls=off";
const EXIT_ALL_REJECTED: u8 = 78;

/// Synchronised market data collection worker.
#[derive(Parser, Debug)]
#[command(name = "md_worker", version, about)]
struct Cli {
    /// Path to a TOML worker manifest file.
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    // ── Tracing ─────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(log_filter(std::env::var(EnvFilter::DEFAULT_ENV).ok()))
        .with_target(true)
        .with_thread_ids(false)
        .init();

    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    let manifest = MarketWorkerManifest::from_toml(&cli.config)?;
    let configs = manifest.resolve_admitted()?;

    if configs.is_empty() {
        anyhow::bail!("manifest contains no worker entries");
    }

    tracing::info!(
        workers = configs.len(),
        config = %cli.config.display(),
        parquet_sink = cfg!(feature = "parquet"),
        "md_worker.starting"
    );

    // ── Shutdown signal ─────────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Ctrl-C handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("md_worker.shutdown_requested");
        let _ = shutdown_tx_clone.send(true);
    });

    // Optional session duration
    if let Some(dur_secs) = manifest.duration_secs() {
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(dur_secs)).await;
            tracing::info!(
                duration_secs = dur_secs,
                "md_worker.session_duration_elapsed"
            );
            let _ = shutdown_tx_clone.send(true);
        });
    }

    // ── Spawn workers ───────────────────────────────────────────────────
    let mut join_set = JoinSet::new();

    for (i, cfg) in configs.into_iter().enumerate() {
        let rx = shutdown_rx.clone();
        let label = format!("{}:{}", cfg.common.exchange, cfg.common.symbol);
        let grpc = cfg.transport == TransportKind::Grpc;

        // Build the worker, wiring a `ParquetSnapshotFlusher` when the
        // `parquet` feature is enabled. Without the flusher, any
        // `OutputSinkConfig::Parquet { dir }` entry in the manifest is
        // silently dropped by `build_sinks` and no parquet files are
        // written to disk.
        let worker = build_worker(cfg)?;

        tracing::info!(
            worker = i,
            label = %label,
            "md_worker.spawning_worker"
        );

        // Stagger startup to avoid thundering-herd on exchange endpoints.
        if i > 0 && !grpc {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        join_set.spawn(async move { (label, grpc, worker.run(rx).await) });
    }

    // ── Await all workers ───────────────────────────────────────────────
    let mut total_snapshots: u64 = 0;
    let mut ends: Vec<(String, Option<String>)> = Vec::new();

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((label, grpc, Ok(report))) => {
                tracing::info!(
                    exchange = %report.ingestion.exchange,
                    symbol = %report.ingestion.symbol,
                    total_events = report.ingestion.total_events,
                    snapshots = report.snapshots_produced,
                    flushes = report.flushes,
                    elapsed_secs = format!("{:.1}", report.ingestion.elapsed_secs),
                    reconnects = report.ingestion.reconnect_count,
                    "md_worker.worker_finished"
                );
                total_snapshots += report.snapshots_produced;
                ends.push((label, rejection(grpc, &report.feeds)));
            }
            Ok((label, _, Err(e))) => {
                tracing::error!(error = %e, "md_worker.worker_error");
                ends.push((label, None));
            }
            Err(e) => {
                tracing::error!(error = %e, "md_worker.worker_panic");
                ends.push((String::new(), None));
            }
        }
    }

    tracing::info!(
        total_snapshots = total_snapshots,
        "md_worker.all_workers_finished"
    );

    let code = exit_code(&ends);
    if code == EXIT_ALL_REJECTED {
        for (label, reason) in &ends {
            tracing::error!(
                worker = %label,
                reason = reason.as_deref().unwrap_or_default(),
                "md_worker.worker_rejected"
            );
        }
        tracing::error!(
            workers = ends.len(),
            exit_code = code,
            "md_worker.all_workers_rejected"
        );
    }
    Ok(ExitCode::from(code))
}

fn rejection(grpc: bool, feeds: &[FeedSnapshot]) -> Option<String> {
    if !grpc
        || feeds.is_empty()
        || feeds.iter().any(|feed| feed.state != FeedState::Rejected)
    {
        return None;
    }
    let mut reasons: Vec<&str> = feeds
        .iter()
        .filter_map(|feed| feed.reason.as_deref())
        .collect();
    reasons.sort_unstable();
    reasons.dedup();
    Some(reasons.join("; "))
}

fn exit_code(ends: &[(String, Option<String>)]) -> u8 {
    if !ends.is_empty() && ends.iter().all(|(_, rejected)| rejected.is_some()) {
        EXIT_ALL_REJECTED
    } else {
        0
    }
}

/// Construct a [`MarketWorker`] with the appropriate sink wiring for the
/// active feature set.
///
/// With `parquet`, the [`ParquetSnapshotFlusher`] from `aetelier-io` is
/// passed into [`MarketWorker::from_config_with_flusher`] so that any
/// `Parquet` sink declared in the manifest receives a concrete
/// `SnapshotFlusher` and actually writes parquet files. Without the
/// `parquet` feature, the worker uses the channel/terminal-only path.
#[cfg(feature = "parquet")]
fn build_worker(cfg: MarketWorkerConfig) -> anyhow::Result<MarketWorker> {
    Ok(MarketWorker::from_config_with_flusher(
        cfg,
        Some(Box::new(ParquetSnapshotFlusher)),
    )?)
}

#[cfg(not(feature = "parquet"))]
fn build_worker(cfg: MarketWorkerConfig) -> anyhow::Result<MarketWorker> {
    MarketWorker::from_config(cfg)
}

fn log_filter(rust_log: Option<String>) -> EnvFilter {
    rust_log
        .and_then(|directives| EnvFilter::try_new(directives).ok())
        .unwrap_or_else(|| EnvFilter::new(UNSET_RUST_LOG_FILTER))
}

#[cfg(test)]
mod tests {
    use tracing::Level;

    use super::*;

    fn admits(rust_log: Option<&str>) -> [bool; 4] {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(log_filter(rust_log.map(str::to_string)))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            [
                tracing::enabled!(target: "aetelier_connect::workers", Level::INFO),
                tracing::enabled!(target: "md_worker", Level::INFO),
                tracing::enabled!(target: "rustls::common_state", Level::WARN),
                tracing::enabled!(target: "rustls::client::hs", Level::DEBUG),
            ]
        })
    }

    #[test]
    fn an_unset_or_unparseable_rust_log_logs_info_without_rustls() {
        assert_eq!(admits(None), [true, true, false, false]);
        assert_eq!(admits(Some("info,rustls=loud")), [true, true, false, false]);
    }

    #[test]
    fn an_explicit_rust_log_keeps_full_control_over_rustls() {
        assert_eq!(admits(Some("info")), [true, true, true, false]);
        assert_eq!(admits(Some("debug")), [true, true, true, true]);
        assert_eq!(admits(Some("info,rustls=off")), [true, true, false, false]);
        assert_eq!(
            admits(Some("aetelier_connect=info,md_worker=info")),
            [true, true, false, false]
        );
    }

    fn feed(state: FeedState, reason: Option<&str>) -> FeedSnapshot {
        FeedSnapshot {
            id: "feed".to_string(),
            instrument: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(),
            datatype: aetelier_connect::framework::FeedDatatype::Transactions,
            state,
            partial: false,
            reason: reason.map(str::to_string),
        }
    }

    fn ended(label: &str, rejected: Option<&str>) -> (String, Option<String>) {
        (label.to_string(), rejected.map(str::to_string))
    }

    #[test]
    fn a_grpc_worker_ends_rejected_only_when_every_feed_is_rejected() {
        let denied = feed(FeedState::Rejected, Some("PermissionDenied: no scope"));
        assert_eq!(
            rejection(true, &[denied.clone(), denied.clone()]),
            Some("PermissionDenied: no scope".to_string())
        );
        assert_eq!(
            rejection(true, &[denied.clone(), feed(FeedState::Closed, None)]),
            None
        );
        assert_eq!(rejection(true, &[]), None);
        assert_eq!(rejection(false, &[denied]), None);
        assert_eq!(
            rejection(true, &[feed(FeedState::Failed, Some("give up"))]),
            None
        );
    }

    #[test]
    fn md_worker_exits_78_only_when_every_worker_ended_rejected() {
        assert_eq!(
            exit_code(&[
                ended("solana:a", Some("denied")),
                ended("solana:b", Some("denied"))
            ]),
            78
        );
        assert_eq!(
            exit_code(&[ended("solana:a", Some("denied")), ended("solana:b", None)]),
            0
        );
        assert_eq!(exit_code(&[ended("binance:BTC/USDT", None)]), 0);
        assert_eq!(exit_code(&[]), 0);
    }
}
