//! `validate` — post-hoc parquet integrity & invariant checker for the
//! `md_worker` data lake.
//!
//! Walks each expected exchange directly under `--data-dir` (defaults to
//! `/data`) for the leaves `BufferedSink + ParquetSnapshotFlusher` writes:
//!
//! ```text
//! /data
//! ├── binance/
//! │   ├── orderbooks/<exchange>_<symbol>_ob_sync_<YYYYMMDD>_<HHMMSS.uuuuuu>[-N].parquet
//! │   ├── trades/<exchange>_<symbol>_trades_sync_<YYYYMMDD>_<HHMMSS.uuuuuu>[-N].parquet
//! │   └── <market_type>/<datatype>/<binding_id>/<YYYY-MM-DD>/{index.jsonl,*.parquet}
//! └── solana/transactions/solana_<address>_txs_sync_<YYYYMMDD>_<HHMMSS.uuuuuu>[-N].parquet
//! ```
//!
//! A leaf with an `index.jsonl` is checked over the files it lists; a file
//! present but unlisted is reported as pending, never validated. Each file
//! runs a battery of 13 tests; the report goes to stdout and `--report-out`
//! (JSON) and merges into a persistent `--state-file` across runs.
//!
//! The tests cover three orthogonal concerns:
//!
//! * **structure** — files exist per exchange/datatype, are non-empty,
//!   match the expected filename convention, and have rows.
//! * **temporal correctness** — timestamps are non-decreasing, the
//!   `last_ts − first_ts` span lines up with `flush_threshold × grid`
//!   within tolerance, and adjacent files for the same symbol in one
//!   exchange or binding do not overlap in time.
//! * **content correctness** — orderbook snapshots are not crossed
//!   (best bid < best ask), prices and sizes are strictly positive,
//!   trade IDs are unique within a file, orderbook snapshot
//!   timestamps are unique, and transaction rows are unique per
//!   `(address, signature)` across a leaf day, ordered by
//!   `(slot, tx_index)` per address and carry their block context.
//!
//! Exit code is `0` if every test passed, `1` if any test failed, `2`
//! on a fatal error (e.g. unreadable state file). Designed to be run
//! periodically from cron — see `serve/validate.Dockerfile`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

use aetelier_io::leaf_index::{INDEX_FILE, pending_files, read_index, read_leaf_index};
use aetelier_io::naming::{FileKind, FileName, parse_file_name};
use aetelier_io::onchain::{
    LeafKeys, check_transactions, read_transaction_keys, read_transactions_parquet,
};
use aetelier_io::orderbooks::read_ob_parquet;
use aetelier_io::trades::read_trades_parquet;
use aetelier_types::onchain::ChainAddress;
use aetelier_types::orderbooks::{Orderbook, OrderbookTarget, OrderbookTargetData};
use aetelier_types::trades::Trade;

// ─────────────────────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────────────────────

/// Validate parquet files produced by `md_worker`.
#[derive(Parser, Debug)]
#[command(name = "validate", version, about)]
struct Cli {
    /// Root directory with the per-exchange parquet output.
    #[arg(long, default_value = "/data")]
    data_dir: PathBuf,

    /// Persistent state file used for cumulative statistics across runs.
    #[arg(long, default_value = "/state/validate_state.json")]
    state_file: PathBuf,

    /// Per-run JSON report.
    #[arg(long, default_value = "/state/last_report.json")]
    report_out: PathBuf,

    /// `flush_threshold` from the manifest (number of grid ticks per
    /// flush). Used to compute the expected file time span.
    #[arg(long, default_value_t = 3600)]
    flush_threshold: u64,

    /// Grid period in microseconds. The expected file span is
    /// `(flush_threshold - 1) * grid_period_us`.
    #[arg(long, default_value_t = 100_000)]
    grid_period_us: u64,

    /// Tolerance for the flush-span test, as a fraction of the
    /// expected span (default 0.20 → ±20 %).
    #[arg(long, default_value_t = 0.20)]
    span_tolerance: f64,

    /// Exchanges expected to have data (used for the "files present"
    /// test). Comma-separated; defaults to the docker-compose set.
    #[arg(long, default_value = "binance,coinbase,kraken", value_delimiter = ',')]
    expect_exchanges: Vec<String>,

    /// Print the per-file detail table in addition to the summary.
    #[arg(long)]
    verbose: bool,

    /// Skip files that have already been validated in a prior run
    /// (matched by absolute path + size). Useful when running on a
    /// rolling cadence over a growing data lake.
    #[arg(long)]
    skip_seen: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Test catalogue
// ─────────────────────────────────────────────────────────────────────────────

/// Stable IDs for the 13 invariants the validator enforces.
///
/// Stored verbatim in the state file so historical runs remain
/// interpretable as the test suite evolves.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TestId {
    /// T01: every expected `<exchange>` kind has ≥1 file across its leaves.
    FilesPresent,
    /// T02: each parquet file is non-empty (size > 0, ≥1 row).
    FileNonEmpty,
    /// T03: filename matches `<exchange>_<symbol>_<kind>_sync_<YYYYMMDD>_<HHMMSS.uuuuuu>[-N].parquet`.
    FilenameConvention,
    /// T04: distinct timestamps within a file are non-decreasing.
    MonotonicTimestamps,
    /// T05: `last_ts − first_ts` ≈ `flush_threshold × grid` within tolerance.
    FlushSpan,
    /// T06: orderbook snapshot timestamps are unique within a file.
    UniqueOrderbookTs,
    /// T07: trade IDs are unique within a file.
    UniqueTradeIds,
    /// T08: every orderbook snapshot satisfies `best_bid < best_ask`.
    NotCrossedOrderbook,
    /// T09: every price and size is strictly positive.
    PositivePricesSizes,
    /// T10: adjacent files for the same `(exchange or binding, symbol,
    /// kind)` do not overlap in time.
    NoTemporalOverlap,
    UniqueTransactionKeys,
    OrderedTransactionPositions,
    TransactionBlockContext,
}

impl TestId {
    fn code(&self) -> &'static str {
        match self {
            TestId::FilesPresent => "T01",
            TestId::FileNonEmpty => "T02",
            TestId::FilenameConvention => "T03",
            TestId::MonotonicTimestamps => "T04",
            TestId::FlushSpan => "T05",
            TestId::UniqueOrderbookTs => "T06",
            TestId::UniqueTradeIds => "T07",
            TestId::NotCrossedOrderbook => "T08",
            TestId::PositivePricesSizes => "T09",
            TestId::NoTemporalOverlap => "T10",
            TestId::UniqueTransactionKeys => "T11",
            TestId::OrderedTransactionPositions => "T12",
            TestId::TransactionBlockContext => "T13",
        }
    }

    fn label(&self) -> &'static str {
        match self {
            TestId::FilesPresent => "files_present_per_exchange",
            TestId::FileNonEmpty => "file_non_empty",
            TestId::FilenameConvention => "filename_convention",
            TestId::MonotonicTimestamps => "monotonic_timestamps",
            TestId::FlushSpan => "flush_span_within_tolerance",
            TestId::UniqueOrderbookTs => "unique_orderbook_timestamps",
            TestId::UniqueTradeIds => "unique_trade_ids",
            TestId::NotCrossedOrderbook => "no_crossed_orderbook",
            TestId::PositivePricesSizes => "positive_prices_and_sizes",
            TestId::NoTemporalOverlap => "no_temporal_overlap_between_files",
            TestId::UniqueTransactionKeys => "unique_address_signature_per_leaf",
            TestId::OrderedTransactionPositions => "slot_index_order_per_address",
            TestId::TransactionBlockContext => "block_context_and_row_invariants",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-file model
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum DataKind {
    Orderbook,
    Trades,
    Transactions,
}

impl DataKind {
    const ALL: [DataKind; 3] = [
        DataKind::Orderbook,
        DataKind::Trades,
        DataKind::Transactions,
    ];

    fn dir_name(&self) -> &'static str {
        match self {
            DataKind::Orderbook => "orderbooks",
            DataKind::Trades => "trades",
            DataKind::Transactions => aetelier_io::onchain::DATATYPE_DIR,
        }
    }

    fn expected_for(exchange: &str) -> &'static [DataKind] {
        match exchange {
            "solana" => &[DataKind::Transactions],
            _ => &[DataKind::Orderbook, DataKind::Trades],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Partition {
    market_type: String,
    binding_id: String,
    day: String,
}

#[derive(Debug, PartialEq, Eq)]
struct Leaf {
    dir: PathBuf,
    exchange: String,
    kind: DataKind,
    partition: Option<Partition>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct LeafListing {
    files: Vec<PathBuf>,
    pending: Vec<String>,
    torn_index_lines: usize,
    heartbeats: usize,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct PendingFile {
    leaf: String,
    filename: String,
}

#[derive(Debug, Clone, Serialize)]
struct FileReport {
    path: String,
    exchange: String,
    symbol: String,
    kind: DataKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition: Option<Partition>,
    size_bytes: u64,
    row_count: u64,
    distinct_ts_count: u64,
    first_ts: u64,
    last_ts: u64,
    span_us: u64,
    failed: Vec<&'static str>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Cumulative state (persisted across runs)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    /// Append-only run history.
    runs: Vec<RunSummary>,
    /// Cumulative counters across all runs since `state_file` was created.
    cumulative: Cumulative,
    /// `path → size_bytes` of files we have already validated.  Used by
    /// `--skip-seen` and for de-dup of cumulative byte counts.
    known_files: HashMap<String, u64>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct RunSummary {
    run_id: u64,
    started_at_unix_ms: u64,
    ended_at_unix_ms: u64,
    files_seen: u64,
    new_files: u64,
    #[serde(default)]
    pending_files: u64,
    #[serde(default)]
    torn_index_lines: u64,
    #[serde(default)]
    heartbeats: u64,
    bytes_seen: u64,
    total_orderbook_snapshots: u64,
    total_trades: u64,
    #[serde(default)]
    total_transactions: u64,
    tests_passed: u64,
    tests_failed: u64,
    failed_test_codes: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Cumulative {
    runs: u64,
    distinct_files: u64,
    bytes: u64,
    orderbook_snapshots: u64,
    trades: u64,
    #[serde(default)]
    transactions: u64,
    per_exchange: BTreeMap<String, ExchangeStats>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ExchangeStats {
    files: u64,
    bytes: u64,
    orderbook_snapshots: u64,
    trades: u64,
    #[serde(default)]
    transactions: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Per-(scope, symbol, kind) accumulator of (first_ts, last_ts, filename).
type PerGroupRows = BTreeMap<(String, String, DataKind), Vec<(u64, u64, String)>>;

/// `(row_count, distinct_ts, first_ts, last_ts, monotonic, failed_codes)`.
type FileValidation = (u64, u64, u64, u64, bool, Vec<&'static str>);

struct RunOutput {
    exchanges: Vec<String>,
    tests: Vec<TestOutcome>,
    files: Vec<FileReport>,
    pending: Vec<PendingFile>,
    summary: RunSummary,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let started_at_unix_ms = unix_ms();

    let mut state = load_state(&cli.state_file).context("loading state file")?;
    let run = run(&cli, &mut state, started_at_unix_ms)?;

    print_report(
        &cli,
        &run.exchanges,
        &run.tests,
        &run.files,
        &run.pending,
        &state,
        &run.summary,
    );

    write_json(
        &cli.report_out,
        &PerRunReport {
            summary: &run.summary,
            tests: &run.tests,
            files: &run.files,
            pending: &run.pending,
        },
    )
    .with_context(|| format!("writing per-run report {}", cli.report_out.display()))?;

    save_state(&cli.state_file, &state)
        .with_context(|| format!("saving state {}", cli.state_file.display()))?;

    if run.summary.tests_failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

fn run(cli: &Cli, state: &mut State, started_at_unix_ms: u64) -> Result<RunOutput> {
    let run_id = state.cumulative.runs + 1;

    let exchanges = discover_exchanges(&cli.data_dir, &cli.expect_exchanges);
    let mut listings: Vec<(Leaf, LeafListing)> = Vec::new();
    for leaf in discover_leaves(&cli.data_dir, &exchanges) {
        let listing = list_leaf(&leaf)
            .with_context(|| format!("listing leaf {}", leaf.dir.display()))?;
        listings.push((leaf, listing));
    }
    let mut t01_failures: Vec<String> = Vec::new();
    for ex in &cli.expect_exchanges {
        for &kind in DataKind::expected_for(ex) {
            let count: usize = listings
                .iter()
                .filter(|(leaf, _)| &leaf.exchange == ex && leaf.kind == kind)
                .map(|(_, listing)| listing.files.len() + listing.heartbeats)
                .sum();
            if count == 0 {
                t01_failures.push(format!(
                    "{}/{}: 0 .parquet files",
                    ex,
                    kind.dir_name()
                ));
            }
        }
    }

    // ── Walk every file and build per-file reports ───────────────────────
    let mut file_reports: Vec<FileReport> = Vec::new();
    let mut pending: Vec<PendingFile> = Vec::new();
    let mut torn_index_lines = 0u64;
    let mut heartbeats = 0u64;
    let mut t11_leaf_failures: Vec<String> = Vec::new();
    let mut new_files = 0u64;
    let mut bytes_seen = 0u64;
    let mut total_ob_snapshots = 0u64;
    let mut total_trades = 0u64;
    let mut total_transactions = 0u64;

    // For T10: group end-state per (scope, symbol, kind).
    let mut per_group: PerGroupRows = BTreeMap::new();

    for (leaf, listing) in &listings {
        torn_index_lines += listing.torn_index_lines as u64;
        heartbeats += listing.heartbeats as u64;
        pending.extend(listing.pending.iter().map(|filename| PendingFile {
            leaf: leaf.dir.to_string_lossy().into_owned(),
            filename: filename.clone(),
        }));

        let (ex, kind) = (&leaf.exchange, leaf.kind);
        let scope = group_scope(leaf);
        let mut validated: HashSet<PathBuf> = HashSet::new();
        for path in &listing.files {
            let path_str = path.to_string_lossy().into_owned();
            let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);

            let already_seen = state.known_files.get(&path_str).copied() == Some(size);
            if cli.skip_seen && already_seen {
                continue;
            }

            if !already_seen {
                new_files += 1;
            }
            bytes_seen += size;
            validated.insert(path.clone());

            let mut report = match validate_file(path, ex, kind, size, cli) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(
                        file = %path.display(),
                        error = %e,
                        "validate.read_failed"
                    );
                    // A read failure is itself a test failure — file_non_empty
                    // can't be confirmed, and the file is unreadable.
                    FileReport {
                        path: path_str.clone(),
                        exchange: ex.clone(),
                        symbol: group_symbol(path),
                        kind,
                        partition: None,
                        size_bytes: size,
                        row_count: 0,
                        distinct_ts_count: 0,
                        first_ts: 0,
                        last_ts: 0,
                        span_us: 0,
                        failed: vec![TestId::FileNonEmpty.code()],
                    }
                }
            };
            report.partition = leaf.partition.clone();

            match kind {
                DataKind::Orderbook => total_ob_snapshots += report.distinct_ts_count,
                DataKind::Trades => total_trades += report.row_count,
                DataKind::Transactions => total_transactions += report.row_count,
            }

            per_group
                .entry((scope.clone(), report.symbol.clone(), kind))
                .or_default()
                .push((report.first_ts, report.last_ts, path_str.clone()));

            state.known_files.insert(path_str, size);
            file_reports.push(report);
        }

        if kind == DataKind::Transactions {
            let index_rows: HashMap<String, u64> = read_index(&leaf.dir)
                .map(|(entries, _)| {
                    entries.into_iter().map(|e| (e.filename, e.rows)).collect()
                })
                .unwrap_or_default();
            t11_leaf_failures.extend(transaction_leaf_failures(
                leaf,
                listing,
                &index_rows,
                &validated,
            ));
        }
    }

    // ── T10: temporal overlap between adjacent files in same group ──────
    let mut overlap_failures: Vec<String> = Vec::new();
    for ((scope, sym, kind), mut spans) in per_group.into_iter() {
        spans.sort_by_key(|(first, _, _)| *first);
        for w in spans.windows(2) {
            let (_, prev_last, prev_path) = &w[0];
            let (curr_first, _, curr_path) = &w[1];
            if curr_first <= prev_last {
                overlap_failures.push(format!(
                    "{}/{}/{}: {} (last={}) overlaps {} (first={})",
                    scope,
                    sym,
                    kind.dir_name(),
                    file_basename(prev_path),
                    prev_last,
                    file_basename(curr_path),
                    curr_first,
                ));
            }
        }
    }

    // ── Aggregate test results ───────────────────────────────────────────
    let test_results = aggregate_results(
        &t01_failures,
        &file_reports,
        &overlap_failures,
        &t11_leaf_failures,
    );

    let tests_passed = test_results.iter().filter(|r| r.passed).count() as u64;
    let tests_failed = test_results.iter().filter(|r| !r.passed).count() as u64;
    let failed_codes: Vec<String> = test_results
        .iter()
        .filter(|r| !r.passed)
        .map(|r| r.id.code().to_string())
        .collect();

    // ── Update cumulative counters ──────────────────────────────────────
    state.cumulative.runs += 1;
    state.cumulative.distinct_files = state.known_files.len() as u64;
    state.cumulative.bytes = state.known_files.values().sum();
    state.cumulative.orderbook_snapshots += total_ob_snapshots;
    state.cumulative.trades += total_trades;
    state.cumulative.transactions += total_transactions;

    for fr in &file_reports {
        let stats = state
            .cumulative
            .per_exchange
            .entry(fr.exchange.clone())
            .or_default();
        stats.files += 1;
        stats.bytes += fr.size_bytes;
        match fr.kind {
            DataKind::Orderbook => stats.orderbook_snapshots += fr.distinct_ts_count,
            DataKind::Trades => stats.trades += fr.row_count,
            DataKind::Transactions => stats.transactions += fr.row_count,
        }
    }

    let summary = RunSummary {
        run_id,
        started_at_unix_ms,
        ended_at_unix_ms: unix_ms(),
        files_seen: file_reports.len() as u64,
        new_files,
        pending_files: pending.len() as u64,
        torn_index_lines,
        heartbeats,
        bytes_seen,
        total_orderbook_snapshots: total_ob_snapshots,
        total_trades,
        total_transactions,
        tests_passed,
        tests_failed,
        failed_test_codes: failed_codes.clone(),
    };
    state.runs.push(summary.clone());

    Ok(RunOutput {
        exchanges,
        tests: test_results,
        files: file_reports,
        pending,
        summary,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// File-level validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_file(
    path: &Path,
    exchange: &str,
    kind: DataKind,
    size_bytes: u64,
    cli: &Cli,
) -> Result<FileReport> {
    let mut failed: Vec<&'static str> = Vec::new();

    // T03: filename convention
    let filename_ok = matches_convention(path, kind);
    if !filename_ok {
        failed.push(TestId::FilenameConvention.code());
    }
    let symbol = group_symbol(path);

    // Read & dispatch
    let (row_count, distinct_ts, first_ts, last_ts, monotonic, sub_failed) = match kind {
        DataKind::Orderbook => validate_orderbook_file(path)?,
        DataKind::Trades => validate_trades_file(path)?,
        DataKind::Transactions => validate_transactions_file(path)?,
    };
    failed.extend(sub_failed);

    // T02: file non-empty
    if size_bytes == 0 || row_count == 0 {
        failed.push(TestId::FileNonEmpty.code());
    }
    // T04: monotonic timestamps
    if !monotonic {
        failed.push(TestId::MonotonicTimestamps.code());
    }
    // T05: flush span
    let expected_span_us =
        (cli.flush_threshold.saturating_sub(1) * cli.grid_period_us) as f64;
    let span_us = last_ts.saturating_sub(first_ts);
    if expected_span_us > 0.0 {
        let lo = expected_span_us * (1.0 - cli.span_tolerance);
        let hi = expected_span_us * (1.0 + cli.span_tolerance);
        // For trade files we only assert the upper bound (trades are
        // sparse — flushes can land on files with few trades that span
        // less than the full window).
        let in_range = match kind {
            DataKind::Orderbook => (span_us as f64) >= lo && (span_us as f64) <= hi,
            DataKind::Trades | DataKind::Transactions => (span_us as f64) <= hi,
        };
        if !in_range {
            failed.push(TestId::FlushSpan.code());
        }
    }

    Ok(FileReport {
        path: path.to_string_lossy().into_owned(),
        exchange: exchange.to_string(),
        symbol,
        kind,
        partition: None,
        size_bytes,
        row_count,
        distinct_ts_count: distinct_ts,
        first_ts,
        last_ts,
        span_us,
        failed,
    })
}

/// Read an orderbook parquet file and check the orderbook-specific
/// invariants (T06 unique timestamps, T08 not-crossed, T09 positive
/// prices/sizes).
///
/// Returns `(row_count, distinct_ts, first_ts, last_ts, monotonic, failed_codes)`.
fn validate_orderbook_file(path: &Path) -> Result<FileValidation> {
    let data = read_ob_parquet(path, OrderbookTarget::Snapshot)
        .map_err(|e| anyhow::anyhow!("read_ob_parquet: {e}"))?;
    let books: Vec<Orderbook> = match data {
        OrderbookTargetData::Snapshot(v) => v,
        OrderbookTargetData::Delta(_) => {
            anyhow::bail!("expected snapshot variant from read_ob_parquet")
        }
    };

    let row_count = books.len() as u64;
    if books.is_empty() {
        return Ok((0, 0, 0, 0, true, vec![]));
    }

    let first_ts = books.first().map(|o| o.orderbook_ts_us).unwrap_or(0);
    let last_ts = books.last().map(|o| o.orderbook_ts_us).unwrap_or(0);

    // T04: monotonic timestamps (allow equal — T06 handles strict uniqueness)
    let monotonic = books
        .windows(2)
        .all(|w| w[0].orderbook_ts_us <= w[1].orderbook_ts_us);

    // T06: unique timestamps
    let mut seen_ts: HashSet<u64> = HashSet::with_capacity(books.len());
    let mut dupe_ts = false;
    for ob in &books {
        if !seen_ts.insert(ob.orderbook_ts_us) {
            dupe_ts = true;
            break;
        }
    }

    // T08 + T09 in a single pass.
    let mut crossed = false;
    let mut non_positive = false;
    for ob in &books {
        let best_bid = ob.bids.last_key_value().map(|(p, _)| *p);
        let best_ask = ob.asks.first_key_value().map(|(p, _)| *p);
        if let (Some(b), Some(a)) = (best_bid, best_ask)
            && b >= a
        {
            crossed = true;
        }
        for (price, lvl) in ob.bids.iter().chain(ob.asks.iter()) {
            if *price <= rust_decimal::Decimal::ZERO
                || lvl.volume <= rust_decimal::Decimal::ZERO
            {
                non_positive = true;
                break;
            }
        }
        if crossed && non_positive {
            break;
        }
    }

    let mut failed: Vec<&'static str> = Vec::new();
    if dupe_ts {
        failed.push(TestId::UniqueOrderbookTs.code());
    }
    if crossed {
        failed.push(TestId::NotCrossedOrderbook.code());
    }
    if non_positive {
        failed.push(TestId::PositivePricesSizes.code());
    }

    Ok((
        row_count,
        seen_ts.len() as u64,
        first_ts,
        last_ts,
        monotonic,
        failed,
    ))
}

/// Read a trades parquet file and check trade-specific invariants
/// (T07 unique IDs, T09 positive prices/sizes).
fn validate_trades_file(path: &Path) -> Result<FileValidation> {
    let trades: Vec<Trade> = read_trades_parquet(path)
        .map_err(|e| anyhow::anyhow!("read_trades_parquet: {e}"))?;

    let row_count = trades.len() as u64;
    if trades.is_empty() {
        return Ok((0, 0, 0, 0, true, vec![]));
    }

    let first_ts = trades.first().map(|t| t.source_trade_ts_us).unwrap_or(0);
    let last_ts = trades.last().map(|t| t.source_trade_ts_us).unwrap_or(0);
    let monotonic = trades
        .windows(2)
        .all(|w| w[0].source_trade_ts_us <= w[1].source_trade_ts_us);

    // T07: unique IDs
    let mut seen_ids: HashSet<&str> = HashSet::with_capacity(trades.len());
    let mut dupe_id = false;
    let mut distinct_ts: HashSet<u64> = HashSet::new();
    let mut non_positive = false;
    for t in &trades {
        if !seen_ids.insert(t.id.as_str()) {
            dupe_id = true;
        }
        distinct_ts.insert(t.source_trade_ts_us);
        if !(t.price > rust_decimal::Decimal::ZERO
            && t.amount > rust_decimal::Decimal::ZERO)
        {
            non_positive = true;
        }
    }

    let mut failed: Vec<&'static str> = Vec::new();
    if dupe_id {
        failed.push(TestId::UniqueTradeIds.code());
    }
    if non_positive {
        failed.push(TestId::PositivePricesSizes.code());
    }

    Ok((
        row_count,
        distinct_ts.len() as u64,
        first_ts,
        last_ts,
        monotonic,
        failed,
    ))
}

fn validate_transactions_file(path: &Path) -> Result<FileValidation> {
    let rows = read_transactions_parquet(path)
        .map_err(|e| anyhow::anyhow!("read_transactions_parquet: {e}"))?;
    if rows.is_empty() {
        return Ok((0, 0, 0, 0, true, vec![]));
    }
    let first_ts = rows.first().map(|t| t.effective_ts_us()).unwrap_or(0);
    let last_ts = rows.last().map(|t| t.effective_ts_us()).unwrap_or(0);
    let monotonic = rows
        .windows(2)
        .all(|w| w[0].effective_ts_us() <= w[1].effective_ts_us());
    let distinct_ts: HashSet<u64> = rows.iter().map(|t| t.effective_ts_us()).collect();

    let checks = check_transactions(&rows);
    let mut failed: Vec<&'static str> = Vec::new();
    for (count, detail, id) in [
        (
            checks.duplicate_keys,
            &checks.first_duplicate,
            TestId::UniqueTransactionKeys,
        ),
        (
            checks.out_of_order,
            &checks.first_out_of_order,
            TestId::OrderedTransactionPositions,
        ),
        (
            checks.invariant_violations,
            &checks.first_invariant_violation,
            TestId::TransactionBlockContext,
        ),
    ] {
        if count > 0 {
            tracing::warn!(
                file = %path.display(),
                test = id.code(),
                count,
                first = detail.as_deref().unwrap_or_default(),
                "validate.transactions_check_failed"
            );
            failed.push(id.code());
        }
    }

    Ok((
        checks.rows,
        distinct_ts.len() as u64,
        first_ts,
        last_ts,
        monotonic,
        failed,
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregate test results across files
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct TestOutcome {
    id: TestId,
    code: &'static str,
    label: &'static str,
    passed: bool,
    /// Detail (e.g. file paths) when the test failed.
    detail: Vec<String>,
}

fn aggregate_results(
    t01_failures: &[String],
    files: &[FileReport],
    t10_failures: &[String],
    t11_leaf_failures: &[String],
) -> Vec<TestOutcome> {
    let collect_for = |id: TestId| -> Vec<String> {
        let code = id.code();
        files
            .iter()
            .filter(|f| f.failed.contains(&code))
            .map(|f| format!("{} (sym={}, ex={})", f.path, f.symbol, f.exchange))
            .collect()
    };

    let mut out: Vec<TestOutcome> = Vec::new();

    // T01 — directory-level
    out.push(TestOutcome {
        id: TestId::FilesPresent,
        code: TestId::FilesPresent.code(),
        label: TestId::FilesPresent.label(),
        passed: t01_failures.is_empty(),
        detail: t01_failures.to_vec(),
    });

    for id in [
        TestId::FileNonEmpty,
        TestId::FilenameConvention,
        TestId::MonotonicTimestamps,
        TestId::FlushSpan,
        TestId::UniqueOrderbookTs,
        TestId::UniqueTradeIds,
        TestId::NotCrossedOrderbook,
        TestId::PositivePricesSizes,
        TestId::UniqueTransactionKeys,
        TestId::OrderedTransactionPositions,
        TestId::TransactionBlockContext,
    ] {
        let mut detail = collect_for(id);
        if id == TestId::UniqueTransactionKeys {
            detail.extend_from_slice(t11_leaf_failures);
        }
        out.push(TestOutcome {
            id,
            code: id.code(),
            label: id.label(),
            passed: detail.is_empty(),
            detail,
        });
    }

    out.push(TestOutcome {
        id: TestId::NoTemporalOverlap,
        code: TestId::NoTemporalOverlap.code(),
        label: TestId::NoTemporalOverlap.label(),
        passed: t10_failures.is_empty(),
        detail: t10_failures.to_vec(),
    });

    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Reporting
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct PerRunReport<'a> {
    summary: &'a RunSummary,
    tests: &'a [TestOutcome],
    files: &'a [FileReport],
    #[serde(skip_serializing_if = "<[PendingFile]>::is_empty")]
    pending: &'a [PendingFile],
}

fn print_report(
    cli: &Cli,
    exchanges: &[String],
    tests: &[TestOutcome],
    files: &[FileReport],
    pending: &[PendingFile],
    state: &State,
    summary: &RunSummary,
) {
    println!("┌─────────────────────────────────────────────────────────────────────");
    println!("│ aetelier-sdk validate — run #{}", summary.run_id);
    println!("├─────────────────────────────────────────────────────────────────────");
    println!("│ data_dir          : {}", cli.data_dir.display());
    println!("│ expect_exchanges  : {}", cli.expect_exchanges.join(", "));
    println!("│ found_exchanges   : {}", exchanges.join(", "));
    println!(
        "│ flush_threshold   : {} (× {} µs grid → ~{:.1} s window)",
        cli.flush_threshold,
        cli.grid_period_us,
        (cli.flush_threshold * cli.grid_period_us) as f64 / 1_000_000.0
    );
    println!("├──────────────────────── this run ──────────────────────────────────");
    println!("│ files_seen        : {}", summary.files_seen);
    println!("│ new_files         : {}", summary.new_files);
    println!("│ pending_files     : {}", summary.pending_files);
    for p in pending.iter().take(5) {
        println!(
            "│         · {}",
            Path::new(&p.leaf).join(&p.filename).display()
        );
    }
    if pending.len() > 5 {
        println!("│         · … and {} more", pending.len() - 5);
    }
    println!("│ torn_index_lines  : {}", summary.torn_index_lines);
    println!("│ heartbeats        : {}", summary.heartbeats);
    println!("│ bytes_seen        : {}", human_bytes(summary.bytes_seen));
    println!(
        "│ ob_snapshots      : {}",
        summary.total_orderbook_snapshots
    );
    println!("│ trades            : {}", summary.total_trades);
    println!("│ transactions      : {}", summary.total_transactions);
    println!(
        "│ tests_passed      : {}/{}",
        summary.tests_passed,
        tests.len()
    );
    println!("│ tests_failed      : {}", summary.tests_failed);
    println!("├──────────────────────── tests ─────────────────────────────────────");
    for t in tests {
        let mark = if t.passed { "PASS" } else { "FAIL" };
        println!("│  [{}] {} {}", mark, t.code, t.label);
        if !t.passed {
            for d in t.detail.iter().take(5) {
                println!("│         · {}", d);
            }
            if t.detail.len() > 5 {
                println!("│         · … and {} more", t.detail.len() - 5);
            }
        }
    }

    if cli.verbose {
        println!("├──────────────────────── per-file ──────────────────────────────────");
        println!(
            "│  {:<8} {:<10} {:<8} {:>10} {:>12} {:>14} failed",
            "exchange", "symbol", "kind", "rows", "span_us", "size"
        );
        for f in files {
            println!(
                "│  {:<8} {:<10} {:<8} {:>10} {:>12} {:>14} {}",
                f.exchange,
                f.symbol,
                f.kind.dir_name(),
                f.row_count,
                f.span_us,
                human_bytes(f.size_bytes),
                if f.failed.is_empty() {
                    "·".to_string()
                } else {
                    f.failed.join(",")
                }
            );
        }
    }

    println!("├──────────────────────── cumulative ────────────────────────────────");
    println!("│ total_runs        : {}", state.cumulative.runs);
    println!("│ distinct_files    : {}", state.cumulative.distinct_files);
    println!(
        "│ total_bytes       : {}",
        human_bytes(state.cumulative.bytes)
    );
    println!(
        "│ ob_snapshots      : {}",
        state.cumulative.orderbook_snapshots
    );
    println!("│ trades            : {}", state.cumulative.trades);
    println!("│ transactions      : {}", state.cumulative.transactions);
    for (ex, s) in &state.cumulative.per_exchange {
        println!(
            "│   {:<8} files={:<5} bytes={:<10} ob={:<7} trades={:<9} txs={}",
            ex,
            s.files,
            human_bytes(s.bytes),
            s.orderbook_snapshots,
            s.trades,
            s.transactions,
        );
    }
    println!("└─────────────────────────────────────────────────────────────────────");
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let json = serde_json::to_string_pretty(value)?;
    fs::write(path, json)?;
    Ok(())
}

fn load_state(path: &Path) -> Result<State> {
    if !path.exists() {
        return Ok(State::default());
    }
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(State::default());
    }
    let state: State = serde_json::from_slice(&bytes)?;
    Ok(state)
}

fn save_state(path: &Path, state: &State) -> Result<()> {
    write_json(path, state)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn discover_exchanges(root: &Path, expected: &[String]) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    if let Ok(rd) = fs::read_dir(root) {
        for e in rd.flatten() {
            if e.file_type().map(|t| t.is_dir()).unwrap_or(false)
                && let Some(name) = e.file_name().to_str()
                && expected.iter().any(|x| x == name)
            {
                found.push(name.to_string());
            }
        }
    }
    found.sort();
    found
}

fn parquet_files_in(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

fn subdirs(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            if e.path().is_dir()
                && let Ok(name) = e.file_name().into_string()
            {
                names.push(name);
            }
        }
    }
    names.sort();
    names
}

fn is_day(name: &str) -> bool {
    name.len() == 10 && chrono::NaiveDate::parse_from_str(name, "%Y-%m-%d").is_ok()
}

fn discover_leaves(root: &Path, exchanges: &[String]) -> Vec<Leaf> {
    let mut leaves: Vec<Leaf> = Vec::new();
    for exchange in exchanges {
        let exchange_dir = root.join(exchange);
        for kind in DataKind::ALL {
            let dir = exchange_dir.join(kind.dir_name());
            if dir.is_dir() {
                leaves.push(Leaf {
                    dir,
                    exchange: exchange.clone(),
                    kind,
                    partition: None,
                });
            }
        }
        for market_type in subdirs(&exchange_dir) {
            if DataKind::ALL
                .iter()
                .any(|kind| kind.dir_name() == market_type)
            {
                continue;
            }
            for kind in DataKind::ALL {
                let kind_dir = exchange_dir.join(&market_type).join(kind.dir_name());
                for binding_id in subdirs(&kind_dir) {
                    let binding_dir = kind_dir.join(&binding_id);
                    for day in subdirs(&binding_dir).into_iter().filter(|d| is_day(d)) {
                        leaves.push(Leaf {
                            dir: binding_dir.join(&day),
                            exchange: exchange.clone(),
                            kind,
                            partition: Some(Partition {
                                market_type: market_type.clone(),
                                binding_id: binding_id.clone(),
                                day,
                            }),
                        });
                    }
                }
            }
        }
    }
    leaves
}

fn list_leaf(leaf: &Leaf) -> Result<LeafListing> {
    if !leaf.dir.join(INDEX_FILE).exists() {
        return Ok(LeafListing {
            files: parquet_files_in(&leaf.dir),
            ..LeafListing::default()
        });
    }
    let index = read_leaf_index(&leaf.dir)?;
    let mut names: Vec<String> = index.entries.into_iter().map(|e| e.filename).collect();
    names.sort();
    names.dedup();
    Ok(LeafListing {
        files: names.iter().map(|name| leaf.dir.join(name)).collect(),
        pending: pending_files(&leaf.dir)?,
        torn_index_lines: index.torn,
        heartbeats: index.heartbeats.len(),
    })
}

fn group_scope(leaf: &Leaf) -> String {
    match &leaf.partition {
        Some(partition) => format!(
            "{}/{}/{}",
            leaf.exchange, partition.market_type, partition.binding_id
        ),
        None => leaf.exchange.clone(),
    }
}

fn transaction_leaf_failures(
    leaf: &Leaf,
    listing: &LeafListing,
    index_rows: &HashMap<String, u64>,
    new_files: &HashSet<PathBuf>,
) -> Vec<String> {
    let mut groups: BTreeMap<(String, String), Vec<&PathBuf>> = BTreeMap::new();
    for path in &listing.files {
        let Some(name) = file_name_of(path) else {
            continue;
        };
        let day = match &leaf.partition {
            Some(partition) => partition.day.clone(),
            None => name.stamp.date_naive().to_string(),
        };
        groups
            .entry((day, name.symbol.to_string()))
            .or_default()
            .push(path);
    }

    let mut failures: Vec<String> = Vec::new();
    for ((day, address), mut files) in groups {
        if !files.iter().any(|path| new_files.contains(*path)) {
            continue;
        }
        files.sort_by_key(|path| new_files.contains(*path));
        let rows = files
            .iter()
            .filter_map(|path| index_rows.get(path.file_name()?.to_str()?))
            .fold(0u64, |total, rows| total.saturating_add(*rows));
        let mut keys =
            LeafKeys::with_capacity(usize::try_from(rows).unwrap_or(usize::MAX));
        let expected = ChainAddress::solana(&address).ok();
        for path in files {
            let file_keys = match read_transaction_keys(path) {
                Ok(file_keys) => file_keys,
                Err(e) => {
                    tracing::warn!(
                        file = %path.display(),
                        error = %e,
                        "validate.transaction_keys_unreadable"
                    );
                    continue;
                }
            };
            let duplicates = keys.admit(&file_keys);
            if !new_files.contains(path) {
                continue;
            }
            let mut foreign = file_keys.iter().filter(|(a, _)| Some(*a) != expected);
            if let Some((other, _)) = foreign.next() {
                failures.push(format!(
                    "{} (sym={address}): {} rows under another address, first {other}",
                    path.display(),
                    1 + foreign.count(),
                ));
            }
            if let Some(first) = duplicates.first {
                failures.push(format!(
                    "{} (sym={address}, day={day}): {} repeated across files, first {first}",
                    path.display(),
                    duplicates.count,
                ));
            }
        }
    }
    failures
}

fn file_name_of(path: &Path) -> Option<FileName<'_>> {
    parse_file_name(path.file_name()?.to_str()?)
}

fn matches_convention(path: &Path, kind: DataKind) -> bool {
    let expected_kind = match kind {
        DataKind::Orderbook => FileKind::Orderbook,
        DataKind::Trades => FileKind::Trades,
        DataKind::Transactions => FileKind::Transactions,
    };
    file_name_of(path)
        .is_some_and(|name| name.mode == "sync" && name.kind == expected_kind)
}

fn parse_symbol_from_filename(path: &Path) -> Option<String> {
    file_name_of(path).map(|name| name.symbol.to_string())
}

fn group_symbol(path: &Path) -> String {
    parse_symbol_from_filename(path).unwrap_or_else(|| {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default()
            .to_string()
    })
}

fn file_basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| path.to_string())
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{:.2} {}", v, UNITS[i])
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_io::orderbooks::write_ob_parquet;
    use aetelier_types::levels::Level;
    use aetelier_types::orders::OrderSide;
    use aetelier_types::trading_pair::TradingPair;
    use rust_decimal::Decimal;
    use std::path::PathBuf;

    #[test]
    fn convention_orderbook() {
        let p = PathBuf::from(
            "/data/binance/orderbooks/binance_NVDAB-USDT_ob_sync_20261006_211107.680123.parquet",
        );
        assert!(matches_convention(&p, DataKind::Orderbook));
        assert!(!matches_convention(&p, DataKind::Trades));
        assert_eq!(
            parse_symbol_from_filename(&p).as_deref(),
            Some("NVDAB-USDT")
        );
    }

    #[test]
    fn convention_trades_with_dashes() {
        let p = PathBuf::from(
            "/data/coinbase/trades/coinbase_BTC-USDC_trades_sync_20261006_212927.433456.parquet",
        );
        assert!(matches_convention(&p, DataKind::Trades));
        assert!(!matches_convention(&p, DataKind::Orderbook));
        assert_eq!(parse_symbol_from_filename(&p).as_deref(), Some("BTC-USDC"));
    }

    #[test]
    fn convention_symbol_with_underscore() {
        let p = PathBuf::from(
            "/data/hyperliquid/orderbooks/hyperliquid_xyz_TSLA-USDC_ob_sync_20261006_211107.680123.parquet",
        );
        assert!(matches_convention(&p, DataKind::Orderbook));
        assert_eq!(
            parse_symbol_from_filename(&p).as_deref(),
            Some("xyz_TSLA-USDC")
        );
    }

    #[test]
    fn convention_accepts_v0_1_0_millisecond_names() {
        let p = PathBuf::from(
            "/data/binance/orderbooks/binance_BTC-USDT_ob_sync_20261006_211107.680.parquet",
        );
        assert!(matches_convention(&p, DataKind::Orderbook));
    }

    #[test]
    fn unparseable_files_group_by_their_own_stem() {
        let p = PathBuf::from(
            "/data/binance/trades/binance_BTC-USDT_trades_rehydrated_20261006_211107.parquet",
        );
        assert!(!matches_convention(&p, DataKind::Trades));
        assert_eq!(
            group_symbol(&p),
            "binance_BTC-USDT_trades_rehydrated_20261006_211107"
        );
    }

    #[test]
    fn convention_rejects_missing_sync_marker() {
        let p = PathBuf::from(
            "/data/binance/orderbooks/binance_BTC-USDT_ob_20261006_211107.680123.parquet",
        );
        assert!(!matches_convention(&p, DataKind::Orderbook));
    }

    #[test]
    fn convention_rejects_raw_mode() {
        let p = PathBuf::from(
            "/data/binance/orderbooks/binance_BTC-USDT_ob_raw_20261006_211107.680123.parquet",
        );
        assert!(!matches_convention(&p, DataKind::Orderbook));
    }

    #[test]
    fn convention_accepts_collision_siblings() {
        let p = PathBuf::from(
            "/data/binance/orderbooks/binance_BTC-USDT_ob_sync_20261006_211107.680123-1.parquet",
        );
        assert!(matches_convention(&p, DataKind::Orderbook));
        assert_eq!(parse_symbol_from_filename(&p).as_deref(), Some("BTC-USDT"));
    }

    fn grid_orderbooks(count: u64, start_us: u64, grid_period_us: u64) -> Vec<Orderbook> {
        let pair = TradingPair::new("NVDAB", "USDT");
        (0..count)
            .map(|i| {
                Orderbook::from_levels(
                    i as u32,
                    start_us + i * grid_period_us,
                    pair.clone(),
                    "binance".to_string(),
                    vec![Level::new(
                        0,
                        OrderSide::Bids,
                        Decimal::new(18000, 2),
                        Decimal::ONE,
                        vec![],
                    )],
                    vec![Level::new(
                        0,
                        OrderSide::Asks,
                        Decimal::new(18010, 2),
                        Decimal::ONE,
                        vec![],
                    )],
                )
            })
            .collect()
    }

    #[test]
    fn writer_output_passes_every_file_check() {
        let dir = tempfile::tempdir().unwrap();
        let books = grid_orderbooks(300, 1_791_321_067_600_000, 100_000);
        let path = write_ob_parquet(&books, dir.path(), "sync").unwrap();
        let size_bytes = fs::metadata(&path).unwrap().len();
        let cli = Cli::parse_from(["validate", "--flush-threshold", "300"]);

        let report =
            validate_file(&path, "binance", DataKind::Orderbook, size_bytes, &cli)
                .unwrap();

        assert_eq!(report.failed, Vec::<&str>::new());
        assert_eq!(report.symbol, "NVDAB-USDT");
    }

    #[test]
    fn span_outside_tolerance_fails_flush_span() {
        let dir = tempfile::tempdir().unwrap();
        let books = grid_orderbooks(300, 1_791_321_067_600_000, 200_000);
        let path = write_ob_parquet(&books, dir.path(), "sync").unwrap();
        let size_bytes = fs::metadata(&path).unwrap().len();
        let cli = Cli::parse_from(["validate", "--flush-threshold", "300"]);

        let report =
            validate_file(&path, "binance", DataKind::Orderbook, size_bytes, &cli)
                .unwrap();

        assert_eq!(report.failed, vec![TestId::FlushSpan.code()]);
    }

    fn transactions(
        address: &str,
        positions: &[(u64, u64, &str)],
    ) -> Vec<aetelier_types::onchain::Transaction> {
        use aetelier_types::onchain::{
            ChainAddress, Commitment, MatchRole, Transaction, TransactionVersion,
        };
        positions
            .iter()
            .map(|&(slot, tx_index, signature)| Transaction {
                exchange: "solana".to_string(),
                address: ChainAddress::solana(address).unwrap(),
                match_role: MatchRole::AccountKey,
                source_tx_ts_us: 1_791_321_067_000_000 + slot - 380_000_000,
                local_tx_ts_us: 0,
                source_tx_rtt_us: 0,
                recv_seq: 0,
                conn_epoch_us: 0,
                commitment: Commitment::Finalized,
                slot,
                block_height: slot - 22_000_000,
                blockhash: "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn".to_string(),
                tx_index,
                signature: signature.to_string(),
                version: TransactionVersion::V0,
                success: true,
                fee_lamports: 5_000,
                compute_units_consumed: None,
                compute_unit_limit: None,
                compute_unit_price_micro_lamports: None,
                priority_fee_lamports: None,
                cost_units: None,
                signers: Vec::new(),
                account_keys: Vec::new(),
                log_messages: None,
                failure: None,
            })
            .collect()
    }

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

    fn validate_transactions(
        rows: &[aetelier_types::onchain::Transaction],
    ) -> FileReport {
        let dir = tempfile::tempdir().unwrap();
        let path = aetelier_io::onchain::write_transactions_parquet_timestamped(
            rows,
            dir.path(),
            "sync",
        )
        .unwrap();
        let size_bytes = fs::metadata(&path).unwrap().len();
        let cli = Cli::parse_from([
            "validate",
            "--flush-threshold",
            "60",
            "--grid-period-us",
            "1000000",
        ]);
        validate_file(&path, "solana", DataKind::Transactions, size_bytes, &cli).unwrap()
    }

    #[test]
    fn transactions_writer_output_passes_every_file_check() {
        let rows = transactions(
            JUPITER_V6,
            &[
                (380_000_000, 1, "a"),
                (380_000_000, 4, "b"),
                (380_000_003, 0, "c"),
            ],
        );
        let report = validate_transactions(&rows);
        assert_eq!(report.failed, Vec::<&str>::new());
        assert_eq!((report.symbol.as_str(), report.row_count), (JUPITER_V6, 3));
    }

    #[test]
    fn duplicate_and_reordered_transactions_fail_t11_and_t12() {
        let rows =
            transactions(JUPITER_V6, &[(380_000_001, 2, "a"), (380_000_001, 2, "a")]);
        let report = validate_transactions(&rows);
        assert!(
            report
                .failed
                .contains(&TestId::UniqueTransactionKeys.code())
        );
        assert!(
            report
                .failed
                .contains(&TestId::OrderedTransactionPositions.code())
        );
        assert!(
            !report
                .failed
                .contains(&TestId::TransactionBlockContext.code())
        );
    }

    #[test]
    fn missing_block_context_fails_t13() {
        let mut rows = transactions(JUPITER_V6, &[(380_000_000, 1, "a")]);
        rows[0].blockhash.clear();
        let report = validate_transactions(&rows);
        assert_eq!(report.failed, vec![TestId::TransactionBlockContext.code()]);
    }

    #[test]
    fn convention_transactions_keep_the_case_sensitive_address() {
        let p = PathBuf::from(format!(
            "/data/solana/transactions/solana_{JUPITER_V6}_txs_sync_20261109_141107.000000.parquet"
        ));
        assert!(matches_convention(&p, DataKind::Transactions));
        assert!(!matches_convention(&p, DataKind::Trades));
        assert_eq!(parse_symbol_from_filename(&p).as_deref(), Some(JUPITER_V6));
    }

    #[test]
    fn files_present_expectations_follow_the_venue() {
        assert_eq!(DataKind::expected_for("solana"), &[DataKind::Transactions]);
        assert_eq!(
            DataKind::expected_for("binance"),
            &[DataKind::Orderbook, DataKind::Trades]
        );
    }

    const WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
    const BOOKS_START_US: u64 = 1_791_321_067_600_000;
    const NEXT_BOOKS_START_US: u64 = BOOKS_START_US + 30_000_000;
    const BOOK_LEAF: &str = "binance/spot/orderbooks/bind-a/2026-10-06";
    const TX_LEAF: &str = "solana/onchain/transactions/bind-a/2026-10-06";

    fn leaf_dir(data: &Path, relative: &str) -> PathBuf {
        let dir = data.join(relative);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn book_file(leaf: &Path, start_us: u64) -> PathBuf {
        write_ob_parquet(&grid_orderbooks(300, start_us, 100_000), leaf, "sync").unwrap()
    }

    fn tx_file(leaf: &Path, rows: &[aetelier_types::onchain::Transaction]) -> PathBuf {
        aetelier_io::onchain::write_transactions_parquet_timestamped(rows, leaf, "sync")
            .unwrap()
    }

    fn index(path: &Path, rows: u64) {
        aetelier_io::leaf_index::append_entry(
            path.parent().unwrap(),
            &aetelier_io::leaf_index::IndexEntry {
                filename: path.file_name().unwrap().to_string_lossy().into_owned(),
                sha256: String::new(),
                rows,
                t_min_us: 0,
                t_max_us: 0,
                schema_id: String::new(),
            },
        )
        .unwrap();
    }

    fn validate_dir(data: &Path, args: &[&str], state: &mut State) -> RunOutput {
        let mut argv = vec!["validate", "--data-dir", data.to_str().unwrap()];
        argv.extend_from_slice(args);
        run(&Cli::parse_from(argv), state, 0).unwrap()
    }

    fn validate_books(data: &Path, state: &mut State) -> RunOutput {
        let args = ["--flush-threshold", "300", "--expect-exchanges", "binance"];
        validate_dir(data, &args, state)
    }

    fn validate_txs(data: &Path, args: &[&str], state: &mut State) -> RunOutput {
        let mut argv = vec!["--expect-exchanges", "solana"];
        argv.extend_from_slice(args);
        validate_dir(data, &argv, state)
    }

    fn outcome(run: &RunOutput, id: TestId) -> &TestOutcome {
        run.tests.iter().find(|t| t.id == id).unwrap()
    }

    fn report_json(run: &RunOutput) -> serde_json::Value {
        serde_json::to_value(PerRunReport {
            summary: &run.summary,
            tests: &run.tests,
            files: &run.files,
            pending: &run.pending,
        })
        .unwrap()
    }

    #[test]
    fn flat_leaf_without_an_index_reports_exactly_as_before() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), "binance/orderbooks");
        let first = book_file(&leaf, BOOKS_START_US);
        let second = book_file(&leaf, NEXT_BOOKS_START_US);

        let leaves = discover_leaves(data.path(), &["binance".to_string()]);
        assert_eq!(
            leaves,
            vec![Leaf {
                dir: leaf.clone(),
                exchange: "binance".to_string(),
                kind: DataKind::Orderbook,
                partition: None,
            }]
        );
        assert_eq!(
            list_leaf(&leaves[0]).unwrap(),
            LeafListing {
                files: vec![first.clone(), second],
                ..LeafListing::default()
            }
        );

        let run = validate_books(data.path(), &mut State::default());
        assert_eq!(
            serde_json::to_string(&run.files[0]).unwrap(),
            format!(
                "{{\"path\":{},\"exchange\":\"binance\",\"symbol\":\"NVDAB-USDT\",\
                 \"kind\":\"orderbook\",\"size_bytes\":{},\"row_count\":300,\
                 \"distinct_ts_count\":300,\"first_ts\":1791321067600000,\
                 \"last_ts\":1791321097500000,\"span_us\":29900000,\"failed\":[]}}",
                serde_json::to_string(&first.to_string_lossy()).unwrap(),
                fs::metadata(&first).unwrap().len(),
            )
        );
        assert_eq!(report_json(&run).get("pending"), None);
        assert_eq!((run.summary.files_seen, run.summary.pending_files), (2, 0));
        assert_eq!(
            outcome(&run, TestId::FilesPresent).detail,
            vec!["binance/trades: 0 .parquet files"]
        );
        assert!(
            run.tests
                .iter()
                .all(|t| t.passed || t.id == TestId::FilesPresent)
        );
    }

    #[test]
    fn platform_leaf_files_come_from_the_index() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), BOOK_LEAF);
        leaf_dir(data.path(), "binance/spot/orderbooks/bind-a/latest");
        let first = book_file(&leaf, BOOKS_START_US);
        let second = book_file(&leaf, NEXT_BOOKS_START_US);
        index(&second, 300);
        index(&first, 300);
        index(&second, 300);
        let index_path = leaf.join(INDEX_FILE);
        let mut lines = fs::read_to_string(&index_path).unwrap();
        lines.push_str("{\"filename\":\n");
        fs::write(&index_path, lines).unwrap();

        let leaves = discover_leaves(data.path(), &["binance".to_string()]);
        let partition = Partition {
            market_type: "spot".to_string(),
            binding_id: "bind-a".to_string(),
            day: "2026-10-06".to_string(),
        };
        assert_eq!(
            leaves,
            vec![Leaf {
                dir: leaf.clone(),
                exchange: "binance".to_string(),
                kind: DataKind::Orderbook,
                partition: Some(partition.clone()),
            }]
        );
        assert_eq!(group_scope(&leaves[0]), "binance/spot/bind-a");
        assert_eq!(
            list_leaf(&leaves[0]).unwrap(),
            LeafListing {
                files: vec![first, second],
                pending: Vec::new(),
                torn_index_lines: 1,
                heartbeats: 0,
            }
        );

        let run = validate_books(data.path(), &mut State::default());
        assert_eq!(run.files.len(), 2);
        assert!(
            run.files
                .iter()
                .all(|f| f.partition == Some(partition.clone()))
        );
        assert_eq!(
            serde_json::to_value(&run.files[0]).unwrap()["partition"],
            serde_json::json!({
                "market_type": "spot",
                "binding_id": "bind-a",
                "day": "2026-10-06",
            })
        );
        assert_eq!(run.summary.torn_index_lines, 1);
    }

    #[test]
    fn pending_files_are_reported_not_validated() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), BOOK_LEAF);
        let unlisted = book_file(&leaf, BOOKS_START_US);
        let retried = book_file(&leaf, BOOKS_START_US);
        index(&retried, 300);
        let unlisted_name = unlisted.file_name().unwrap().to_string_lossy().into_owned();

        let mut state = State::default();
        let run = validate_books(data.path(), &mut state);

        assert_eq!(run.files.len(), 1);
        assert_eq!(run.files[0].path, retried.to_string_lossy());
        assert_eq!(
            run.pending,
            vec![PendingFile {
                leaf: leaf.to_string_lossy().into_owned(),
                filename: unlisted_name.clone(),
            }]
        );
        assert_eq!(run.summary.pending_files, 1);
        assert_eq!(report_json(&run)["pending"][0]["filename"], unlisted_name);
        assert!(outcome(&run, TestId::NoTemporalOverlap).passed);
        assert!(!state.known_files.contains_key(&*unlisted.to_string_lossy()));
    }

    #[test]
    fn listed_file_missing_on_disk_fails_t02() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), BOOK_LEAF);
        let present = book_file(&leaf, BOOKS_START_US);
        let missing =
            leaf.join("binance_NVDAB-USDT_ob_sync_20261006_211207.600000.parquet");
        index(&present, 300);
        index(&missing, 300);

        let run = validate_books(data.path(), &mut State::default());

        assert_eq!(run.files.len(), 2);
        assert_eq!(
            outcome(&run, TestId::FileNonEmpty).detail,
            vec![format!(
                "{} (sym=NVDAB-USDT, ex=binance)",
                missing.display()
            )]
        );
    }

    #[test]
    fn one_symbol_in_two_bindings_is_not_an_overlap() {
        let data = tempfile::tempdir().unwrap();
        let bind_a = leaf_dir(data.path(), BOOK_LEAF);
        let bind_b = leaf_dir(data.path(), "binance/spot/orderbooks/bind-b/2026-10-06");
        index(&book_file(&bind_a, BOOKS_START_US), 300);
        index(&book_file(&bind_b, BOOKS_START_US), 300);

        let run = validate_books(data.path(), &mut State::default());
        assert_eq!(run.files.len(), 2);
        assert!(outcome(&run, TestId::NoTemporalOverlap).passed);

        index(&book_file(&bind_a, BOOKS_START_US), 300);
        let run = validate_books(data.path(), &mut State::default());
        let overlaps = &outcome(&run, TestId::NoTemporalOverlap).detail;
        assert_eq!(overlaps.len(), 1);
        assert!(overlaps[0].starts_with("binance/spot/bind-a/NVDAB-USDT/orderbooks: "));
    }

    #[test]
    fn t01_counts_files_in_partitioned_leaves() {
        let data = tempfile::tempdir().unwrap();
        let books = leaf_dir(data.path(), BOOK_LEAF);
        index(&book_file(&books, BOOKS_START_US), 300);
        let txs = leaf_dir(data.path(), TX_LEAF);
        index(
            &tx_file(&txs, &transactions(JUPITER_V6, &[(380_000_000, 1, "a")])),
            1,
        );
        let pending_only = leaf_dir(data.path(), "binance/spot/trades/bind-a/2026-10-06");
        fs::write(pending_only.join(INDEX_FILE), "").unwrap();
        fs::write(
            pending_only
                .join("binance_NVDAB-USDT_trades_sync_20261006_211107.600000.parquet"),
            "",
        )
        .unwrap();

        let run = validate_dir(
            data.path(),
            &["--expect-exchanges", "solana,binance"],
            &mut State::default(),
        );

        assert_eq!(
            outcome(&run, TestId::FilesPresent).detail,
            vec!["binance/trades: 0 .parquet files"]
        );
        assert_eq!(run.summary.pending_files, 1);
    }

    #[test]
    fn repeated_signature_across_two_flushes_fails_t11() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), TX_LEAF);
        let first = tx_file(
            &leaf,
            &transactions(JUPITER_V6, &[(380_000_000, 1, "a"), (380_000_000, 4, "b")]),
        );
        let second = tx_file(
            &leaf,
            &transactions(JUPITER_V6, &[(380_000_003, 0, "b"), (380_000_004, 0, "c")]),
        );
        index(&first, 2);
        index(&second, 2);

        let run = validate_txs(data.path(), &[], &mut State::default());

        assert!(run.files.iter().all(|f| f.failed.is_empty()));
        assert_eq!(
            outcome(&run, TestId::UniqueTransactionKeys).detail,
            vec![format!(
                "{} (sym={JUPITER_V6}, day=2026-10-06): 1 repeated across files, \
                 first row 0: ({JUPITER_V6}, b)",
                second.display()
            )]
        );
        assert_eq!(
            outcome(&run, TestId::UniqueTransactionKeys).label,
            "unique_address_signature_per_leaf"
        );
    }

    #[test]
    fn one_signature_under_two_addresses_passes_t11() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), TX_LEAF);
        index(
            &tx_file(&leaf, &transactions(JUPITER_V6, &[(380_000_000, 1, "a")])),
            1,
        );
        index(
            &tx_file(&leaf, &transactions(WHIRLPOOL, &[(380_000_000, 1, "a")])),
            1,
        );

        let run = validate_txs(data.path(), &[], &mut State::default());

        assert_eq!(run.files.len(), 2);
        assert!(outcome(&run, TestId::UniqueTransactionKeys).passed);
    }

    #[test]
    fn t11_rekeys_seen_files_under_skip_seen() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), TX_LEAF);
        let mut state = State::default();
        index(
            &tx_file(
                &leaf,
                &transactions(
                    JUPITER_V6,
                    &[(380_000_000, 1, "a"), (380_000_001, 0, "b")],
                ),
            ),
            2,
        );
        let first_run = validate_txs(data.path(), &["--skip-seen"], &mut state);
        assert!(outcome(&first_run, TestId::UniqueTransactionKeys).passed);

        let second = tx_file(
            &leaf,
            &transactions(JUPITER_V6, &[(380_000_002, 0, "b"), (380_000_003, 0, "c")]),
        );
        index(&second, 2);
        let second_run = validate_txs(data.path(), &["--skip-seen"], &mut state);
        assert_eq!(second_run.summary.files_seen, 1);
        let detail = &outcome(&second_run, TestId::UniqueTransactionKeys).detail;
        assert_eq!(detail.len(), 1);
        assert!(
            detail[0].starts_with(&format!("{} (sym={JUPITER_V6}", second.display()))
        );

        let third_run = validate_txs(data.path(), &["--skip-seen"], &mut state);
        assert_eq!(third_run.summary.files_seen, 0);
        assert!(outcome(&third_run, TestId::UniqueTransactionKeys).passed);
    }

    #[test]
    fn flat_leaf_scopes_t11_by_file_day() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), "solana/transactions");
        tx_file(&leaf, &transactions(JUPITER_V6, &[(380_000_000, 1, "a")]));
        let mut next_day = transactions(JUPITER_V6, &[(380_000_000, 1, "a")]);
        next_day[0].source_tx_ts_us += 86_400_000_000;
        tx_file(&leaf, &next_day);

        let run = validate_txs(data.path(), &[], &mut State::default());
        assert_eq!(run.files.len(), 2);
        assert!(outcome(&run, TestId::UniqueTransactionKeys).passed);

        let same_day =
            tx_file(&leaf, &transactions(JUPITER_V6, &[(380_000_005, 0, "a")]));
        let run = validate_txs(data.path(), &[], &mut State::default());
        assert_eq!(
            outcome(&run, TestId::UniqueTransactionKeys).detail,
            vec![format!(
                "{} (sym={JUPITER_V6}, day=2026-10-06): 1 repeated across files, \
                 first row 0: ({JUPITER_V6}, a)",
                same_day.display()
            )]
        );
    }

    #[test]
    fn file_holding_another_address_is_reported() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), TX_LEAF);
        let mut rows = transactions(JUPITER_V6, &[(380_000_000, 1, "a")]);
        rows.extend(transactions(WHIRLPOOL, &[(380_000_000, 2, "b")]));
        let mixed = tx_file(&leaf, &rows);
        index(&mixed, 2);

        let run = validate_txs(data.path(), &[], &mut State::default());

        assert_eq!(
            outcome(&run, TestId::UniqueTransactionKeys).detail,
            vec![format!(
                "{} (sym={JUPITER_V6}): 1 rows under another address, first {WHIRLPOOL}",
                mixed.display()
            )]
        );
    }

    fn heartbeat(leaf: &Path, t_min_us: u64, t_max_us: u64) {
        aetelier_io::leaf_index::append_heartbeat(
            leaf,
            &aetelier_io::leaf_index::HeartbeatEntry {
                heartbeat: true,
                symbol: JUPITER_V6.to_string(),
                rows: 0,
                t_min_us,
                t_max_us,
                last_slot: Some(380_000_000),
                last_block_height: Some(358_000_000),
                schema_id: "transactions.v1".to_string(),
            },
        )
        .unwrap();
    }

    #[test]
    fn a_leaf_with_only_heartbeats_passes_and_counts_them() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), TX_LEAF);
        heartbeat(&leaf, BOOKS_START_US, BOOKS_START_US + 59_000_000);
        heartbeat(
            &leaf,
            BOOKS_START_US + 60_000_000,
            BOOKS_START_US + 119_000_000,
        );

        let run = validate_txs(data.path(), &[], &mut State::default());

        let failed: Vec<&str> = run
            .tests
            .iter()
            .filter(|t| !t.passed)
            .map(|t| t.code)
            .collect();
        assert_eq!(failed, Vec::<&str>::new());
        assert_eq!(
            (
                run.summary.files_seen,
                run.summary.pending_files,
                run.summary.torn_index_lines,
                run.summary.heartbeats,
            ),
            (0, 0, 0, 2)
        );
        assert_eq!(report_json(&run)["summary"]["heartbeats"], 2);
    }

    #[test]
    fn heartbeats_beside_files_skip_the_file_checks_and_t10() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), TX_LEAF);
        let file = tx_file(&leaf, &transactions(JUPITER_V6, &[(380_000_000, 1, "a")]));
        heartbeat(&leaf, 0, u64::MAX);
        index(&file, 1);
        heartbeat(&leaf, 0, u64::MAX);

        let run = validate_txs(data.path(), &[], &mut State::default());

        assert!(run.tests.iter().all(|t| t.passed));
        assert_eq!(run.files.len(), 1);
        assert_eq!(run.files[0].path, file.to_string_lossy());
        assert_eq!(
            (
                run.summary.pending_files,
                run.summary.torn_index_lines,
                run.summary.heartbeats
            ),
            (0, 0, 2)
        );
    }

    #[test]
    fn cex_leaves_report_no_heartbeats() {
        let data = tempfile::tempdir().unwrap();
        let leaf = leaf_dir(data.path(), BOOK_LEAF);
        index(&book_file(&leaf, BOOKS_START_US), 300);

        let run = validate_books(data.path(), &mut State::default());

        assert_eq!(run.summary.heartbeats, 0);
        assert_eq!(report_json(&run)["summary"]["heartbeats"], 0);
    }
}
