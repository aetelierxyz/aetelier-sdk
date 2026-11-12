use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aetelier_connect::ConnectionState;
use aetelier_connect::config::workers::{MarketWorkerConfig, MarketWorkerManifest};
use aetelier_connect::framework::FeedState;
use aetelier_connect::framework::feed::FeedSnapshot;
use aetelier_connect::framework::grpc::GrpcConnections;
use aetelier_connect::workers::{
    FlushReport, MarketWorker, MarketWorkerReport, SlotWindow, SnapshotFlusher,
    WorkerCommand, WorkerStatus,
};
use aetelier_types::WorkerId;
use aetelier_types::errors::PersistError;
use aetelier_types::exchanges::{Exchange, MarketType};
use aetelier_types::onchain::{ChainAddress, Transaction};
use aetelier_types::snapshots::MarketSnapshot;
use aetelier_types::synchronizers::{ClockMode, WorkerMode};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use yellowstone_grpc_proto::tonic::Status;

use crate::chain::{Chain, account_include, address, ping_id, pong, server_ping};
use crate::server::{ENDPOINT, MockConnection, MockGeyser, duplex_pool, subscribed};

const WAIT: Duration = Duration::from_secs(60);

type Windows = Arc<Mutex<Vec<(SlotWindow, u64)>>>;

struct RecordingFlusher(Arc<Mutex<Vec<MarketSnapshot>>>, Windows);

impl SnapshotFlusher for RecordingFlusher {
    fn flush_snapshots(
        &self,
        snapshots: &[MarketSnapshot],
        _output_dir: &str,
    ) -> Result<FlushReport, PersistError> {
        self.0
            .lock()
            .expect("the recording is not poisoned")
            .extend_from_slice(snapshots);
        Ok(FlushReport::default())
    }

    fn flush_slot_window(
        &self,
        snapshots: &[MarketSnapshot],
        output_dir: &str,
        window: &SlotWindow,
    ) -> Result<FlushReport, PersistError> {
        let t_max_us = snapshots.iter().map(|snapshot| snapshot.ts_us).max();
        self.1
            .lock()
            .expect("the recording is not poisoned")
            .push((window.clone(), t_max_us.unwrap_or_default()));
        self.flush_snapshots(snapshots, output_dir)
    }
}

struct RunningWorker {
    status: watch::Receiver<WorkerStatus>,
    snapshots: Arc<Mutex<Vec<MarketSnapshot>>>,
    windows: Windows,
    stop: watch::Sender<bool>,
    commands: mpsc::Sender<WorkerCommand>,
    task: JoinHandle<anyhow::Result<MarketWorkerReport>>,
    ledger: PathBuf,
}

struct Stopped {
    report: MarketWorkerReport,
    status: WorkerStatus,
    rows: Vec<Transaction>,
    windows: Vec<(SlotWindow, u64)>,
    ledger: Vec<serde_json::Value>,
}

impl RunningWorker {
    async fn until(&mut self, what: impl FnMut(&WorkerStatus) -> bool) -> WorkerStatus {
        timeout(WAIT, self.status.wait_for(what))
            .await
            .expect("the status arrives within the wait")
            .expect("the worker keeps publishing its status")
            .clone()
    }

    async fn stop(self) -> Stopped {
        let _ = self.stop.send(true);
        self.ended().await
    }

    async fn ended(self) -> Stopped {
        let report = timeout(WAIT, self.task)
            .await
            .expect("the worker ends within the wait")
            .expect("the worker task joins")
            .expect("run returns Ok");
        let rows = self
            .snapshots
            .lock()
            .expect("the recording is not poisoned")
            .iter()
            .flat_map(|snapshot| snapshot.transactions.iter().cloned())
            .collect();
        let status = self.status.borrow().clone();
        let windows = self
            .windows
            .lock()
            .expect("the recording is not poisoned")
            .clone();
        Stopped {
            report,
            status,
            rows,
            windows,
            ledger: ledger_lines(&self.ledger),
        }
    }
}

fn entry(seed: u8, extra: &str) -> String {
    format!("\n[[workers]]\nsymbol = \"{}\"\n{extra}", address(seed))
}

fn configs(dir: &Path, workers: &str) -> Vec<MarketWorkerConfig> {
    configs_with(dir, "", workers)
}

fn configs_with(dir: &Path, reconnect: &str, workers: &str) -> Vec<MarketWorkerConfig> {
    let toml = format!(
        r#"
[collect]
exchange = "solana"
market_type = "onchain"
transport = "grpc"

[collect.grpc]
endpoint = "{ENDPOINT}"

[collect.reconnect]
jitter_factor = 0.0
{reconnect}
[collect.datatypes.transactions]
enabled = true

[collect.sync]
sync_mode = "on_slot"
flush_threshold = 1

[collect.sync.update_frequency]
value = 1
unit = "Secs"

[[collect.output]]
type = "parquet"
dir = "{}"
{workers}"#,
        dir.display()
    );
    MarketWorkerManifest::from_str(&toml)
        .expect("the manifest parses")
        .resolve_admitted()
        .expect("every worker is admitted")
}

fn start(
    config: MarketWorkerConfig,
    pool: &GrpcConnections,
    dir: &Path,
) -> RunningWorker {
    let address = ChainAddress::solana(&config.common.symbol).expect("a solana address");
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let windows = Windows::default();
    let initial = WorkerStatus::initial(
        WorkerId::new(),
        Exchange::Solana,
        address,
        MarketType::Onchain,
        WorkerMode::Clock {
            clock: ClockMode::SlotDriven,
            period_us: 1_000_000,
        },
        vec!["parquet".to_string()],
        vec!["transactions".to_string()],
    );
    let (status_tx, status) = watch::channel(initial);
    let (stop, shutdown) = watch::channel(false);
    let (commands, received) = mpsc::channel(4);
    let worker = MarketWorker::from_config_with_flusher(
        config,
        Some(Box::new(RecordingFlusher(
            snapshots.clone(),
            windows.clone(),
        ))),
    )
    .expect("the worker builds")
    .with_grpc_connections(pool.clone())
    .with_status_sender(status_tx)
    .with_command_receiver(received);
    RunningWorker {
        status,
        snapshots,
        windows,
        stop,
        commands,
        task: tokio::spawn(worker.run(shutdown)),
        ledger: dir
            .join("gaps")
            .join(format!("solana_{address}_gap_ledger.jsonl")),
    }
}

fn ledger_lines(path: &Path) -> Vec<serde_json::Value> {
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .lines()
            .map(|line| serde_json::from_str(line).expect("a JSON ledger line"))
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("{}: {error}", path.display()),
    }
}

fn feed(status: &WorkerStatus) -> Option<&FeedSnapshot> {
    match status.feeds.as_slice() {
        [feed] => Some(feed),
        _ => None,
    }
}

fn states(status: &WorkerStatus) -> Option<(ConnectionState, FeedState)> {
    feed(status).map(|feed| (status.connection_state, feed.state))
}

fn streamed(events: u64) -> impl FnMut(&WorkerStatus) -> bool {
    move |status| {
        status.total_events == events
            && states(status) == Some((ConnectionState::Streaming, FeedState::Live))
    }
}

fn bounds(line: &serde_json::Value) -> Vec<u64> {
    [
        "prev_slot",
        "prev_height",
        "next_slot",
        "next_height",
        "blocks_lost",
        "window_us",
    ]
    .into_iter()
    .map(|key| {
        line[key]
            .as_u64()
            .unwrap_or_else(|| panic!("{key} in {line}"))
    })
    .collect()
}

fn positions(rows: &[Transaction]) -> Vec<(u64, u64)> {
    rows.iter().map(Transaction::position).collect()
}

fn base58(seeds: &[u8]) -> Vec<String> {
    seeds
        .iter()
        .map(|&seed| address(seed).to_base58())
        .collect()
}

async fn settle(connection: &mut MockConnection) {
    connection.send(server_ping());
    while ping_id(&connection.next_request().await).is_none() {}
}

const BANK_ROWS: u64 = 2_000;

#[derive(Clone, Copy, Debug)]
enum Halt {
    Stop,
    Restart,
    Shutdown,
    PauseThenStop,
}

async fn halted_after(halt: Halt, yields: usize) -> (u64, Stopped) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut worker = start(
        configs(dir.path(), &entry(1, "")).remove(0),
        &pool,
        dir.path(),
    );
    let mut connection = subscribed(&mock).await;
    connection.send_all([chain.meta(100), chain.meta(101)]);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    settle(&mut connection).await;
    worker.until(streamed(2)).await;
    let watched = [address(1)];
    let rows: Vec<(u64, &[ChainAddress])> =
        (0..BANK_ROWS).map(|index| (index, &watched[..])).collect();
    connection.send_all(chain.bank(103, &rows));
    for _ in 0..yields {
        tokio::task::yield_now().await;
    }
    let commands: &[WorkerCommand] = match halt {
        Halt::Stop => &[WorkerCommand::Stop],
        Halt::Restart => &[WorkerCommand::Restart],
        Halt::Shutdown => &[],
        Halt::PauseThenStop => &[WorkerCommand::Pause, WorkerCommand::Stop],
    };
    for command in commands {
        worker
            .commands
            .send(command.clone())
            .await
            .expect("the worker takes commands");
    }
    if commands.is_empty() {
        worker
            .stop
            .send(true)
            .expect("the worker watches its shutdown");
    }
    let stopped = worker.ended().await;
    drop(connection);
    let kept = stopped.rows.iter().filter(|row| row.slot == 103).count() as u64;
    (kept, stopped)
}

#[tokio::test(start_paused = true)]
async fn a_solana_worker_streams_rows_into_snapshots_and_ledgers_a_slot_gap_across_a_reconnect()
 {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let config = configs(dir.path(), &entry(1, "")).remove(0);
    let mut worker = start(config, &pool, dir.path());
    let mut connection = subscribed(&mock).await;
    assert_eq!(account_include(connection.first_request()), base58(&[1]));
    let connecting = worker.until(|status| feed(status).is_some()).await;
    assert_eq!(
        states(&connecting),
        Some((ConnectionState::Connecting, FeedState::Subscribing))
    );
    connection.send_all([chain.meta(100), chain.meta(101)]);
    for slot in 102..=105 {
        connection.send_all(chain.bank(slot, &[(0, &[address(1)])]));
    }
    settle(&mut connection).await;
    worker.until(streamed(8)).await;
    connection.reset();
    let reconnecting = worker
        .until(|status| {
            states(status)
                == Some((
                    ConnectionState::Reconnecting { attempt: 1 },
                    FeedState::Reconnecting,
                ))
        })
        .await;
    assert_eq!(reconnecting.reconnect_count, 1);
    let mut reconnected = subscribed(&mock).await;
    assert_eq!(dial.dials().len(), 2);
    assert_eq!(reconnected.first_request(), connection.first_request());
    let subscribing = worker
        .until(|status| {
            states(status) == Some((ConnectionState::Subscribing, FeedState::Subscribing))
        })
        .await;
    assert_eq!(subscribing.total_events, 8);
    reconnected.send_all([chain.meta(139), chain.meta(140)]);
    for slot in 141..=142 {
        reconnected.send_all(chain.bank(slot, &[(0, &[address(1)])]));
    }
    settle(&mut reconnected).await;
    worker.until(streamed(12)).await;
    let stopped = worker.stop().await;
    assert_eq!(
        positions(&stopped.rows),
        [(102, 0), (103, 0), (104, 0), (105, 0), (141, 0), (142, 0)]
    );
    for row in &stopped.rows {
        assert_eq!(row.address, address(1));
        assert_eq!(row.check_invariants(), Ok(()));
    }
    assert_eq!(stopped.ledger.len(), 1, "{:?}", stopped.ledger);
    let line = &stopped.ledger[0];
    assert_eq!(line["cause"], "slot_gap");
    assert_eq!(line["exchange"], "solana");
    assert_eq!(line["symbol"], address(1).to_base58().as_str());
    assert_eq!(
        bounds(line),
        [
            105,
            Chain::height(105),
            141,
            Chain::height(141),
            35,
            36_000_000
        ]
    );
    assert!(line["possible_dropped_trades"].is_null(), "{line}");
    let status = &stopped.status;
    assert_eq!(
        states(status),
        Some((ConnectionState::Disconnected, FeedState::Closed))
    );
    assert!(!feed(status).is_some_and(|feed| feed.partial));
    assert_eq!(status.reconnect_count, 1);
    assert_eq!(stopped.report.ingestion.reconnect_count, 1);
    assert!(
        status.ws_latency_us.is_some_and(|latency| latency > 0.0),
        "{:?}",
        status.ws_latency_us
    );
    assert_eq!(mock.subscribes().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn two_solana_workers_share_one_opening_request_and_one_conn_epoch() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let signer =
        "\n[workers.datatypes.transactions]\nenabled = true\nmatch = \"signer\"\n";
    let mut configs =
        configs(dir.path(), &format!("{}{}", entry(1, ""), entry(2, signer))).into_iter();
    let mut first = start(configs.next().expect("two workers"), &pool, dir.path());
    let mut second = start(configs.next().expect("two workers"), &pool, dir.path());
    let mut connection = subscribed(&mock).await;
    assert_eq!(account_include(connection.first_request()), base58(&[1, 2]));
    assert_eq!(
        pool.connections()
            .into_iter()
            .map(|(_, attaches)| attaches)
            .collect::<Vec<_>>(),
        [2]
    );
    connection.send_all([chain.meta(100), chain.meta(101)]);
    connection.send_all(chain.bank(
        102,
        &[
            (0, &[address(1)]),
            (1, &[address(2)]),
            (2, &[address(9), address(2)]),
        ],
    ));
    connection.send_all(chain.bank(103, &[(0, &[address(2)]), (1, &[address(1)])]));
    settle(&mut connection).await;
    first.until(streamed(4)).await;
    second.until(streamed(4)).await;
    let first = first.stop().await;
    let second = second.stop().await;
    assert_eq!(positions(&first.rows), [(102, 0), (103, 1)]);
    assert_eq!(positions(&second.rows), [(102, 1), (103, 0)]);
    assert!(first.rows.iter().all(|row| row.address == address(1)));
    assert!(second.rows.iter().all(|row| row.address == address(2)));
    let mut rows: Vec<&Transaction> = first.rows.iter().chain(&second.rows).collect();
    rows.sort_by_key(|row| row.position());
    let epoch = rows[0].conn_epoch_us;
    assert!(epoch > 0);
    assert!(
        rows.iter().all(|row| row.conn_epoch_us == epoch),
        "{rows:?}"
    );
    assert!(
        rows.windows(2)
            .all(|pair| pair[0].recv_seq < pair[1].recv_seq),
        "{rows:?}"
    );
    assert_eq!(first.status.source_metrics.filtered_out, 1);
    assert_eq!(second.status.source_metrics.filtered_out, 0);
    assert_eq!(mock.subscribes().len(), 1);
    assert_eq!(dial.dials().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_rejected_stream_leaves_the_worker_rejected_and_disconnected_without_a_retry() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut worker = start(
        configs(dir.path(), &entry(1, "")).remove(0),
        &pool,
        dir.path(),
    );
    let mut connection = subscribed(&mock).await;
    connection.send_all([chain.meta(100), chain.meta(101)]);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    settle(&mut connection).await;
    worker.until(streamed(2)).await;
    connection.fail(Status::permission_denied(
        "the token lacks the subscribe scope",
    ));
    let ended = worker.ended().await;
    let status = &ended.status;
    assert_eq!(
        states(status),
        Some((ConnectionState::Disconnected, FeedState::Rejected))
    );
    let reason = feed(status)
        .and_then(|feed| feed.reason.clone())
        .unwrap_or_default();
    for part in [
        "PermissionDenied",
        "the token lacks the subscribe scope",
        "x_token_env",
    ] {
        assert!(reason.contains(part), "{reason}");
    }
    assert_eq!(status.reconnect_count, 0);
    assert_eq!(ended.report.ingestion.reconnect_count, 0);
    assert_eq!(ended.report.feeds, status.feeds);
    assert_eq!(positions(&ended.rows), [(102, 0)]);
    sleep(Duration::from_secs(120)).await;
    assert_eq!(mock.subscribes().len(), 1);
    assert_eq!(dial.dials().len(), 1);
    assert!(pool.connections().is_empty());
    assert!(ended.ledger.is_empty(), "{:?}", ended.ledger);
}

#[tokio::test(start_paused = true)]
async fn a_rejected_update_rejects_only_the_late_worker() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mock = MockGeyser::default();
    mock.auto_pong(false);
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut configs =
        configs(dir.path(), &format!("{}{}", entry(1, ""), entry(2, ""))).into_iter();
    let mut early = start(configs.next().expect("two workers"), &pool, dir.path());
    let late_config = configs.next().expect("two workers");
    let mut connection = subscribed(&mock).await;
    connection.send(pong(1));
    connection.send_all([chain.meta(100), chain.meta(101)]);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    settle(&mut connection).await;
    early.until(streamed(2)).await;
    let late = start(late_config, &pool, dir.path());
    let update = connection.next_request().await;
    assert_eq!(account_include(&update), base58(&[1, 2]));
    assert!(ping_id(&connection.next_request().await).is_some());
    connection.fail(Status::invalid_argument(
        "account_include names a closed account",
    ));
    let rejected = late.ended().await;
    assert_eq!(
        states(&rejected.status),
        Some((ConnectionState::Disconnected, FeedState::Rejected))
    );
    let reason = feed(&rejected.status)
        .and_then(|feed| feed.reason.clone())
        .unwrap_or_default();
    assert!(reason.contains("filter update rejected: "), "{reason}");
    assert!(reason.contains("InvalidArgument"), "{reason}");
    assert_eq!(rejected.status.reconnect_count, 0);
    assert!(rejected.rows.is_empty() && rejected.ledger.is_empty());
    early
        .until(|status| {
            states(status)
                == Some((
                    ConnectionState::Reconnecting { attempt: 1 },
                    FeedState::Reconnecting,
                ))
        })
        .await;
    let mut reconnected = subscribed(&mock).await;
    assert_eq!(account_include(reconnected.first_request()), base58(&[1]));
    early
        .until(|status| {
            states(status) == Some((ConnectionState::Subscribing, FeedState::Subscribing))
        })
        .await;
    reconnected.send(pong(1));
    reconnected.send_all([chain.meta(120), chain.meta(121)]);
    reconnected.send_all(chain.bank(122, &[(0, &[address(1)])]));
    settle(&mut reconnected).await;
    early.until(streamed(4)).await;
    let early = early.stop().await;
    assert_eq!(positions(&early.rows), [(102, 0), (122, 0)]);
    assert_eq!(early.status.reconnect_count, 1);
    assert_eq!(early.ledger.len(), 1, "{:?}", early.ledger);
    assert_eq!(early.ledger[0]["cause"], "slot_gap");
    assert_eq!(
        bounds(&early.ledger[0]),
        [
            102,
            Chain::height(102),
            122,
            Chain::height(122),
            19,
            20_000_000
        ]
    );
    assert_eq!(mock.subscribes().len(), 2);
    assert_eq!(dial.dials().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_stop_during_an_outage_detaches_at_once_and_never_redials() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let slow = "initial_delay_ms = 20000\nmax_delay_ms = 20000\n";
    let config = configs_with(dir.path(), slow, &entry(1, "")).remove(0);
    let mut worker = start(config, &pool, dir.path());
    let mut connection = subscribed(&mock).await;
    connection.send_all([chain.meta(100), chain.meta(101)]);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    settle(&mut connection).await;
    worker.until(streamed(2)).await;
    connection.reset();
    worker
        .until(|status| {
            states(status)
                == Some((
                    ConnectionState::Reconnecting { attempt: 1 },
                    FeedState::Reconnecting,
                ))
        })
        .await;
    let stopped_at = Instant::now();
    worker
        .commands
        .send(WorkerCommand::Stop)
        .await
        .expect("the worker takes commands");
    let ended = worker.ended().await;
    assert!(
        stopped_at.elapsed() < Duration::from_secs(1),
        "{:?}",
        stopped_at.elapsed()
    );
    assert!(pool.connections().is_empty());
    assert_eq!(
        states(&ended.status),
        Some((ConnectionState::Disconnected, FeedState::Closed))
    );
    assert_eq!(ended.status.reconnect_count, 1);
    assert_eq!(positions(&ended.rows), [(102, 0)]);
    assert!(ended.ledger.is_empty(), "{:?}", ended.ledger);
    sleep(Duration::from_secs(120)).await;
    assert_eq!(dial.dials().len(), 1);
    assert_eq!(mock.subscribes().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_stop_restart_or_shutdown_mid_bank_finishes_the_bank_and_closes_clean() {
    let mut whole = 0;
    for halt in [Halt::Stop, Halt::Restart, Halt::Shutdown] {
        for yields in 0..8 {
            let (kept, stopped) = halted_after(halt, yields).await;
            assert!(
                kept == 0 || kept == BANK_ROWS,
                "{halt:?} after {yields} yields kept {kept} of {BANK_ROWS} rows"
            );
            assert_eq!(
                states(&stopped.status),
                Some((ConnectionState::Disconnected, FeedState::Closed))
            );
            assert!(
                !feed(&stopped.status).is_some_and(|feed| feed.partial),
                "{halt:?} after {yields} yields"
            );
            assert!(stopped.ledger.is_empty(), "{:?}", stopped.ledger);
            whole += usize::from(kept == BANK_ROWS);
        }
    }
    assert!(whole > 0);
}

#[tokio::test(start_paused = true)]
async fn a_bank_cut_by_a_pause_closes_the_feed_partial() {
    let mut cut = 0;
    for yields in 0..8 {
        let (kept, stopped) = halted_after(Halt::PauseThenStop, yields).await;
        assert_eq!(
            states(&stopped.status),
            Some((ConnectionState::Disconnected, FeedState::Closed))
        );
        let partial = feed(&stopped.status).is_some_and(|feed| feed.partial);
        assert!(
            partial || kept == 0 || kept == BANK_ROWS,
            "a pause after {yields} yields kept {kept} of {BANK_ROWS} rows"
        );
        cut += usize::from(kept > 0 && kept < BANK_ROWS);
    }
    assert!(cut > 0);
}

#[tokio::test(start_paused = true)]
async fn a_quiet_solana_worker_flushes_windows_with_its_address_and_the_newest_slot_at_their_end()
 {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut worker = start(
        configs(dir.path(), &entry(1, "")).remove(0),
        &pool,
        dir.path(),
    );
    let mut connection = subscribed(&mock).await;
    connection.send_all([chain.meta(100), chain.meta(101)]);
    for slot in 102..=106 {
        connection.send_all(chain.bank(slot, &[]));
    }
    settle(&mut connection).await;
    worker.until(|status| status.total_events == 5).await;
    let stopped = worker.stop().await;

    assert!(stopped.rows.is_empty());
    assert!(
        stopped
            .windows
            .iter()
            .all(|(window, _)| window.symbol == address(1).to_base58())
    );
    assert_eq!(
        stopped
            .windows
            .iter()
            .map(|(window, _)| window.last_tick.map(|tick| tick.slot))
            .collect::<Vec<_>>(),
        [None, Some(102), Some(103), Some(104), Some(105)]
    );
    for (window, t_max_us) in &stopped.windows {
        if let Some(tick) = window.last_tick {
            assert_eq!(tick.block_height, Chain::height(tick.slot));
            assert_eq!(tick.block_time_us, *t_max_us);
        }
    }
}
