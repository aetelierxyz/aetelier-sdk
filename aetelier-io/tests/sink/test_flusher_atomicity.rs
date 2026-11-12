//! Atomic-finalize and leaf-index behavior of `ParquetSnapshotFlusher`:
//! complete-or-absent files, the append-only `index.jsonl`, the
//! single-writer lock, the defined "pending" crash state, and the staging
//! orphan sweep.

use aetelier_connect::framework::SlotTick;
use aetelier_connect::workers::{SlotWindow, SnapshotFlusher};
use aetelier_io::leaf_index;
use aetelier_io::sink::ParquetSnapshotFlusher;
use aetelier_types::TradeSide;
use aetelier_types::orderbooks::f64_to_decimal;
use aetelier_types::snapshots::MarketSnapshot;
use aetelier_types::trades::Trade;
use aetelier_types::trading_pair::TradingPair;
use tempfile::tempdir;

fn snapshot_with_trade(ts_us: u64) -> MarketSnapshot {
    let mut snap = MarketSnapshot::empty(ts_us);
    snap.trades.push(Trade {
        source_trade_ts_us: ts_us,
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair: TradingPair::new("BTC", "USDT"),
        side: TradeSide::Buy,
        amount: f64_to_decimal(0.001),
        price: f64_to_decimal(23536.30),
        exchange: "kraken".to_string(),
        id: format!("kraken_{ts_us}"),
        origin: Default::default(),
    });
    snap
}

#[test]
fn flush_finalizes_hashes_and_indexes_every_file() {
    let dir = tempdir().unwrap();
    let snaps = vec![
        snapshot_with_trade(1_700_000_000_000_000),
        snapshot_with_trade(1_700_000_000_250_000),
    ];
    let report = ParquetSnapshotFlusher
        .flush_snapshots(&snaps, dir.path().to_str().unwrap())
        .unwrap();
    assert_eq!(report.files_written, 1);

    let leaf = dir.path().join("trades");
    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert_eq!(torn, 0);
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.rows, 2);
    assert_eq!(entry.t_min_us, 1_700_000_000_000_000);
    assert_eq!(entry.t_max_us, 1_700_000_000_250_000);
    assert_eq!(entry.schema_id, "trades.v1");
    assert_eq!(
        entry.sha256,
        leaf_index::sha256_file(&leaf.join(&entry.filename)).unwrap()
    );
    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
    let staging = leaf.join(leaf_index::STAGING_DIR);
    assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 0);
}

#[test]
fn unlisted_file_classifies_pending_never_silent() {
    let dir = tempdir().unwrap();
    ParquetSnapshotFlusher
        .flush_snapshots(
            &[snapshot_with_trade(1_700_000_001_000_000)],
            dir.path().to_str().unwrap(),
        )
        .unwrap();
    let leaf = dir.path().join("trades");
    let (entries, _) = leaf_index::read_index(&leaf).unwrap();
    let listed = leaf.join(&entries[0].filename);
    let planted = leaf.join("kraken_BTC-USDT_trades_sync_planted.parquet");
    std::fs::copy(&listed, &planted).unwrap();

    assert_eq!(
        leaf_index::pending_files(&leaf).unwrap(),
        vec!["kraken_BTC-USDT_trades_sync_planted.parquet".to_string()]
    );
}

#[test]
fn legacy_leaf_without_index_has_nothing_pending() {
    let dir = tempdir().unwrap();
    let leaf = dir.path().join("trades");
    std::fs::create_dir_all(&leaf).unwrap();
    std::fs::write(leaf.join("legacy.parquet"), b"legacy bytes").unwrap();

    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert!(entries.is_empty());
    assert_eq!(torn, 0);
}

#[test]
fn staging_orphans_are_swept_on_the_next_flush() {
    let dir = tempdir().unwrap();
    let staging = dir.path().join("trades").join(leaf_index::STAGING_DIR);
    std::fs::create_dir_all(&staging).unwrap();
    let orphan = staging.join("crashed_flush_leftover.parquet");
    std::fs::write(&orphan, b"torn write").unwrap();

    ParquetSnapshotFlusher
        .flush_snapshots(
            &[snapshot_with_trade(1_700_000_002_000_000)],
            dir.path().to_str().unwrap(),
        )
        .unwrap();

    assert!(!orphan.exists());
    let leaf = dir.path().join("trades");
    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
}

#[test]
fn second_writer_on_the_same_leaf_is_refused() {
    use fs4::fs_std::FileExt;

    let dir = tempdir().unwrap();
    ParquetSnapshotFlusher
        .flush_snapshots(
            &[snapshot_with_trade(1_700_000_003_000_000)],
            dir.path().to_str().unwrap(),
        )
        .unwrap();

    let lock_path = dir.path().join("trades").join(leaf_index::LOCK_FILE);
    let foreign = std::fs::OpenOptions::new()
        .write(true)
        .open(&lock_path)
        .unwrap();
    assert!(
        !foreign.try_lock_exclusive().unwrap(),
        "the flusher must still hold the leaf's exclusive flock"
    );
}

#[test]
fn templated_dir_lands_day_bounded_canonical_leaves() {
    let dir = tempdir().unwrap();
    let binding = "0f6e2b1c-8f4e-4c1e-9d2a-1b2c3d4e5f60";
    let template = dir
        .path()
        .join("recorded/binance/spot/{data_type}")
        .join(binding);
    let ts_us: u64 = 1_788_913_805_200_000;
    let mut snap = snapshot_with_trade(ts_us);
    snap.funding_rate
        .push(aetelier_types::funding::FundingRate {
            funding_rate_ts_us: ts_us,
            local_funding_ts_us: ts_us,
            recv_seq: 1,
            conn_epoch_us: 1,
            pair: TradingPair::new("BTC", "USDT"),
            funding_rate: "0.0001".parse().unwrap(),
            premium: None,
            interval_hours: 8,
            next_funding_ts_us: 0,
            exchange: "binance".to_string(),
        });
    ParquetSnapshotFlusher
        .flush_snapshots(&[snap], template.to_str().unwrap())
        .unwrap();

    let day = chrono::DateTime::from_timestamp_micros(ts_us as i64)
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();
    for datatype in ["trades", "funding_rates"] {
        let leaf = dir
            .path()
            .join("recorded/binance/spot")
            .join(datatype)
            .join(binding)
            .join(&day);
        let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
        assert_eq!(torn, 0, "{datatype}");
        assert_eq!(entries.len(), 1, "{datatype}");
        assert_eq!(entries[0].schema_id, format!("{datatype}.v1"));
        assert!(leaf.join(&entries[0].filename).exists());
    }
    assert!(
        !dir.path().join("recorded/binance/spot/fundings").exists(),
        "legacy fundings leaf must not appear on the wave layout"
    );
}

#[test]
fn torn_index_tail_is_counted_not_fatal() {
    let dir = tempdir().unwrap();
    ParquetSnapshotFlusher
        .flush_snapshots(
            &[snapshot_with_trade(1_700_000_004_000_000)],
            dir.path().to_str().unwrap(),
        )
        .unwrap();
    let leaf = dir.path().join("trades");
    let index = leaf.join(leaf_index::INDEX_FILE);
    let mut bytes = std::fs::read(&index).unwrap();
    bytes.extend_from_slice(b"{\"filename\":\"torn");
    std::fs::write(&index, bytes).unwrap();

    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(torn, 1);
}

fn trade_for(base: &str, ts_us: u64) -> Trade {
    Trade {
        source_trade_ts_us: ts_us,
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair: TradingPair::new(base, "USDT"),
        side: TradeSide::Sell,
        amount: f64_to_decimal(0.5),
        price: f64_to_decimal(100.0),
        exchange: "binance".to_string(),
        id: format!("{base}_{ts_us}"),
        origin: Default::default(),
    }
}

#[test]
fn concurrent_writers_sharing_a_leaf_never_lose_a_staged_file() {
    let dir = tempdir().unwrap();
    let template = dir
        .path()
        .join("recorded/binance/spot/{data_type}/shared-binding")
        .to_str()
        .unwrap()
        .to_string();
    let ts_us: u64 = 1_788_913_805_200_000;
    let bases = ["BTC", "ETH", "SOL", "XRP", "ADA", "DOT"];
    let rounds: u64 = 30;
    std::thread::scope(|scope| {
        for base in bases {
            let template = template.clone();
            scope.spawn(move || {
                for round in 0..rounds {
                    let mut snap = MarketSnapshot::empty(ts_us + round);
                    snap.trades.push(trade_for(base, ts_us + round));
                    ParquetSnapshotFlusher
                        .flush_snapshots(&[snap], &template)
                        .unwrap_or_else(|e| panic!("{base} round {round}: {e}"));
                }
            });
        }
    });

    let day = chrono::DateTime::from_timestamp_micros(ts_us as i64)
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();
    let leaf = dir
        .path()
        .join("recorded/binance/spot/trades/shared-binding")
        .join(day);
    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert_eq!(torn, 0);
    assert_eq!(entries.len() as u64, bases.len() as u64 * rounds);
    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
    for entry in &entries {
        assert_eq!(
            entry.sha256,
            leaf_index::sha256_file(&leaf.join(&entry.filename)).unwrap()
        );
    }
}

#[test]
fn transactions_land_in_their_own_day_bounded_leaf() {
    use aetelier_io::naming::{FileKind, parse_file_name};
    use aetelier_types::onchain::{
        ChainAddress, Commitment, MatchRole, Transaction, TransactionVersion,
    };

    let address = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
    let dir = tempdir().unwrap();
    let binding = "8d1c2a77-5b8e-4f0e-a1c3-0d9e8f7a6b5c";
    let template = dir
        .path()
        .join("recorded/solana/onchain/{data_type}")
        .join(binding);
    let ts_us: u64 = 1_791_321_067_000_000;
    let tx = |slot: u64, tx_index: u64, signature: &str| Transaction {
        exchange: "solana".to_string(),
        address: ChainAddress::solana(address).unwrap(),
        match_role: MatchRole::AccountKey,
        source_tx_ts_us: ts_us,
        local_tx_ts_us: ts_us + 400_000,
        source_tx_rtt_us: 0,
        recv_seq: tx_index,
        conn_epoch_us: 1,
        commitment: Commitment::Finalized,
        slot,
        block_height: slot - 22_000_000,
        blockhash: "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn".to_string(),
        tx_index,
        signature: signature.to_string(),
        version: TransactionVersion::Legacy,
        success: true,
        fee_lamports: 5_000,
        compute_units_consumed: None,
        compute_unit_limit: None,
        compute_unit_price_micro_lamports: None,
        priority_fee_lamports: None,
        cost_units: None,
        signers: Vec::new(),
        account_keys: vec![address.to_string()],
        log_messages: None,
        failure: None,
    };
    let mut first = MarketSnapshot::empty(ts_us);
    first.transactions = vec![tx(380_000_000, 1, "sig-1"), tx(380_000_000, 2, "sig-2")];
    let mut second = MarketSnapshot::empty(ts_us + 1_000_000);
    second.transactions = vec![tx(380_000_000, 2, "sig-2"), tx(380_000_001, 0, "sig-3")];

    let report = ParquetSnapshotFlusher
        .flush_snapshots(&[first, second], template.to_str().unwrap())
        .unwrap();
    assert_eq!(report.files_written, 1);

    let day = chrono::DateTime::from_timestamp_micros(ts_us as i64)
        .unwrap()
        .format("%Y-%m-%d")
        .to_string();
    let leaf = dir
        .path()
        .join("recorded/solana/onchain/transactions")
        .join(binding)
        .join(day);
    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert_eq!(torn, 0);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].schema_id, "transactions.v1");
    assert_eq!(entries[0].rows, 3);
    let parsed = parse_file_name(&entries[0].filename).unwrap();
    assert_eq!(
        (parsed.exchange, parsed.symbol, parsed.kind),
        ("solana", address, FileKind::Transactions)
    );
    let rows =
        aetelier_io::onchain::read_transactions_parquet(&leaf.join(&entries[0].filename))
            .unwrap();
    let keys: Vec<(u64, u64, &str)> = rows
        .iter()
        .map(|t| (t.slot, t.tx_index, t.signature.as_str()))
        .collect();
    assert_eq!(
        keys,
        vec![
            (380_000_000, 1, "sig-1"),
            (380_000_000, 2, "sig-2"),
            (380_000_001, 0, "sig-3")
        ]
    );
}

const JUPITER: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const WINDOW_START_US: u64 = 1_791_321_067_000_000;
const MIDNIGHT_US: u64 = 1_791_331_200_000_000;

fn quiet_window(start_us: u64, rows: u64) -> Vec<MarketSnapshot> {
    (0..rows)
        .map(|row| MarketSnapshot::empty(start_us + row * 1_000_000))
        .collect()
}

fn jupiter_window(last_tick: Option<SlotTick>) -> SlotWindow {
    SlotWindow {
        symbol: JUPITER.to_string(),
        last_tick,
    }
}

fn utc_day(ts_us: u64) -> String {
    chrono::DateTime::from_timestamp_micros(ts_us as i64)
        .unwrap()
        .format("%Y-%m-%d")
        .to_string()
}

fn jupiter_transaction(
    slot: u64,
    signature: &str,
    ts_us: u64,
) -> aetelier_types::onchain::Transaction {
    use aetelier_types::onchain::{
        ChainAddress, Commitment, MatchRole, Transaction, TransactionVersion,
    };
    Transaction {
        exchange: "solana".to_string(),
        address: ChainAddress::solana(JUPITER).unwrap(),
        match_role: MatchRole::AccountKey,
        source_tx_ts_us: ts_us,
        local_tx_ts_us: ts_us + 400_000,
        source_tx_rtt_us: 0,
        recv_seq: 0,
        conn_epoch_us: 1,
        commitment: Commitment::Finalized,
        slot,
        block_height: slot - 22_000_000,
        blockhash: "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn".to_string(),
        tx_index: 0,
        signature: signature.to_string(),
        version: TransactionVersion::Legacy,
        success: true,
        fee_lamports: 5_000,
        compute_units_consumed: None,
        compute_unit_limit: None,
        compute_unit_price_micro_lamports: None,
        priority_fee_lamports: None,
        cost_units: None,
        signers: Vec::new(),
        account_keys: vec![JUPITER.to_string()],
        log_messages: None,
        failure: None,
    }
}

#[test]
fn a_quiet_slot_window_appends_exactly_one_heartbeat_line() {
    let dir = tempdir().unwrap();
    let binding = "8d1c2a77-5b8e-4f0e-a1c3-0d9e8f7a6b5c";
    let template = dir
        .path()
        .join("recorded/solana/onchain/{data_type}")
        .join(binding);
    let tick = SlotTick {
        slot: 380_000_123,
        block_height: 358_000_001,
        block_time_us: WINDOW_START_US + 2_000_000,
    };

    let report = ParquetSnapshotFlusher
        .flush_slot_window(
            &quiet_window(WINDOW_START_US, 3),
            template.to_str().unwrap(),
            &jupiter_window(Some(tick)),
        )
        .unwrap();

    assert_eq!((report.files_written, report.bytes_written), (0, 0));
    let leaf = dir
        .path()
        .join("recorded/solana/onchain/transactions")
        .join(binding)
        .join(utc_day(WINDOW_START_US));
    assert_eq!(
        std::fs::read_to_string(leaf.join(leaf_index::INDEX_FILE)).unwrap(),
        format!(
            "{{\"heartbeat\":true,\"symbol\":\"{JUPITER}\",\"rows\":0,\
             \"t_min_us\":1791321067000000,\"t_max_us\":1791321069000000,\
             \"last_slot\":380000123,\"last_block_height\":358000001,\
             \"schema_id\":\"transactions.v1\"}}\n"
        )
    );
    let index = leaf_index::read_leaf_index(&leaf).unwrap();
    assert_eq!(
        (index.entries.len(), index.heartbeats.len(), index.torn),
        (0, 1, 0)
    );
    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
    assert!(!leaf.join(leaf_index::STAGING_DIR).exists());
}

#[test]
fn a_quiet_window_without_a_slot_tick_writes_null_slots_in_the_flat_leaf() {
    let dir = tempdir().unwrap();

    ParquetSnapshotFlusher
        .flush_slot_window(
            &quiet_window(WINDOW_START_US, 2),
            dir.path().to_str().unwrap(),
            &jupiter_window(None),
        )
        .unwrap();

    let leaf = dir.path().join("transactions");
    assert_eq!(
        leaf_index::read_leaf_index(&leaf).unwrap(),
        leaf_index::LeafIndex {
            entries: Vec::new(),
            heartbeats: vec![leaf_index::HeartbeatEntry {
                heartbeat: true,
                symbol: JUPITER.to_string(),
                rows: 0,
                t_min_us: WINDOW_START_US,
                t_max_us: WINDOW_START_US + 1_000_000,
                last_slot: None,
                last_block_height: None,
                schema_id: "transactions.v1".to_string(),
            }],
            torn: 0,
        }
    );
    assert!(
        std::fs::read_to_string(leaf.join(leaf_index::INDEX_FILE))
            .unwrap()
            .contains("\"last_slot\":null,\"last_block_height\":null,")
    );
}

#[test]
fn a_quiet_window_across_midnight_lands_in_the_day_of_its_end() {
    let dir = tempdir().unwrap();
    let template = dir
        .path()
        .join("recorded/solana/onchain/{data_type}/bind-a");

    ParquetSnapshotFlusher
        .flush_slot_window(
            &quiet_window(MIDNIGHT_US - 1_000_000, 2),
            template.to_str().unwrap(),
            &jupiter_window(None),
        )
        .unwrap();

    let binding = dir
        .path()
        .join("recorded/solana/onchain/transactions/bind-a");
    let ending =
        leaf_index::read_leaf_index(&binding.join(utc_day(MIDNIGHT_US))).unwrap();
    assert_eq!(ending.heartbeats.len(), 1);
    assert_eq!(ending.heartbeats[0].t_min_us, MIDNIGHT_US - 1_000_000);
    assert!(!binding.join(utc_day(MIDNIGHT_US - 1_000_000)).exists());
}

#[test]
fn a_slot_window_with_rows_writes_its_file_and_no_heartbeat() {
    let dir = tempdir().unwrap();
    let mut snapshots = quiet_window(WINDOW_START_US, 3);
    snapshots[1].transactions = vec![jupiter_transaction(
        380_000_000,
        "sig-1",
        WINDOW_START_US + 500_000,
    )];

    let report = ParquetSnapshotFlusher
        .flush_slot_window(
            &snapshots,
            dir.path().to_str().unwrap(),
            &jupiter_window(None),
        )
        .unwrap();

    assert_eq!(report.files_written, 1);
    let leaf = dir.path().join("transactions");
    let index = leaf_index::read_leaf_index(&leaf).unwrap();
    assert_eq!(
        (index.entries.len(), index.heartbeats.len(), index.torn),
        (1, 0, 0)
    );
    assert_eq!(index.entries[0].rows, 1);
    assert_eq!(
        std::fs::read_to_string(leaf.join(leaf_index::INDEX_FILE))
            .unwrap()
            .lines()
            .count(),
        1
    );
}

#[test]
fn cex_windows_write_their_files_and_never_a_heartbeat() {
    let dir = tempdir().unwrap();
    let template = dir.path().join("recorded/kraken/spot/{data_type}/bind-a");
    for output in [dir.path().to_path_buf(), template] {
        let output = output.to_str().unwrap();
        ParquetSnapshotFlusher
            .flush_snapshots(&[snapshot_with_trade(WINDOW_START_US)], output)
            .unwrap();
        let quiet = ParquetSnapshotFlusher
            .flush_snapshots(&quiet_window(WINDOW_START_US + 1_000_000, 2), output)
            .unwrap();
        assert_eq!((quiet.files_written, quiet.bytes_written), (0, 0));
    }

    for leaf in [
        dir.path().join("trades"),
        dir.path()
            .join("recorded/kraken/spot/trades/bind-a")
            .join(utc_day(WINDOW_START_US)),
    ] {
        let index = leaf_index::read_leaf_index(&leaf).unwrap();
        assert_eq!(
            (index.entries.len(), index.heartbeats.len(), index.torn),
            (1, 0, 0)
        );
    }
    assert!(!dir.path().join("transactions").exists());
    assert!(
        !dir.path()
            .join("recorded/kraken/spot/transactions")
            .exists()
    );
}

#[test]
fn heartbeat_lines_are_neither_files_nor_torn() {
    let dir = tempdir().unwrap();
    let output = dir.path().to_str().unwrap();
    let mut with_rows = quiet_window(WINDOW_START_US, 1);
    with_rows[0].transactions =
        vec![jupiter_transaction(380_000_000, "sig-1", WINDOW_START_US)];
    ParquetSnapshotFlusher
        .flush_slot_window(&with_rows, output, &jupiter_window(None))
        .unwrap();
    ParquetSnapshotFlusher
        .flush_slot_window(
            &quiet_window(WINDOW_START_US + 1_000_000, 1),
            output,
            &jupiter_window(None),
        )
        .unwrap();
    let leaf = dir.path().join("transactions");
    let index_path = leaf.join(leaf_index::INDEX_FILE);
    let mut lines = std::fs::read_to_string(&index_path).unwrap();
    lines.push_str(&format!(
        "{{\"heartbeat\":false,\"symbol\":\"{JUPITER}\",\"rows\":0,\"t_min_us\":0,\
         \"t_max_us\":0,\"last_slot\":null,\"last_block_height\":null,\
         \"schema_id\":\"transactions.v1\"}}\n\
         {{\"heartbeat\":true}}\n\
         {{\"filename\":\"listed.parquet\",\"sha256\":\"\",\"rows\":1,\"t_min_us\":0,\
         \"t_max_us\":0,\"schema_id\":\"transactions.v1\",\"heartbeat\":true}}\n\
         {{\"heartbeat\":true,\"symbol\":\"JUP"
    ));
    std::fs::write(&index_path, lines).unwrap();

    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert_eq!((entries.len(), torn), (1, 2));
    let index = leaf_index::read_leaf_index(&leaf).unwrap();
    assert_eq!(
        (index.entries.len(), index.heartbeats.len(), index.torn),
        (1, 3, 2)
    );
    assert_eq!(
        index.heartbeats[1],
        leaf_index::HeartbeatEntry {
            heartbeat: true,
            ..Default::default()
        }
    );
    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
}
