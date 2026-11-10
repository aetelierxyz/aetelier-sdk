# Changelog

Every promotion into `main` adds one line under `Unreleased` → `Promotions`:
date, source branch, effect. Released version entries are frozen; a released
entry is edited only to correct a factual error, never to restate its scope.

## Unreleased

### Promotions

### Added

- `aetelier_types::onchain`: `Chain`, `ChainAddress` (case-preserving base58, validated to 32 bytes), `Instrument`, and the `Transaction` record with `MatchRole`, `Commitment`, `TransactionVersion` (`legacy`, `v0`, `v1`) and `TransactionFailure`. `DeclaredDatatype::Transactions`, the `[datatypes.transactions]` feed (`enabled`, `match`, `include_failed`, `logs`) and `MarketSnapshot.transactions`.
- `aetelier_io::onchain`: the `transactions.v1` Parquet writer and reader (file tag `txs`, leaf `transactions`), its schema fingerprint, and `check_transactions`.
- `validate` T11–T13: unique `(address, signature)` per file, `(slot, tx_index)` order per address, block context and row invariants. T01 expects `transactions` for `solana` and `orderbooks` + `trades` elsewhere.
- `Exchange::Solana` (`solana`, no alias) and `MarketType::Onchain` (`onchain`); `Exchange::chain()` names a venue's chain. `Instrument` serializes as `BASE/QUOTE` or bare base58 and parses by the slash; `Instrument::from_exchange_symbol` decodes per venue.
- `SymbolCodec::ChainAddress` and `SymbolCodec::decode_instrument`; `FeedDatatype::Transactions` with `FeedDatatype::ALL` and `FeedDatatype::for_declared`.
- `SyncMode::OnSlot` (`on_slot`) and `ClockMode::SlotDriven` (label `ON_SLOT`); `SyncMode::clock_mode`, `WorkerStatus::initial` and `IngestionCore::exchange`.
- `transport = "grpc"` with a strict `[collect.grpc]` section (`GrpcSection`: endpoint, token env, commitment, filter and size limits).
- Admission: `ExchangeAdapter::supported_market_types`, `ExchangeAdapter::default_emission_delay_us`, `admit_market_type`, `admit_datatypes`, `admit_registered`, `MarketWorkerConfig::admit` and `MarketWorkerManifest::resolve_admitted`.
- `FeedSet::for_declared`.
- `SourceMetricsSnapshot` counters `filtered_out`, `slot_gaps`, `blocks_lost`, `late_txs`, `partial_banks_discarded`, `abandoned_banks`, `fanout_overflow`, `votes_excluded`, `tx_error_decode_failures` and the gauge `behind_tip_slots`, zero on CEX venues.
- Gap ledger cause `slot_gap` with `prev_slot`, `prev_height`, `next_slot`, `next_height` and `blocks_lost`, omitted from lines of other causes.
- Conformance kind `feed_lifecycle` on every venue.
- `aetelier_connect::framework::slotbook`: `SourcedSlotbook` assembles transactions per `(slot, bank_id)`, seals each bank on its block meta, releases banks in block-height order with a bounded reorder hold, opens coverage after each subscribe, and reports `SlotGap`, `SlotTick` and loss counters.
- `aetelier_connect::framework::onchain`: decoded transaction input, address roles, the watched-address predicate, requested compute, `failure_*` derivation and `transaction_rows` for `transactions.v1`.
- `aetelier_types::onchain::encode_base58`.
- `ReconstructionModel::Stream` (book level `none`; no books, no seeding).
- `DomainEvent` and `ReconstructedEvent` `Transaction`, `SlotTick`, `SlotGap`, `ConnectionLost`, `ConnectionResumed`; a slot gap is counted (`slot_gaps`, `blocks_lost`) and written to the gap ledger, never resynced.
- `MarketSynchronizer::slot_driven`, `on_transaction` and `on_slot_tick`: a chain-time grid whose emission hold-back is measured in chain time.
- `GapIncident::from_slot_gap`, `ExchangeAdapter::replay_stream` and `ReplayFrame`.
- `aetelier_io::onchain::read_transaction_keys` and `LeafKeys`.
- Cargo feature `grpc` in `aetelier-connect` and `aetelier-sdk`, off by default: `yellowstone-grpc-client` 17 and `yellowstone-grpc-proto` 14.0.1, with `TransactionError` decoded through `wincode`.
- `framework::grpc`: the full Yellowstone subscribe request (one transaction filter over every watched address, `blocks_meta`, explicit commitment, never `from_slot`) and ping-only requests, checked against `account_include_max` and `account_include_reject`; `classify_status` with retryable, rate-limited, terminal and rejected-update outcomes, terminal statuses staying terminal after data; `SubscribeUpdate` decoding into the on-chain transaction input and slot-book block meta, keeping undecodable error bytes.

### Changed

- `WorkerStatus.pair` is an `Instrument`; CEX status JSON is unchanged.
- `md_worker` admits every worker before starting any.
- `MarketWorker` refuses `transport = "grpc"`, `sync_mode = "on_slot"`, `market_type = "onchain"` and `[collect.datatypes.transactions]` on CEX venues. A `solana` worker needs `transport = "grpc"`, a valid `[collect.grpc]`, a base58 address, `sync_mode = "on_slot"`, no enabled `[collect.reconcile]` and a declared datatype, and fails naming the `grpc` feature while no solana adapter is built in.
- `DataWorker` refuses chain venues, `market_type = "onchain"` and `[collect.datatypes.transactions]`.
- `[collect.datatypes.transactions]` rejects unknown keys; a manifest with a chain venue rejects duplicate addresses and unread keys under `[collect.sync]` and `[[workers]]`.
- `Feed`, `FeedSet` and `SourceRuntime` liveness are keyed by `Instrument`; `Feed::new` takes any `Into<Instrument>`; feed snapshots keep the `BASE/QUOTE` string.
- `FeedSet::mark_all_reconnecting` and `mark_all_subscribing` leave feeds already in the target state unchanged.
- `validate` walks `<exchange>/<market_type>/<datatype>/<binding_id>/<YYYY-MM-DD>/` leaves as well as `<exchange>/<datatype>/`, takes a leaf's files from its `index.jsonl` when present, and counts pending files and torn index lines. T11 checks `(address, signature)` across the files of a leaf day.
- CI runs clippy and nextest with `aetelier-sdk/parquet,aetelier-sdk/grpc` in the gate and portability jobs, audits that dependency graph with `cargo deny`, and runs `cargo check --all-features`.

### Fixed

- Workers flushing into the same leaf in one process could delete each other's staged Parquet file in the `.staging` sweep, failing the flush with `ENOENT` and forcing a retry. Flushes into one leaf are now serialized in-process.
- A file left unlisted by a crashed flush is reported as pending instead of validated, so it no longer overlaps its retried sibling in T10.

## v0.1.1 — 2026-10-06

### Promotions

- 2026-10-06 · `fix/validate-filename-and-span-units` (#26) — grid-stamped book rows; microsecond file stamps; `validate` aligned with the writers.
- 2026-10-06 · `chore/rust-1.99-lints` (#27) — Rust 1.99.0 toolchain pin; async-trait 0.1.92; `aetelier-connect` `md_worker` stub removed.
- 2026-08-22 · `staging` (#14) — staging promoted to `main`.
- 2026-08-22 · `chore/pr-template` (#13) — pull-request template gains evidence sections.
- 2026-08-22 · `fix/storage-type-correctness` (#12) — decode bounds, archive client deadlines, delta column declaration order.
- 2026-08-19 · `feat/entrepot-transport` (#11) — `aetelier-entrepot` object-store transport crate, Hyperliquid S3 archive replay, `entrepot` binary, bounded backfill retries, decode-panic containment.
- 2026-08-14 · `chore/hyperliquid-monitoring-tune` (#9) — fleet heartbeat, settlement window contract, arrival-clock settlement drain.
- 2026-08-10 · `hotfix/pin-actions-scrub-comments` (#8) — release workflow actions pinned to commit SHAs.
- 2026-08-10 · hyperliquid WSS (#7) — hyperliquid registered as the thirteenth framework adapter (perpetual, FullRefresh); funding and open-interest settlement path; stage-2 conformance.
- 2026-08-05 · `feature/delta-before-snapshot-guards` — out-of-band seeding taxonomy, in-band buffering exit, seed deadline.

### Fixed

- Delta orderbook Parquet declared its `side` and `exchange` columns in the opposite order to the values it wrote, so every consumer resolving by column name read exchange strings as sides and sides as exchanges. The declaration now matches the data, the sibling snapshot writer, and both readers. Readers that resolve positionally — including this crate's own — are unaffected in either direction, since no byte position moved; positional tolerance cannot express a rename, so a file written before this fix is identified by its embedded schema, where `side` precedes `exchange`.
- `validate` T03 rejected every writer file; T05 rejected every full book file.
- Funding-settlement file names replace `:` like the other writers.

### Added

- Decode bounds on untrusted input: a gzip WebSocket frame that inflates past 64 MiB and an archive object that decompresses past 256 MiB are refused rather than allocated, so a compression bomb cannot exhaust a long-running collector.
- Deadlines on the archive HTTP client: a 15 s connect bound so a black-holed endpoint surfaces instead of parking, and a 30 s idle-read bound that fires only when no bytes arrive. No total request timeout is set, so multi-gigabyte object downloads are never killed mid-transfer.
- `aetelier_io::naming` (`file_name`, `parse_file_name`, `FileKind`) and `Orderbook::effective_ts_us`.

### Changed

- A failed response-body read on the archive path is now classified and retried by the same transport policy as a failed send, instead of terminating the run; the two paths share one retry budget.
- Builds use Rust 1.99.0 via `rust-toolchain.toml`; MSRV stays 1.88.0.
- `aetelier-connect` no longer ships an `md_worker` binary; run `aetelier-sdk`'s.
- Parquet book rows carry the grid tick in `orderbook_ts_us` (never earlier than the book); venue time stays in `source_orderbook_ts_us`.
- Per-symbol writer file stamps are the earliest row time in µs (v0.1.0: write time in ms); `parse_file_name` reads both.
- `validate` takes `--grid-period-us`; `serve/run_validate.sh` reads `AETELIER_GRID_PERIOD_US`; report field `span_ms` is `span_us`.

## v0.1.0 — 2026-07-30

The first public release: the public front door of the aetelier market-data engine. Five crates — types, connect, io, telemetry, and the sdk facade — with 12 conformance-certified venues, exact loss accounting, and columnar Parquet persistence.

### Added

- Venue conformance harness: every venue is certified by replaying its own captured wire frames through the production decode path, across a ratcheting matrix of test kinds — decode surface, seeding taxonomy, symbol canonicalization, book delta application, sequence-gap detection (a dropped delta must gap the book, proven per venue), checksum validation, trade continuity, and independent physical oracles (books never crossed, strictly positive, ordered; trades positive with microsecond-scale timestamps). The suite runs in CI on every push; a certified venue losing a passing kind fails the build.
- Data-completeness instrumentation in every collector: exact trade-loss counters where venue sequences are dense (nine venues, including across reconnects), labeled estimates where they are not; book gap incidents and windows; a crash-safe JSONL gap ledger beside the Parquet output; connection-level gap oracles for Coinbase (split sockets, heartbeat-attributed sequence tracking) and Bitso (dense per-book envelope sequence).
- Live reconciliation: REST-recovered trades merge into their still-buffered grid rows within a configurable emission hold-back window, deduplicated and stamped with `origin: rest`; recovery never rewrites loss accounting.
- Batch rehydration: a `rehydrate` engine and binary that set-differences persisted trade parquets against the venue's REST history by id, writes a dense `*_rehydrated` file beside the immutable originals, and reports honestly what retention makes unrecoverable.
- Trade provenance: `Trade.origin` (`ws` | `rest`) in memory and as a Parquet column appended last; readers tolerate files written before the column existed.
- Grid synchronization guarantees: timestamp-membership attribution (every event lands in the row that closes its own period — no look-ahead leakage, no backlog dumping), as-of-boundary books with true timestamps, idempotent `finalize()` with a real terminal state, and counted late-event drops.
- A snapshot broadcast channel: the synchronized stream is subscribable in-process (`MarketWorker::snapshot_channel`), never blocking the collector, with receiver-side lag quantified exactly.
- The curated facade: `aetelier-sdk` re-exports the recommended path by name — canonical types, `MarketWorker`, Parquet persistence, telemetry, and the unified `AetelierError` — with whole-crate re-exports for power users.
- Tier-0 CI: format, clippy with denied warnings, workspace nextest on default and parquet legs, doc tests, rustdoc with denied warnings, per-feature check via cargo hack, cargo deny, an MSRV job at 1.88, and a macOS leg.
- First-class datasets: the captured fixture corpus ships in-repo with a provenance manifest, powering both the conformance suite and offline replay.

### Changed

- Trades and liquidations carry exact `rust_decimal::Decimal` amounts and prices in memory; the JSON wire format (floats) and the Parquet physical type (Float64) are unchanged.
- Timestamps are UTC epoch microseconds everywhere, under `_us` field and column names; venue wire units convert exactly once at the adapter boundary.
- The WebSocket transport runs tokio-tungstenite 0.30 on a single rustls 0.23 line.
- The error taxonomy is fully typed: no stringly-typed results in public signatures, no anyhow in library targets of the IO and telemetry crates, venue transport errors wrapped opaquely.
- Ingestion is framework-first: every shipped configuration runs the 12-venue framework engine; the legacy per-venue clients are deprecated with forwarding re-exports.
- Seeding derives from each adapter's declared reconstruction model — a single source of truth replaced string-matched venue special-casing.
- The private platform wire contract and the remote agent moved out of the workspace into their own repository; this repo builds from a bare clone with no private dependencies.
- Every venue's wire layer lives in `sources/<venue>/` — one folder per venue for all twelve: the six later venues' decoders, event enums, and response structs moved out of their adapter files into the same layout the original six always had; adapters are uniform composition files importing from `sources/`.

### Fixed

- Reconnect backoff resets on recovery (`on_connected`/`on_message_received` are wired into the framework loop), the circuit breaker is reachable, and all three reconnect mechanisms carry test coverage with an injected clock.
- Parquet readers reject malformed input with typed errors: an unknown trade side or malformed symbol is an error, never a panic and never a silently fabricated value.
- A misconfigured Parquet output (declared sink, no flusher) is a startup error instead of a silent data drop; flush failures retain the buffer for retry on every path.
- Adapters drop and count trades with unrecognized sides instead of coercing them.
- OKX book reconstruction migrated off the venue's deprecated checksum to sequence continuity — caught live by the conformance capture before it could gap-loop production.
- A live Bitso decode defect (trade timestamps read from the wrong wire field) was caught by the first independent-oracle conformance run and fixed the same day.

### Documented

- The README layer is the front door: root README with a credential-free live quickstart and real captured output, five real per-crate READMEs whose code blocks compile as doc tests in CI, an indexed example catalog with exact commands, and CONTRIBUTING with the true CI gate and the crates.io publish order.
