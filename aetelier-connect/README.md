# aetelier-connect

Exchange connectivity and reconstruction: WebSocket transports, a Yellowstone gRPC transport (feature `grpc`), per-venue protocol adapters, the order-book and trade-book reconstruction runtime, grid synchronizers, and the collection workers. This crate is where wire frames become verified market data.

## Venues

Thirteen venues run the same framework path: twelve spot venues — binance, bitget, bitso, bybit, coinbase, gateio, htx, kraken, kucoin, okx, poloniex, upbit — plus hyperliquid perpetuals (bare-coin symbols, USDC-margined, full-refresh snapshot book). Each is certified by the conformance suite in `tests/conformance/`, which replays real captured frames (committed under `datasets/`) through the production decode path on every CI run: decode surface, seeding taxonomy, symbol canonicalization, book delta application, book runtime replay, datatype isolation, gap detection, checksum validation, trade continuity, and physical book and trade invariants.

The `grpc` feature registers a fourteenth venue, `solana`: market type `onchain`, datatype `transactions`, environments `production` and `testnet`, one base58 address per worker, read from a Yellowstone gRPC endpoint (`yellowstone-grpc-proto@14.0.1`) through `framework::grpc`. It has no committed fixtures and no conformance suite.

```rust
use aetelier_connect::framework::registry::registry;

assert!(registry().get("binance").is_some());
assert!(registry().get("upbit").is_some());
assert_eq!(registry().get("solana").is_some(), cfg!(feature = "grpc"));
```

## Architecture

- `framework/` — the venue-generic engine: `transport` (WSS pump with staleness and RTT), `grpc` (feature `grpc`: Yellowstone subscribe requests, gRPC status classification, `SubscribeUpdate` decoding, `GrpcSession`, and `GrpcConnections`, which share one subscription per endpoint, commitment, token variable and shard), `adapters/` (13 exchange protocol implementations, plus `solana` with feature `grpc`), `model` (reconstruction models and book state machines), `runtime` (seed/reseed/escalation), `feed` (per-subscription FSM), `budget` (SourceMetrics counters), `reconcile` (REST trade recovery), `atlas` (invariant-ID registry backing the conformance coverage check), `slotbook` (per-bank transaction assembly with height-ordered release and slot-gap accounting), `onchain` (decoded transaction input, address roles and `transactions.v1` row building).
- `synchronizers/` — grid alignment: `MarketSynchronizer` (clock modes, timestamp-membership attribution, emission hold-back), `ObSynchronizer`, `EventSynchronizer`.
- `workers/` — `MarketWorker` (the recommended entry point: TOML manifest in, synchronized Parquet out), `DataWorker`, the sink layer (buffered Parquet, channels, terminal), and the gap ledger.
- `clients/` — reconnect policy (jittered backoff, circuit breaker, health monitoring) and the legacy per-venue clients, deprecated in favor of the framework path.
- `sources/` — the per-venue wire-type layer for the thirteen exchanges: decoders, event enums, and response structs, one folder per venue, imported by the framework adapters. The six original venues additionally carry a `client/` module — the deprecated pre-framework ingestion path, retained while the extended datatypes (funding, open interest, liquidations) complete their migration to the framework engine.

## Features

All off by default.

- `grpc` — the `solana` venue on `yellowstone-grpc-client` 17 and `yellowstone-grpc-proto` 14.0.1: `framework::grpc`, `framework::adapters::solana`, its registry entry, `MarketWorker::with_grpc_connections`, the `capture_grpc` example and the `geyser` tests against an in-memory mock Geyser server. Without it a valid `solana` worker is refused at admission with an error naming the feature.
- `parquet` — the Parquet examples `run_market_worker`, `read_market_worker` and `multi_sync_workers`; it gates no library code.
- `telemetry` — OpenTelemetry meters on the ingestion core (`IngestionCore::with_meters`).

## Running

Live quickstart, no credentials:

```text
cargo run -p aetelier-connect --example framework_live binance 8 BTCUSDT
```

The full example catalog — collection to Parquet, read-back statistics, multi-venue sync, wire capture — is in examples/README.md.

## Tests

Workspace tests plus the conformance suite (`tests/conformance.rs`) run in CI on every push; the conformance matrix hard-fails if any certified venue loses a passing kind.
