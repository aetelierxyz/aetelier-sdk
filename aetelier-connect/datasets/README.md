# aetelier datasets

Small, real captures of exchange and Solana wire data — the reproducible ground
truth the test suite replays. Every file here is bytes a venue actually sent
(public market or chain data, no credentials), captured over the venue's real
protocol, trimmed to a snapshot- or slot-anchored window, and committed so
anyone can reproduce the tests against the exact same input.

## Why these exist

Hand-authored JSON that *mimics* a venue's wire format drifts from reality and
can't catch a real protocol change. These captures let the decoder/normalizer
tests assert against genuine frames instead — the input is real, and the
expected values are pinned from the captured frame, so the oracle stays exact.
The venue conformance suite (`tests/conformance.rs`) and the per-venue parsing
tests (`tests/sources/`) both read from here.

## Provenance

Captured live via `examples/capture_fixture` (which drives each adapter's real
`ProtocolHooks` — endpoint, subscribe frames, heartbeat, frame codec — the same
path the runtime uses) for the WSS streams, and the venues' REST endpoints for
the seed snapshots. Symbol is BTC-quoted per venue (Upbit is KRW-BTC;
Hyperliquid is the bare perp coin `BTC`). Captured 2026-07 across the venue
conformance cycles; hyperliquid captured 2026-08-05 (cycle #5).

| Venue | File | Kind | Frames | Size |
|---|---|---|---:|---:|
| binance | `binance/btcusdt_depth_trade.jsonl` | WSS depth + trade | 1500 | 402K |
| binance | `binance/btcusdt_rest_snapshot.json` | REST depth snapshot | 1 | 312K |
| bitget | `bitget/btcusdt_book_trade.jsonl` | WSS book + trade | 700 | 303K |
| bitso | `bitso/btcmxn_book_trade.jsonl` | WSS L3 diff + trade | 915 | 197K |
| bitso | `bitso/btcmxn_rest_l3.json` | REST L3 snapshot (top-300/side) | 1 | 48K |
| bybit | `bybit/btcusdt_book_trade.jsonl` | WSS book + trade | 800 | 203K |
| coinbase | `coinbase/btcusd_book_trade.jsonl` | WSS book + trade | 201 | 895K |
| coinbase | `coinbase/btcusd_l2_heartbeats.jsonl` | WSS book-socket window: level2 + heartbeats + the 2 sequenced acks (captured 2026-07-16; snapshot depth-truncated to 150/side; `sequence_num` contiguous 0..799) | 800 | 705K |
| gateio | `gateio/btcusdt_book_trade.jsonl` | WSS book + trade | 400 | 379K |
| htx | `htx/btcusdt_book_trade.jsonl` | WSS book + trade (gzip, mid-stream REQ seed) | 231 | 127K |
| htx | `htx/btcusdt_trades_density.jsonl` | trade.detail frames only, selected from a live 600s window (2026-07-16; 50 trades, tradeId dense +1 after per-tick reverse — the loss-accounting density evidence) | 43 | 12K |
| hyperliquid | `hyperliquid/btc_book_trade.jsonl` | WSS full-book + trade + activeAssetCtx (600s window, captured 2026-08-05, stage-2; 112 l2Book snapshots all 20×20, 1458 trade frames / 4502 prints A:2560 B:1942, 587 activeAssetCtx ~1 Hz funding+OI, decimal-string values, no venue timestamp on ctx) | 2179 | 1.6M |
| kraken | `kraken/btcusd_book_trade.jsonl` | WSS book + trade | 1800 | 326K |
| kraken | `kraken/books_btcusd.jsonl` | WSS book (CRC32 reference) | 146 | 27K |
| kucoin | `kucoin/btcusdt_book_trade.jsonl` | WSS book + trade | 707 | 187K |
| kucoin | `kucoin/btcusdt_rest_l2.json` | REST L2 snapshot (mid-stream) | 1 | 4K |
| okx | `okx/btcusdt_book_trade.jsonl` | WSS book + trade | 600 | 278K |
| okx | `okx/books_btcusdt.jsonl` | WSS book (CRC32 reference) | 59 | 50K |
| poloniex | `poloniex/btcusdt_book_trade.jsonl` | WSS book + trade | 700 | 145K |
| upbit | `upbit/krwbtc_book_trade.jsonl` | WSS full-book + trade | 250 | 560K |

The `books_*` captures (kraken, okx) carry real per-frame CRC32 checksums and
bless the checksum algorithms in `src/framework/checksum.rs`; the rest are
combined book+trade windows used for reconstruction and normalization.

## Regenerating a capture

```
cargo run --example capture_fixture -- <venue> <wire_symbol> <seconds> <out.jsonl>
```

Coinbase runs split sockets in production: the `coinbase` venue arg captures
the book socket (level2 + heartbeats, the sequence-tracked one) and
`coinbase-trades` captures the market_trades socket.

Binance's parity capture (REST snapshot + a depth/trade window whose
`lastUpdateId` lands inside the delta stream) has a dedicated helper:
`datasets/binance/capture.sh`.

Seed-alignment note: for sequence-seeded venues the seed must land *inside* the
captured delta window, or offline reconstruction can't bridge to it. KuCoin's
REST `level2_100` snapshot is fetched a few seconds into the WSS capture; HTX's
in-band REQ is sent mid-stream by the capture bin (at t≈6s) for the same reason.
Bitso is L3 (order-keyed, seeded 0-based), so its REST snapshot is
alignment-immune.

Solana captures come from `examples/capture_grpc` (feature `grpc`), which
records raw Yellowstone gRPC `SubscribeUpdate` bytes through the production
subscribe request and reads the provider token from the variable named by
`--x-token-env` (default `GRPC_X_TOKEN`):

```
GRPC_X_TOKEN=<token> cargo run -p aetelier-connect --example capture_grpc --features grpc -- --endpoint https://solana-yellowstone-grpc.publicnode.com:443 --seconds 60 --out /tmp/solana_capture.jsonl JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4
```

The `solana/` files were captured on 2026-11-10 from
`https://solana-yellowstone-grpc.publicnode.com:443` in this way, one
subscription per file. Each request has one transaction filter (`vote: false`,
failed transactions kept, the watched address in `account_include`, or no
address for the unfiltered file), `blocks_meta`, an explicit commitment, and no
`from_slot`. Each file is a window of whole slots cut from a longer capture: it
keeps every transaction update and `blocks_meta` of its slots and drops pings
and pongs. Replay opens coverage two slots after the first input slot, so the
first two slots of each file emit no rows. In `unfiltered_finalized.jsonl` and
`jupiter_v6_paired_finalized.jsonl`, those two lead-in slots keep only their
`blocks_meta`. The longer windows they were cut from (600 s per address, 60 s
unfiltered, and a forced resubscribe) are kept outside the repository. No file
holds a token.

| File | Filter | Commitment | Slots | Tx / meta / reconnect | Size | sha256 |
|---|---|---|---|---:|---:|---|
| `solana/jupiter_v6_finalized.jsonl` | `JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4` (Jupiter Aggregator v6) | finalized | 467624000–467624013 | 58 / 14 / 0 | 964K | `c866a10b0521dd40d7bac82121d047c6d41b4499e35220052a08383ff3d4271f` |
| `solana/jupiter_v6_confirmed.jsonl` | Jupiter Aggregator v6 | confirmed | 467626900–467626919 | 65 / 20 / 0 | 908K | `765176f49da1a940b70037568132d97b4dbf0825dca7afe7823bf3f3f5b9b6d1` |
| `solana/raydium_amm_v4_finalized.jsonl` | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` (Raydium AMM v4 program) | finalized | 467624000–467624049 | 91 / 50 / 0 | 988K | `f83cf93e3dadf7becc248dc66947a63d131dc38e15370ab96bcaa2c10b72dc6c` |
| `solana/raydium_sol_usdc_pool_finalized.jsonl` | `58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2` (Raydium AMM v4 SOL/USDC pool) | finalized | 467624000–467624049 | 39 / 50 / 0 | 508K | `d40f283dd31678732a3983ac2888165e7d833b8ad1b1ca686283c99394062abf` |
| `solana/raydium_sol_usdc_pool_reconnect_finalized.jsonl` | Raydium AMM v4 SOL/USDC pool, resubscribed at tip once (`--reconnect-after 90`) | finalized | 467627245–467627285, slots 467627261–467627263 lost | 59 / 40 / 1 | 688K | `c11ce73aeb27fbc9e789ef4d598eece6e131ff18efb3de0c82de8b59a154f6bd` |
| `solana/config_program_quiet_finalized.jsonl` | `Config1111111111111111111111111111111111111` (quiet address) | finalized | 467624000–467624049 | 0 / 50 / 0 | 20K | `0aec67c552f0e14764aba3e04528ac78e0a8ca676d17f940f8a9476e22f69178` |
| `solana/unfiltered_finalized.jsonl` | none | finalized | 467626650–467626652 | 163 / 3 / 0 | 844K | `eb43d2494d36f6bf061a18a5678d9b85e2eb2bafd8ca63e9ebfd32a920260022` |
| `solana/jupiter_v6_paired_finalized.jsonl` | Jupiter Aggregator v6, the same slots as the unfiltered file | finalized | 467626650–467626652 | 10 / 3 / 0 | 152K | `751d46869a48e1a4c252c467ccc7175be9cf3f70a9e3fea1c9c116a1e06f0afe` |

In the longer captures:
- Every filtered stream held exactly the transactions of the unfiltered stream
  over the same slots that name its address, including addresses that appear
  only through address lookup tables.
- The unfiltered stream matched `executed_transaction_count` on every inner
  slot.
- No vote transaction appeared with the vote filter off.
- Banks arrived in slot order at both commitments, with no child bank before
  its parent.
- `fee_lamports` equals 5000 lamports per signature plus the priority fee.
  The priority fee is `priority_fee_lamports` on v1, and on legacy and v0 the
  ceiling of price × limit / 10⁶. A legacy or v0 transaction without a limit
  instruction pays on the runtime's default limit, and precompile signatures
  count as signatures.

