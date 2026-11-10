//! Data-plane venue conformance harness (consolidated `[[test]]` binary).
//!
//! Each venue has a `VenueConformance` descriptor; `conformance_suite!`
//! instantiates every kind in `kinds::KINDS` as a named `#[test]` for it, so
//! the kinds matrix is the test surface. A new kind ratchets to every venue at
//! once (add it to `KINDS` and to the macro body); a venue missing a required
//! fixture fails its kind.
//!
//! Venue cycle status: all 13 venues wired and fully DONE — every venue's row is
//! green across all applicable kinds, which the `conformance_matrix` meta-test
//! enforces for every `done: true` venue (no Skip, no Fail). Hyperliquid's
//! stage-1 declared surface is Book + Trade; funding/open-interest extend it in
//! a later cycle and reopen the venue via the ratchet.

#[path = "conformance/coverage.rs"]
mod coverage;
#[path = "conformance/harness.rs"]
mod harness;
#[path = "conformance/kinds.rs"]
mod kinds;
#[cfg(feature = "grpc")]
#[path = "conformance/stream.rs"]
mod stream;

#[cfg(feature = "grpc")]
use aetelier_types::onchain::{Chain, Commitment};
use harness::{Class, ExpectInstrument, ExpectSource, VenueConformance};
#[cfg(feature = "grpc")]
use harness::{ExpectGap, StreamFixture};

// ── Venue descriptors (fixtures + doc-derived expectations only) ────────────

const BINANCE: VenueConformance = VenueConformance {
    venue: "binance",
    wire_symbol: "BTCUSDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    // Binance depth is a REST-seeded incremental stream (docs: fetch the depth
    // snapshot, then apply @depth diffs whose U/u straddle lastUpdateId).
    expect_source: ExpectSource::Rest,
    expect_needs_rest: true,
    ws_fixture: "binance/btcusdt_depth_trade.jsonl",
    rest_fixture: Some("binance/btcusdt_rest_snapshot.json"),
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

// OKX and Kraken: ChecksumDelta venues (CRC32 continuity, no REST seed,
// self-seeded from the first snapshot frame). Cycle #2 certified the book +
// checksum surface; the trade channel was later captured live (combined
// book+trade fixtures) and their monotonic trade ids armed, flipping both to
// fully DONE.
const OKX: VenueConformance = VenueConformance {
    venue: "okx",
    wire_symbol: "BTC-USDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "okx/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

const KRAKEN: VenueConformance = VenueConformance {
    venue: "kraken",
    wire_symbol: "BTC/USD",
    canonical_base: "BTC",
    canonical_quote: "USD",
    expect_source: ExpectSource::None,
    expect_needs_rest: false,
    ws_fixture: "kraken/btcusd_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

// Cycle #3: Coinbase, Bybit, Gate.io — self-seed venues with book + trade
// channels captured live (examples/capture_fixture). First fully-DONE venues
// with real trade coverage since Binance.
const COINBASE: VenueConformance = VenueConformance {
    venue: "coinbase",
    wire_symbol: "BTC-USD",
    canonical_base: "BTC",
    canonical_quote: "USD",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "coinbase/btcusd_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

const BYBIT: VenueConformance = VenueConformance {
    venue: "bybit",
    wire_symbol: "BTCUSDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "bybit/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

const GATEIO: VenueConformance = VenueConformance {
    venue: "gateio",
    wire_symbol: "BTC_USDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::None,
    expect_needs_rest: false,
    ws_fixture: "gateio/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

// Cycle #4 (self-seed subset): bitget, poloniex (SeqDelta ExactPrev), upbit
// (FullRefresh), htx (SeqDelta ExactPrev, in-band ReqOnSocket seed — the REQ
// reply is captured in the fixture). All captured live via capture_fixture.
const BITGET: VenueConformance = VenueConformance {
    venue: "bitget",
    wire_symbol: "BTCUSDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "bitget/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};
const POLONIEX: VenueConformance = VenueConformance {
    venue: "poloniex",
    wire_symbol: "BTC_USDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "poloniex/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};
const UPBIT: VenueConformance = VenueConformance {
    venue: "upbit",
    wire_symbol: "KRW-BTC",
    canonical_base: "BTC",
    canonical_quote: "KRW",
    expect_source: ExpectSource::None,
    expect_needs_rest: false,
    ws_fixture: "upbit/krwbtc_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};
const HTX: VenueConformance = VenueConformance {
    venue: "htx",
    wire_symbol: "btcusdt",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::ReqOnSocket,
    expect_needs_rest: false,
    ws_fixture: "htx/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    // The in-band REQ reply is captured mid-stream (fired at t=6s), so it aligns
    // with the delta stream and the book reconstructs via the kind's
    // buffer-and-reconcile path.
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

// Cycle #4 (REST/L3-seeded): bitso (L3 diff-orders, seeded 0-based from a REST
// L3 snapshot) and kucoin (SeqDelta RangeInclusive, REST level2 seed). Both
// use replay_seed against a committed REST snapshot fixture.
const BITSO: VenueConformance = VenueConformance {
    venue: "bitso",
    // btc_mxn is Bitso's flagship (Mexican) market — it actually trades, where
    // btc_usdt was too quiet to certify the trade channel.
    wire_symbol: "btc_mxn",
    canonical_base: "BTC",
    canonical_quote: "MXN",
    expect_source: ExpectSource::Rest,
    expect_needs_rest: true,
    ws_fixture: "bitso/btcmxn_book_trade.jsonl",
    rest_fixture: Some("bitso/btcmxn_rest_l3.json"),
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};
// KuCoin: RangeInclusive REST-seed. The seed (level2 snapshot) is captured
// mid-stream so its sequence lands inside the buffered delta window — the
// straddling delta chains from it and reconstruction proceeds.
const KUCOIN: VenueConformance = VenueConformance {
    venue: "kucoin",
    wire_symbol: "BTC-USDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::Rest,
    expect_needs_rest: true,
    ws_fixture: "kucoin/btcusdt_book_trade.jsonl",
    rest_fixture: Some("kucoin/btcusdt_rest_l2.json"),
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

const HYPERLIQUID: VenueConformance = VenueConformance {
    venue: "hyperliquid",
    wire_symbol: "BTC",
    canonical_base: "BTC",
    canonical_quote: "USDC",
    expect_source: ExpectSource::None,
    expect_needs_rest: false,
    ws_fixture: "hyperliquid/btc_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[
        Class::Book,
        Class::Trade,
        Class::FundingRate,
        Class::OpenInterest,
    ],
    done: true,
    datatype_isolation_done: true,
    expect_instrument: ExpectInstrument::Pair,
    stream_fixtures: &[],
};

#[cfg(feature = "grpc")]
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
#[cfg(feature = "grpc")]
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
#[cfg(feature = "grpc")]
const RAYDIUM_SOL_USDC_POOL: &str = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2";
#[cfg(feature = "grpc")]
const CONFIG_PROGRAM: &str = "Config1111111111111111111111111111111111111";

#[cfg(feature = "grpc")]
const SOLANA: VenueConformance = VenueConformance {
    venue: "solana",
    wire_symbol: JUPITER_V6,
    canonical_base: "",
    canonical_quote: "",
    expect_source: ExpectSource::None,
    expect_needs_rest: false,
    ws_fixture: "",
    rest_fixture: None,
    expect_classes: &[Class::Transaction],
    done: true,
    datatype_isolation_done: false,
    expect_instrument: ExpectInstrument::Address(Chain::Solana),
    stream_fixtures: &[
        StreamFixture {
            file: "solana/jupiter_v6_finalized.jsonl",
            watched: JUPITER_V6,
            commitment: Commitment::Finalized,
            unfiltered: None,
            rows: 52,
            ticks: 12,
            gaps: &[],
        },
        StreamFixture {
            file: "solana/jupiter_v6_confirmed.jsonl",
            watched: JUPITER_V6,
            commitment: Commitment::Confirmed,
            unfiltered: None,
            rows: 59,
            ticks: 18,
            gaps: &[],
        },
        StreamFixture {
            file: "solana/raydium_amm_v4_finalized.jsonl",
            watched: RAYDIUM_AMM_V4,
            commitment: Commitment::Finalized,
            unfiltered: None,
            rows: 89,
            ticks: 48,
            gaps: &[],
        },
        StreamFixture {
            file: "solana/raydium_sol_usdc_pool_finalized.jsonl",
            watched: RAYDIUM_SOL_USDC_POOL,
            commitment: Commitment::Finalized,
            unfiltered: None,
            rows: 38,
            ticks: 48,
            gaps: &[],
        },
        StreamFixture {
            file: "solana/raydium_sol_usdc_pool_reconnect_finalized.jsonl",
            watched: RAYDIUM_SOL_USDC_POOL,
            commitment: Commitment::Finalized,
            unfiltered: None,
            rows: 51,
            ticks: 36,
            gaps: &[ExpectGap {
                prev_slot: 467_627_260,
                prev_height: 445_644_849,
                next_slot: 467_627_264,
                next_height: 445_644_853,
            }],
        },
        StreamFixture {
            file: "solana/config_program_quiet_finalized.jsonl",
            watched: CONFIG_PROGRAM,
            commitment: Commitment::Finalized,
            unfiltered: None,
            rows: 0,
            ticks: 48,
            gaps: &[],
        },
        StreamFixture {
            file: "solana/unfiltered_finalized.jsonl",
            watched: JUPITER_V6,
            commitment: Commitment::Finalized,
            unfiltered: None,
            rows: 10,
            ticks: 1,
            gaps: &[],
        },
        StreamFixture {
            file: "solana/jupiter_v6_paired_finalized.jsonl",
            watched: JUPITER_V6,
            commitment: Commitment::Finalized,
            unfiltered: Some("solana/unfiltered_finalized.jsonl"),
            rows: 10,
            ticks: 1,
            gaps: &[],
        },
    ],
};

/// Instantiate every kind in `kinds::KINDS` as a named `#[test]` for one
/// venue. The body lists each kind once; adding a kind means adding a line
/// here and its entry in `KINDS` — it then applies to every venue.
macro_rules! conformance_suite {
    ($mod:ident, $desc:expr) => {
        mod $mod {
            use super::*;

            fn kind(name: &str) -> &'static harness::Kind {
                kinds::KINDS
                    .iter()
                    .find(|k| k.name == name)
                    .expect("kind registered")
            }

            #[test]
            fn decode_surface() {
                harness::assert_kind(&$desc, kind("decode_surface"));
            }
            #[test]
            fn seeding_taxonomy() {
                harness::assert_kind(&$desc, kind("seeding_taxonomy"));
            }
            #[test]
            fn symbol_canonicalization() {
                harness::assert_kind(&$desc, kind("symbol_canonicalization"));
            }
            #[test]
            fn trade_continuity() {
                harness::assert_kind(&$desc, kind("trade_continuity"));
            }
            #[test]
            fn trade_invariants() {
                harness::assert_kind(&$desc, kind("trade_invariants"));
            }
            #[test]
            fn book_delta_application() {
                harness::assert_kind(&$desc, kind("book_delta_application"));
            }
            #[test]
            fn book_runtime_replay() {
                harness::assert_kind(&$desc, kind("book_runtime_replay"));
            }
            #[test]
            fn datatype_isolation() {
                harness::assert_kind(&$desc, kind("datatype_isolation"));
            }
            #[test]
            fn book_invariants() {
                harness::assert_kind(&$desc, kind("book_invariants"));
            }
            #[test]
            fn gap_detection() {
                harness::assert_kind(&$desc, kind("gap_detection"));
            }
            #[test]
            fn checksum_validation() {
                harness::assert_kind(&$desc, kind("checksum_validation"));
            }
            #[test]
            fn derivative_invariants() {
                harness::assert_kind(&$desc, kind("derivative_invariants"));
            }
            #[test]
            fn feed_lifecycle() {
                harness::assert_kind(&$desc, kind("feed_lifecycle"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn tx_decode_surface() {
                harness::assert_kind(&$desc, kind("tx_decode_surface"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn slot_assembly() {
                harness::assert_kind(&$desc, kind("slot_assembly"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn slot_reorder() {
                harness::assert_kind(&$desc, kind("slot_reorder"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn tx_dedup() {
                harness::assert_kind(&$desc, kind("tx_dedup"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn slot_gap_detection() {
                harness::assert_kind(&$desc, kind("slot_gap_detection"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn slot_tick_grid() {
                harness::assert_kind(&$desc, kind("slot_tick_grid"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn shared_connection_fanout() {
                harness::assert_kind(&$desc, kind("shared_connection_fanout"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn address_match_roles() {
                harness::assert_kind(&$desc, kind("address_match_roles"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn failed_inclusion() {
                harness::assert_kind(&$desc, kind("failed_inclusion"));
            }
            #[cfg(feature = "grpc")]
            #[test]
            fn tx_invariants() {
                harness::assert_kind(&$desc, kind("tx_invariants"));
            }
        }
    };
}

conformance_suite!(binance, BINANCE);
conformance_suite!(okx, OKX);
conformance_suite!(kraken, KRAKEN);
conformance_suite!(coinbase, COINBASE);
conformance_suite!(bybit, BYBIT);
conformance_suite!(gateio, GATEIO);
conformance_suite!(bitget, BITGET);
conformance_suite!(poloniex, POLONIEX);
conformance_suite!(upbit, UPBIT);
conformance_suite!(htx, HTX);
conformance_suite!(bitso, BITSO);
conformance_suite!(kucoin, KUCOIN);
conformance_suite!(hyperliquid, HYPERLIQUID);
#[cfg(feature = "grpc")]
conformance_suite!(solana, SOLANA);

// ── Registry of wired venues, for the matrix meta-test ──────────────────────

const WIRED: &[&VenueConformance] = &[
    &BINANCE,
    &OKX,
    &KRAKEN,
    &COINBASE,
    &BYBIT,
    &GATEIO,
    &BITGET,
    &POLONIEX,
    &UPBIT,
    &HTX,
    &BITSO,
    &KUCOIN,
    &HYPERLIQUID,
    #[cfg(feature = "grpc")]
    &SOLANA,
];

/// Meta-test: emit the kinds x venues coverage matrix and enforce the ratchet
/// invariants — the macro body must instantiate exactly `KINDS`, and every
/// DONE venue's full row must be green (no Skip, no Fail).
#[test]
fn conformance_matrix() {
    let kind_names: Vec<&str> = kinds::KINDS.iter().map(|k| k.name).collect();

    println!("\n=== data-plane conformance matrix ===");
    println!("kinds ({}):", kind_names.len());
    for k in kinds::KINDS {
        println!("  {:<24} -> {}", k.name, k.atlas);
    }
    let mut certified = std::collections::HashSet::new();
    for desc in WIRED {
        let outcomes: Vec<harness::Outcome> =
            kinds::KINDS.iter().map(|k| (k.run)(desc)).collect();
        let mut row = Vec::new();
        for (k, outcome) in kinds::KINDS.iter().zip(&outcomes) {
            let cell = match outcome {
                harness::Outcome::Pass => "PASS",
                harness::Outcome::NotApplicable(_) => "n/a",
                harness::Outcome::Skip(_) => "skip",
                harness::Outcome::Fail(_) => "FAIL",
            };
            if matches!(outcome, harness::Outcome::Pass) {
                certified.insert(k.name);
            }
            row.push(format!("{}={}", k.name, cell));
        }
        println!("{:<10} done={} | {}", desc.venue, desc.done, row.join("  "));

        if desc.done {
            for (k, outcome) in kinds::KINDS.iter().zip(&outcomes) {
                match outcome {
                    // Pass and NotApplicable are both green for a DONE venue;
                    // NotApplicable is a by-design non-gap, not a coverage hole.
                    harness::Outcome::Pass | harness::Outcome::NotApplicable(_) => {}
                    other => panic!(
                        "DONE venue {} has non-green kind {}: {:?}",
                        desc.venue, k.name, other
                    ),
                }
            }
        }
    }
    println!("=====================================\n");
    for k in kinds::KINDS.iter().filter(|k| !k.invariants.is_empty()) {
        assert!(
            certified.contains(k.name),
            "kind {} certifies {:?} but passes on no wired venue",
            k.name,
            k.invariants
        );
    }
}
