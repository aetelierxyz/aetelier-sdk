//! Adapter registry: the single explicit list of compiled-in venue adapters,
//! keyed by venue id and resolved at boot.

use std::collections::HashMap;
use std::sync::OnceLock;

use super::budget::{ConnectionBudget, SourceMetrics};
use super::model::{DomainEvent, ReconstructionModel};
use super::symbol::SymbolCodec;
use crate::clients::disconnect::DisconnectReason;

/// Static, data-only description of a venue, versioned via `schema_version` /
/// `protocol_revision`.
#[derive(Debug, Clone)]
pub struct ExchangeProfile {
    pub id: &'static str,
    pub symbol_codec: SymbolCodec,
    pub budget: ConnectionBudget,
    pub schema_version: u32,
    /// e.g. `"okx-v5"`, `"kraken-v2"` — pinned so a wire bump is caught at boot.
    pub protocol_revision: &'static str,
}

/// Terminal outcome of an Ingest Task run, used by the worker driving this
/// adapter to advance the task to its terminal state. Encodes graceful
/// Stop/Drain.
#[derive(Debug)]
pub enum TaskExit {
    /// Natural end, or a clean Stop fully drained within
    /// [`STOP_DRAIN_TIMEOUT`](crate::framework::driver::STOP_DRAIN_TIMEOUT).
    Completed,
    /// Stop requested but drain exceeded
    /// [`STOP_DRAIN_TIMEOUT`](crate::framework::driver::STOP_DRAIN_TIMEOUT);
    /// outstanding artifacts are marked `partial`.
    DrainTimedOut,
    /// Runtime failure beyond the retry budget; carries the transport cause for
    /// telemetry.
    Failed(DisconnectReason),
    /// A finite source delivered everything it has. Terminal success: the
    /// worker drains, flushes, and exits — never reconnects. Only finite
    /// adapters (object-store replay) produce this; `drive()` never does.
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayFrame {
    Update { t_us: u64, bytes: Vec<u8> },
    Reconnect { t_us: u64 },
}

/// A venue's full integration behind the registry. Object-safe.
pub trait ExchangeAdapter: Send + Sync + 'static {
    /// Stable venue id (`"binance"`, `"okx"`, …).
    fn id(&self) -> &'static str;

    /// Data-only profile (codec, budget, version).
    fn profile(&self) -> &ExchangeProfile;

    /// The venue's REST snapshot seeder, when its reconstruction model
    /// seeds from REST (`model.needs_rest()`). The adapter is the single
    /// source of truth: declaring a REST-seeded model while returning
    /// `None` here is a construction error the worker rejects loudly.
    fn rest_seeder(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::framework::rest::RestSnapshot>> {
        None
    }

    /// Reconstruction-model for a given channel — keyed per channel, not per
    /// venue (a venue may run FullRefresh on one channel and SeqDelta on another).
    fn book_model(&self, channel: &str) -> ReconstructionModel;

    fn supported_datatypes(
        &self,
    ) -> &'static [aetelier_types::config::markets::market_config::DeclaredDatatype] {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        &[DD::Orderbook, DD::Trades]
    }

    fn supported_market_types(&self) -> &'static [aetelier_types::exchanges::MarketType] {
        use aetelier_types::exchanges::MarketType as MT;
        &[MT::Spot, MT::Perpetual, MT::Inverse]
    }

    fn default_emission_delay_us(&self) -> Option<u64> {
        None
    }

    fn resubscribe_replays_trades(&self) -> bool {
        false
    }

    fn max_declared_depth(&self) -> Option<usize> {
        None
    }

    fn supports_environment(
        &self,
        environment: aetelier_types::exchanges::VenueEnvironment,
    ) -> bool {
        matches!(
            environment,
            aetelier_types::exchanges::VenueEnvironment::Production
        )
    }

    /// Spawn one Ingest connection carrying a SET of symbols, pumping
    /// `DomainEvent`s into `tx`.
    ///
    /// `shutdown` drives a graceful Stop/Drain, bounded by
    /// [`STOP_DRAIN_TIMEOUT`](crate::framework::driver::STOP_DRAIN_TIMEOUT).
    /// The returned handle resolves to a [`TaskExit`] the caller maps onto the
    /// task's terminal transition.
    ///
    /// `tx` is the in-process Ingest→Sync buffer.
    /// Venue-guaranteed publication cadence for `datatype`, mirroring the
    /// same declaration the transport reads off `ProtocolHooks`. `None` means
    /// the venue guarantees nothing and the datatype is never judged silent.
    fn datatype_cadence(
        &self,
        _datatype: crate::framework::feed::FeedDatatype,
    ) -> Option<std::time::Duration> {
        None
    }

    fn spawn(
        &self,
        symbols: Vec<String>,
        declared: aetelier_types::config::markets::market_config::DeclaredSet,
        tx: tokio::sync::mpsc::Sender<DomainEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> tokio::task::JoinHandle<TaskExit>;

    fn spawn_env(
        &self,
        _environment: aetelier_types::exchanges::VenueEnvironment,
        symbols: Vec<String>,
        declared: aetelier_types::config::markets::market_config::DeclaredSet,
        tx: tokio::sync::mpsc::Sender<DomainEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> tokio::task::JoinHandle<TaskExit> {
        self.spawn(symbols, declared, tx, shutdown, metrics)
    }

    /// Decode and normalize one raw wire frame OFFLINE — the same
    /// decoder + normalizer `spawn` drives, without a socket. This is the
    /// fixture-replay path the conformance harness (and any replay tooling)
    /// runs committed captures through, so a kind exercises the real
    /// production decode path rather than a re-implementation.
    ///
    /// Returns the `DomainEvent`s the frame yields (empty for a control /
    /// heartbeat / non-data frame). The default reports the venue as not yet
    /// offline-replayable; each adapter overrides it during its conformance
    /// cycle.
    fn subscribe_frames_preview(
        &self,
        _symbols: &[String],
        _declared: &aetelier_types::config::markets::market_config::DeclaredSet,
    ) -> Vec<String> {
        Vec::new()
    }

    fn replay_frame(
        &self,
        _raw: &str,
    ) -> Result<Vec<DomainEvent>, Box<crate::errors::ExchangeError>> {
        Err(Box::new(crate::errors::ExchangeError::Unsupported(
            format!("offline replay not implemented for venue '{}'", self.id()),
        )))
    }

    fn replay_stream(
        &self,
        _symbols: &[String],
        _datatypes: &aetelier_types::config::markets::market_config::DataTypesSection,
        _commitment: aetelier_types::onchain::Commitment,
        _frames: &[ReplayFrame],
        _metrics: &SourceMetrics,
    ) -> Result<Vec<DomainEvent>, Box<crate::errors::ExchangeError>> {
        Err(Box::new(crate::errors::ExchangeError::Unsupported(
            format!(
                "offline stream replay not implemented for venue '{}'",
                self.id()
            ),
        )))
    }

    /// Parse a raw REST seed-snapshot body OFFLINE into the normalized delta
    /// that seeds a `SourcedOrderbook` — the same parse `rest_seeder` performs
    /// live, without a network fetch. Lets the conformance harness seed a
    /// REST-model book from a committed snapshot fixture and replay the
    /// straddling deltas generically.
    ///
    /// `Ok(None)` for a self-seeding venue (no REST seed to parse). The
    /// default reports the venue as not yet offline-seedable; REST-model
    /// adapters override it during their conformance cycle.
    fn replay_seed(
        &self,
        _raw: &str,
        _wire_symbol: &str,
    ) -> Result<
        Option<aetelier_types::orderbooks::NormalizedDelta>,
        Box<crate::errors::ExchangeError>,
    > {
        Err(Box::new(crate::errors::ExchangeError::Unsupported(
            format!(
                "offline seed replay not implemented for venue '{}'",
                self.id()
            ),
        )))
    }
}

/// The single explicit adapter list, one line per compiled-in venue.
fn register_all() -> Vec<&'static dyn ExchangeAdapter> {
    vec![
        &super::adapters::binance::BINANCE,
        &super::adapters::okx::OKX,
        &super::adapters::upbit::UPBIT,
        &super::adapters::htx::HTX,
        &super::adapters::kucoin::KUCOIN,
        &super::adapters::bitso::BITSO,
        &super::adapters::bybit::BYBIT,
        &super::adapters::coinbase::COINBASE,
        &super::adapters::kraken::KRAKEN,
        &super::adapters::gateio::GATEIO,
        &super::adapters::bitget::BITGET,
        &super::adapters::poloniex::POLONIEX,
        &super::adapters::hyperliquid::HYPERLIQUID,
        #[cfg(feature = "grpc")]
        &super::adapters::solana::SOLANA,
    ]
}

/// Process-wide adapter registry, keyed by venue id. Built once.
pub fn registry() -> &'static HashMap<&'static str, &'static dyn ExchangeAdapter> {
    static R: OnceLock<HashMap<&'static str, &'static dyn ExchangeAdapter>> =
        OnceLock::new();
    R.get_or_init(|| register_all().into_iter().map(|a| (a.id(), a)).collect())
}

/// Boot self-check: every requested venue resolves to a registered adapter.
/// Turns a silent empty stream into a fail-fast at startup. Returns the list of
/// unknown venue ids on failure.
pub fn resolve(ids: &[&str]) -> Result<(), Vec<String>> {
    let reg = registry();
    let missing: Vec<String> = ids
        .iter()
        .filter(|id| !reg.contains_key(**id))
        .map(|id| id.to_string())
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

pub fn admit_declared_depth(
    venue: &str,
    cap: Option<usize>,
    orderbook_enabled: bool,
    declared: usize,
) -> Result<(), String> {
    if !orderbook_enabled {
        return Ok(());
    }
    match cap {
        Some(cap) if declared > cap => Err(format!(
            "venue '{venue}' delivers at most {cap} book levels but this worker \
             asks for depth {declared} — set `[collect.datatypes.orderbook] depth` \
             to {cap} or less (an omitted depth defaults to 50)"
        )),
        _ => Ok(()),
    }
}

pub fn admit_environment(
    venue: &str,
    supported: bool,
    environment: aetelier_types::exchanges::VenueEnvironment,
) -> Result<(), String> {
    if supported {
        return Ok(());
    }
    Err(format!(
        "venue '{venue}' has no {environment} endpoint — remove `environment = \
         \"{environment}\"` rather than collecting production data under a \
         {environment} label"
    ))
}

pub fn admit_market_type(
    venue: &str,
    supported: &[aetelier_types::exchanges::MarketType],
    market_type: aetelier_types::exchanges::MarketType,
) -> Result<(), String> {
    if supported.contains(&market_type) {
        return Ok(());
    }
    Err(format!(
        "venue '{venue}' has no {market_type} market — set `market_type` to {}",
        quoted_choices(supported)
    ))
}

pub fn admit_datatypes(
    exchange: aetelier_types::exchanges::Exchange,
    supported: &[aetelier_types::config::markets::market_config::DeclaredDatatype],
    declared: &aetelier_types::config::markets::market_config::DeclaredSet,
) -> Result<(), String> {
    use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
    if exchange.chain().is_none() {
        if declared.contains(DD::Transactions) && !supported.contains(&DD::Transactions) {
            return Err(format!(
                "venue '{exchange}' delivers no transactions — remove \
                 `[collect.datatypes.transactions]` or set `enabled = false` there"
            ));
        }
        return Ok(());
    }
    let ids: Vec<&str> = supported.iter().map(|dt| dt.id()).collect();
    if declared.is_empty() {
        return Err(format!(
            "venue '{exchange}' declares no datatype — enable {}",
            quoted_choices(&ids)
        ));
    }
    match declared.iter().find(|dt| !supported.contains(dt)) {
        Some(dt) => Err(format!(
            "venue '{exchange}' delivers no {} — enable only {}",
            dt.id(),
            quoted_choices(&ids)
        )),
        None => Ok(()),
    }
}

pub fn admit_registered(
    exchange: aetelier_types::exchanges::Exchange,
) -> Result<&'static dyn ExchangeAdapter, String> {
    if let Some(adapter) = registry().get(exchange.to_string().as_str()) {
        return Ok(*adapter);
    }
    match exchange.chain() {
        Some(_) => Err(format!(
            "venue '{exchange}' needs the `grpc` feature: this build registers no \
             {exchange} adapter"
        )),
        None => Err(format!(
            "venue '{exchange}' has no adapter registered in this build"
        )),
    }
}

fn quoted_choices<T: std::fmt::Display>(items: &[T]) -> String {
    let quoted: Vec<String> = items.iter().map(|item| format!("\"{item}\"")).collect();
    match quoted.split_last() {
        Some((last, rest)) if !rest.is_empty() => {
            format!("{} or {last}", rest.join(", "))
        }
        Some((last, _)) => last.clone(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All twelve venues are compiled in and self-describe a stable id matching
    /// their registry key.
    #[test]
    fn all_thirteen_venues_register_and_resolve() {
        let ids = [
            "binance",
            "okx",
            "upbit",
            "htx",
            "kucoin",
            "bitso",
            "bybit",
            "coinbase",
            "kraken",
            "gateio",
            "bitget",
            "poloniex",
            "hyperliquid",
        ];
        let chain_ids: &[&str] = if cfg!(feature = "grpc") {
            &["solana"]
        } else {
            &[]
        };
        let reg = registry();
        assert_eq!(reg.len(), ids.len() + chain_ids.len());
        for id in ids.iter().chain(chain_ids) {
            let adapter = reg.get(id).unwrap_or_else(|| panic!("{id} not registered"));
            assert_eq!(adapter.id(), *id, "registry key must equal adapter.id()");
            assert_eq!(adapter.profile().id, *id, "profile.id must equal venue id");
        }
        assert!(resolve(&ids).is_ok());
        assert_eq!(resolve(&["solana"]).is_ok(), cfg!(feature = "grpc"));
    }

    #[test]
    fn admit_declared_depth_refuses_beyond_cap_and_admits_within() {
        assert!(admit_declared_depth("hyperliquid", Some(20), true, 50).is_err());
        assert!(admit_declared_depth("hyperliquid", Some(20), true, 20).is_ok());
        assert!(
            admit_declared_depth("binance", None, true, 5000).is_ok(),
            "venues declaring no cap are unconstrained"
        );
        let err = admit_declared_depth("hyperliquid", Some(20), true, 50).unwrap_err();
        assert!(err.contains("20") && err.contains("50"), "{err}");
    }

    #[test]
    fn declared_depth_is_irrelevant_when_the_book_feed_is_off() {
        assert!(
            admit_declared_depth("hyperliquid", Some(20), false, 50).is_ok(),
            "a trades-only worker never subscribes a book, so the serde default \
             depth of 50 must not refuse it"
        );
    }

    #[test]
    fn admit_environment_refuses_an_unsupported_deployment() {
        use aetelier_types::exchanges::VenueEnvironment as VE;
        assert!(admit_environment("hyperliquid", true, VE::Testnet).is_ok());
        assert!(admit_environment("bybit", true, VE::Production).is_ok());
        let err = admit_environment("bybit", false, VE::Testnet).unwrap_err();
        assert!(err.contains("testnet"), "{err}");
    }

    #[test]
    fn only_hyperliquid_and_solana_declare_a_testnet_deployment() {
        use aetelier_types::exchanges::VenueEnvironment as VE;
        for (id, adapter) in registry() {
            assert!(
                adapter.supports_environment(VE::Production),
                "{id} must support production"
            );
            assert_eq!(
                adapter.supports_environment(VE::Testnet),
                matches!(*id, "hyperliquid" | "solana"),
                "{id} testnet support must be declared, never assumed"
            );
        }
    }

    #[test]
    fn max_declared_depth_matches_the_twenty_level_l2book() {
        let hl = registry().get("hyperliquid").copied().unwrap();
        assert_eq!(hl.max_declared_depth(), Some(20));
        let bybit = registry().get("bybit").copied().unwrap();
        assert_eq!(bybit.max_declared_depth(), None);
    }

    #[test]
    fn resolve_reports_unknown_venues() {
        let err = resolve(&["binance", "ftx"]).unwrap_err();
        assert_eq!(err, vec!["ftx".to_string()]);
    }

    #[test]
    fn only_chain_venues_carry_the_chain_address_codec() {
        use aetelier_types::exchanges::Exchange;
        for (id, adapter) in registry() {
            let exchange: Exchange = id.parse().unwrap();
            let codec = &adapter.profile().symbol_codec;
            match exchange.chain() {
                Some(chain) => assert!(
                    matches!(codec, SymbolCodec::ChainAddress { chain: c } if *c == chain),
                    "{id} must decode {chain} addresses, not {codec:?}"
                ),
                None => assert!(
                    !matches!(codec, SymbolCodec::ChainAddress { .. }),
                    "{id} is not a chain venue but carries {codec:?}"
                ),
            }
        }
    }

    #[test]
    fn admit_market_type_names_the_supported_set() {
        use aetelier_types::exchanges::MarketType as MT;
        let cex = [MT::Spot, MT::Perpetual, MT::Inverse];
        assert!(admit_market_type("bybit", &cex, MT::Perpetual).is_ok());
        let err = admit_market_type("bybit", &cex, MT::Onchain).unwrap_err();
        assert_eq!(
            err,
            "venue 'bybit' has no onchain market — set `market_type` to \"spot\", \
             \"perpetual\" or \"inverse\""
        );
        assert!(admit_market_type("solana", &[MT::Onchain], MT::Onchain).is_ok());
        let err = admit_market_type("solana", &[MT::Onchain], MT::Spot).unwrap_err();
        assert_eq!(
            err,
            "venue 'solana' has no spot market — set `market_type` to \"onchain\""
        );
    }

    #[test]
    fn every_cex_adapter_admits_spot_perpetual_inverse_and_refuses_onchain() {
        use aetelier_types::exchanges::{Exchange, MarketType as MT};
        for (id, adapter) in registry() {
            let exchange: Exchange = id.parse().unwrap();
            if exchange.chain().is_some() {
                continue;
            }
            let supported = adapter.supported_market_types();
            for market_type in [MT::Spot, MT::Perpetual, MT::Inverse] {
                assert!(
                    admit_market_type(id, supported, market_type).is_ok(),
                    "{id} must admit {market_type}"
                );
            }
            assert!(
                admit_market_type(id, supported, MT::Onchain).is_err(),
                "{id}"
            );
        }
    }

    #[test]
    fn admit_datatypes_keeps_the_cex_derivative_fallback_and_refuses_transactions() {
        use aetelier_types::config::markets::market_config::{
            DataTypesSection, DeclaredDatatype as DD,
        };
        use aetelier_types::exchanges::Exchange;
        let bybit = registry().get("bybit").copied().unwrap();
        let supported = bybit.supported_datatypes();
        assert!(!supported.contains(&DD::Liquidations));
        let mut section = DataTypesSection::default();
        section.orderbook.enabled = true;
        section.trades.enabled = true;
        section.liquidations.enabled = true;
        assert!(
            admit_datatypes(Exchange::Bybit, supported, &section.declared_set()).is_ok()
        );
        section.transactions.enabled = true;
        let err = admit_datatypes(Exchange::Bybit, supported, &section.declared_set())
            .unwrap_err();
        assert!(
            err.contains("bybit") && err.contains("transactions"),
            "{err}"
        );
    }

    #[test]
    fn admit_datatypes_on_a_chain_venue_requires_a_non_empty_subset() {
        use aetelier_types::config::markets::market_config::{
            DataTypesSection, DeclaredDatatype as DD, DeclaredSet,
        };
        use aetelier_types::exchanges::Exchange;
        let supported = [DD::Transactions];
        let only_transactions = DeclaredSet::only(DD::Transactions);
        assert!(
            admit_datatypes(Exchange::Solana, &supported, &only_transactions).is_ok()
        );
        let err = admit_datatypes(Exchange::Solana, &supported, &DeclaredSet::default())
            .unwrap_err();
        assert!(
            err.contains("declares no datatype") && err.contains("\"transactions\""),
            "{err}"
        );
        let mut section = DataTypesSection::default();
        section.transactions.enabled = true;
        section.orderbook.enabled = true;
        let err = admit_datatypes(Exchange::Solana, &supported, &section.declared_set())
            .unwrap_err();
        assert!(err.contains("delivers no orderbook"), "{err}");
    }

    #[cfg(not(feature = "grpc"))]
    #[test]
    fn admit_registered_names_the_grpc_feature_for_solana() {
        use aetelier_types::exchanges::Exchange;
        let err = admit_registered(Exchange::Solana)
            .err()
            .expect("this build registers no solana adapter");
        assert_eq!(
            err,
            "venue 'solana' needs the `grpc` feature: this build registers no solana \
             adapter"
        );
        for exchange in Exchange::all().into_iter().filter(|e| e.chain().is_none()) {
            match admit_registered(exchange) {
                Ok(adapter) => assert_eq!(adapter.id(), exchange.to_string()),
                Err(e) => panic!("{e}"),
            }
        }
    }

    #[test]
    fn no_cex_adapter_sets_a_default_emission_delay() {
        use aetelier_types::exchanges::Exchange;
        for (id, adapter) in registry() {
            if id.parse::<Exchange>().unwrap().chain().is_some() {
                continue;
            }
            assert_eq!(adapter.default_emission_delay_us(), None, "{id}");
        }
    }

    #[test]
    fn no_cex_adapter_declares_a_transactions_cadence() {
        use crate::framework::feed::FeedDatatype;
        use aetelier_types::exchanges::Exchange;
        for (id, adapter) in registry() {
            let exchange: Exchange = id.parse().unwrap();
            if exchange.chain().is_none() {
                assert_eq!(
                    adapter.datatype_cadence(FeedDatatype::Transactions),
                    None,
                    "{id}"
                );
            }
        }
    }

    #[test]
    fn book_venues_refuse_stream_replay() {
        use aetelier_types::config::markets::market_config::DataTypesSection;
        use aetelier_types::onchain::Commitment;
        let mut datatypes = DataTypesSection::default();
        datatypes.transactions.enabled = true;
        let frames = [
            ReplayFrame::Update {
                t_us: 1_791_321_067_000_000,
                bytes: vec![0x0a, 0x00],
            },
            ReplayFrame::Reconnect {
                t_us: 1_791_321_068_000_000,
            },
            ReplayFrame::Update {
                t_us: 1_791_321_069_000_000,
                bytes: Vec::new(),
            },
        ];
        let metrics = SourceMetrics::default();
        for (id, adapter) in registry() {
            if id
                .parse::<aetelier_types::exchanges::Exchange>()
                .unwrap()
                .chain()
                .is_some()
            {
                continue;
            }
            assert!(adapter.book_model("orders").reconstructs_books(), "{id}");
            let err = adapter
                .replay_stream(
                    &["BTCUSDT".to_string()],
                    &datatypes,
                    Commitment::Finalized,
                    &frames,
                    &metrics,
                )
                .expect_err("a book venue has no stream replay");
            match *err {
                crate::errors::ExchangeError::Unsupported(reason) => assert_eq!(
                    reason,
                    format!("offline stream replay not implemented for venue '{id}'")
                ),
                other => panic!("{id}: {other:?}"),
            }
        }
        assert_eq!(metrics.snapshot(), SourceMetrics::default().snapshot());
    }
}
