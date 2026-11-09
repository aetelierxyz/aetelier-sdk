//! Ingest subscription primitive: a `Feed` is a live subscription to one
//! `(venue, instrument, datatype)` market-data stream from an external
//! exchange. Its `FeedId` is allocated at creation and its state is reported
//! via the telemetry channel.

use std::sync::{Arc, Mutex};

use aetelier_types::Instrument;
use aetelier_types::config::markets::market_config::{DeclaredDatatype, DeclaredSet};
use aetelier_types::exchanges::Exchange;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identifier for one `Feed` (UUIDv4). Distinct newtype so it cannot be
/// confused with the wire `ArtifactId`/`TaskId`/`SinkId`. The wire shape stays
/// a flat string; this wraps it in the language layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedId(pub Uuid);

impl FeedId {
    /// Allocate a fresh `FeedId` at Feed creation.
    pub fn new() -> Self {
        FeedId(Uuid::new_v4())
    }
}

impl Default for FeedId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for FeedId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The market-data type a `Feed` subscribes to. Selects the bound book
/// (orders → `SourcedOrderbook`, trades → `SourcedTradebook`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeedDatatype {
    Orders,
    Trades,
    FundingRates,
    OpenInterest,
    Transactions,
}

impl FeedDatatype {
    pub const ALL: [FeedDatatype; 5] = [
        FeedDatatype::Orders,
        FeedDatatype::Trades,
        FeedDatatype::FundingRates,
        FeedDatatype::OpenInterest,
        FeedDatatype::Transactions,
    ];

    pub const fn for_declared(declared: DeclaredDatatype) -> Option<FeedDatatype> {
        match declared {
            DeclaredDatatype::Orderbook => Some(FeedDatatype::Orders),
            DeclaredDatatype::Trades => Some(FeedDatatype::Trades),
            DeclaredDatatype::Liquidations => None,
            DeclaredDatatype::FundingRates => Some(FeedDatatype::FundingRates),
            DeclaredDatatype::OpenInterest => Some(FeedDatatype::OpenInterest),
            DeclaredDatatype::Transactions => Some(FeedDatatype::Transactions),
        }
    }
}

/// `Feed` lifecycle state. `Closed`/`Rejected`/`Failed` are terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeedState {
    /// Created from the TaskSpec; FeedId allocated; nothing on the wire.
    Requested,
    /// Subscribe frame(s) sent; awaiting first data / seed.
    Subscribing,
    /// Streaming; the bound book is Live.
    Live,
    /// Streaming socket, silent datatype: no event for this
    /// `(instrument, datatype)` within its venue-declared cadence envelope.
    /// Datatype-scoped — the connection and every sibling feed stay Live.
    /// Recovers to `Live` on the next event; never terminal.
    Stale,
    /// Recovering from a book `Gapped`; re-seed per the recovery action.
    Resubscribing,
    /// The shared venue connection dropped; awaiting re-establishment.
    Reconnecting,
    /// Task Stop; flushing in-flight, bounded by [`STOP_DRAIN_TIMEOUT`](crate::framework::driver::STOP_DRAIN_TIMEOUT).
    Draining,
    /// Normal terminal (possibly `partial`).
    Closed,
    /// Terminal; venue/datatype unsupported (surfaces an ErrorKind).
    Rejected,
    /// Terminal; connection unrecoverable within the reconnect budget.
    Failed,
}

impl FeedState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            FeedState::Closed | FeedState::Rejected | FeedState::Failed
        )
    }

    /// The legal transition graph — the executable form of the Feed FSM's
    /// edge set (atlas DAT-F-T-*). Setters `debug_assert!` against it, so an
    /// illegal jump panics under test/CI and costs nothing in release.
    /// Terminal states have no outgoing edges.
    pub fn can_transition_to(self, next: FeedState) -> bool {
        use FeedState::*;
        matches!(
            (self, next),
            (Requested, Subscribing | Draining)
                | (Subscribing, Live | Resubscribing | Reconnecting | Draining)
                | (Live, Stale | Resubscribing | Reconnecting | Draining)
                | (Stale, Live | Resubscribing | Reconnecting | Draining)
                | (Resubscribing, Live | Reconnecting | Draining)
                | (Reconnecting, Subscribing | Rejected | Failed | Draining)
                | (Draining, Closed)
        )
    }
}

/// One live `(venue, instrument, datatype)` subscription. The bound book is
/// held by the Sync layer keyed off `id`; this struct carries the subscription
/// identity and FSM state.
#[derive(Debug, Clone)]
pub struct Feed {
    id: FeedId,
    venue: Exchange,
    instrument: Instrument,
    datatype: FeedDatatype,
    state: FeedState,
    /// Set when the drain exceeded its bound: `Closed` with outstanding
    /// in-flight data (`TaskExit::DrainTimedOut`).
    closed_partial: bool,
    /// Set on entry to `Rejected`/`Failed`: the venue rejection or give-up
    /// reason, so terminal feeds carry WHY on the status/telemetry surface.
    terminal_reason: Option<String>,
}

impl Feed {
    /// Create a Feed for a TaskSpec entry; allocates the `FeedId` and
    /// lands in `Requested`.
    pub fn new(
        venue: Exchange,
        instrument: impl Into<Instrument>,
        datatype: FeedDatatype,
    ) -> Self {
        Self {
            id: FeedId::new(),
            venue,
            instrument: instrument.into(),
            datatype,
            state: FeedState::Requested,
            closed_partial: false,
            terminal_reason: None,
        }
    }

    pub fn id(&self) -> FeedId {
        self.id
    }
    pub fn venue(&self) -> Exchange {
        self.venue
    }
    pub fn instrument(&self) -> &Instrument {
        &self.instrument
    }
    pub fn datatype(&self) -> FeedDatatype {
        self.datatype
    }
    pub fn state(&self) -> FeedState {
        self.state
    }

    // FSM transitions. The worker invokes these at the transition boundaries
    // after performing the guard/effects; this records the state. Each
    // setter asserts the edge against `FeedState::can_transition_to` in
    // debug builds.

    fn transition(&mut self, next: FeedState) {
        debug_assert!(
            self.state.can_transition_to(next),
            "illegal Feed transition {:?} -> {:?} (feed {})",
            self.state,
            next,
            self.id
        );
        self.state = next;
    }

    /// Subscribe sent (from Requested or Reconnecting).
    pub fn to_subscribing(&mut self) {
        self.transition(FeedState::Subscribing);
    }
    /// First data + bound book Live (from Subscribing or Resubscribing).
    pub fn to_stale(&mut self) {
        self.transition(FeedState::Stale);
    }

    pub fn to_live(&mut self) {
        self.transition(FeedState::Live);
    }
    /// Bound book `Gapped`; begin recovery.
    pub fn to_resubscribing(&mut self) {
        self.transition(FeedState::Resubscribing);
    }
    /// Shared venue connection dropped (retryable).
    pub fn to_reconnecting(&mut self) {
        self.transition(FeedState::Reconnecting);
    }
    /// Task Stop/Drain.
    pub fn to_draining(&mut self) {
        self.transition(FeedState::Draining);
    }
    /// Drained (clean or partial-past-timeout) → terminal.
    pub fn to_closed(&mut self) {
        self.transition(FeedState::Closed);
    }
    /// Drain exceeded its bound → terminal with outstanding data (`partial`).
    pub fn to_closed_partial(&mut self) {
        self.transition(FeedState::Closed);
        self.closed_partial = true;
    }
    /// Whether a `Closed` feed drained past the bound with data outstanding.
    pub fn closed_partial(&self) -> bool {
        self.closed_partial
    }
    /// Venue/datatype unsupported → terminal, carrying the venue reason.
    pub fn to_rejected(&mut self, reason: &str) {
        self.transition(FeedState::Rejected);
        self.terminal_reason = Some(reason.to_string());
    }
    /// Reconnect budget exhausted → terminal, carrying the give-up reason.
    pub fn to_failed(&mut self, reason: &str) {
        self.transition(FeedState::Failed);
        self.terminal_reason = Some(reason.to_string());
    }
    /// The rejection/give-up reason of a `Rejected`/`Failed` feed.
    pub fn terminal_reason(&self) -> Option<&str> {
        self.terminal_reason.as_deref()
    }
}

/// Point-in-time view of one feed, for status/telemetry fan-out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedSnapshot {
    /// The `FeedId`, stringified for the wire.
    pub id: String,
    /// Canonical instrument (e.g. `BTC/USDT`).
    pub instrument: String,
    pub datatype: FeedDatatype,
    pub state: FeedState,
    /// Meaningful only in `Closed`: drain exceeded its bound.
    pub partial: bool,
    /// Meaningful only in `Rejected`/`Failed`: the venue rejection or
    /// give-up reason (`serde(default)` keeps older producers valid).
    #[serde(default)]
    pub reason: Option<String>,
}

/// The worker's set of feeds for one connection — one per
/// (instrument, datatype) it subscribes. Owned by the worker so `FeedId`s
/// survive socket reconnects; shared into each `SourceRuntime` iteration.
///
/// Terminal states absorb: every `mark_*` skips feeds whose state
/// `is_terminal()`, so a `Closed`/`Rejected`/`Failed` feed never regresses.
#[derive(Debug, Default)]
pub struct FeedSet {
    feeds: Vec<Feed>,
}

/// Shared handle: worker owns it, runtimes transition it. Lock discipline:
/// never held across an await.
pub type SharedFeedSet = Arc<Mutex<FeedSet>>;

impl FeedSet {
    pub fn new(feeds: Vec<Feed>) -> Self {
        Self { feeds }
    }

    pub fn for_declared(
        venue: Exchange,
        instrument: &Instrument,
        declared: &DeclaredSet,
    ) -> Self {
        Self::new(
            declared
                .iter()
                .filter_map(FeedDatatype::for_declared)
                .map(|datatype| Feed::new(venue, instrument.clone(), datatype))
                .collect(),
        )
    }

    /// Wrap into the shared handle the worker hands to each runtime.
    pub fn into_shared(self) -> SharedFeedSet {
        Arc::new(Mutex::new(self))
    }

    fn each_live_mut(&mut self, f: impl Fn(&mut Feed)) {
        for feed in self.feeds.iter_mut().filter(|x| !x.state().is_terminal()) {
            f(feed);
        }
    }

    /// Subscribe frames sent for this connection (initial or after reconnect).
    pub fn mark_all_subscribing(&mut self) {
        self.each_live_mut(|f| f.to_subscribing());
    }

    /// The shared venue connection dropped; every non-terminal feed waits.
    pub fn mark_all_reconnecting(&mut self) {
        self.each_live_mut(|f| f.to_reconnecting());
    }

    /// First data for `(instrument, datatype)` — its bound book is live.
    pub fn mark_live(&mut self, instrument: &Instrument, datatype: FeedDatatype) {
        if let Some(f) = self.get_mut(instrument, datatype)
            && !f.state().is_terminal()
            && f.state() != FeedState::Live
        {
            f.to_live();
        }
    }

    /// No event for `(instrument, datatype)` inside its declared cadence
    /// envelope, while the socket itself stays live.
    pub fn mark_stale(&mut self, instrument: &Instrument, datatype: FeedDatatype) {
        if let Some(f) = self.get_mut(instrument, datatype)
            && f.state() == FeedState::Live
        {
            f.to_stale();
        }
    }

    /// The bound book for `(instrument, datatype)` gapped; recovery begins.
    pub fn mark_resubscribing(
        &mut self,
        instrument: &Instrument,
        datatype: FeedDatatype,
    ) {
        if let Some(f) = self.get_mut(instrument, datatype)
            && !f.state().is_terminal()
            && f.state() != FeedState::Resubscribing
        {
            f.to_resubscribing();
        }
    }

    /// Task Stop received; drain begins.
    pub fn mark_all_draining(&mut self) {
        self.each_live_mut(|f| f.to_draining());
    }

    /// Drain finished; `partial` when it exceeded the bound.
    pub fn mark_all_closed(&mut self, partial: bool) {
        self.each_live_mut(|f| {
            if partial {
                f.to_closed_partial();
            } else {
                f.to_closed();
            }
        });
    }

    /// Venue rejected the subscription protocol — terminal for every feed,
    /// each carrying the rejection reason.
    pub fn mark_all_rejected(&mut self, reason: &str) {
        self.each_live_mut(|f| f.to_rejected(reason));
    }

    /// Connection unrecoverable — terminal for every feed, each carrying the
    /// give-up reason.
    pub fn mark_all_failed(&mut self, reason: &str) {
        self.each_live_mut(|f| f.to_failed(reason));
    }

    fn get_mut(
        &mut self,
        instrument: &Instrument,
        datatype: FeedDatatype,
    ) -> Option<&mut Feed> {
        self.feeds
            .iter_mut()
            .find(|f| f.instrument() == instrument && f.datatype() == datatype)
    }

    /// Point-in-time snapshot for status/telemetry.
    pub fn snapshot(&self) -> Vec<FeedSnapshot> {
        self.feeds
            .iter()
            .map(|f| FeedSnapshot {
                id: f.id().to_string(),
                instrument: f.instrument().canonical(),
                datatype: f.datatype(),
                state: f.state(),
                partial: f.closed_partial(),
                reason: f.terminal_reason().map(str::to_string),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_live_feed_may_go_stale_and_recover() {
        assert!(FeedState::Live.can_transition_to(FeedState::Stale));
        assert!(FeedState::Stale.can_transition_to(FeedState::Live));
        assert!(!FeedState::Stale.is_terminal());
    }

    #[test]
    fn staleness_is_not_reachable_before_data() {
        assert!(!FeedState::Requested.can_transition_to(FeedState::Stale));
        assert!(!FeedState::Subscribing.can_transition_to(FeedState::Stale));
        assert!(!FeedState::Resubscribing.can_transition_to(FeedState::Stale));
    }

    #[test]
    fn a_stale_feed_still_reaches_recovery_and_shutdown() {
        assert!(FeedState::Stale.can_transition_to(FeedState::Resubscribing));
        assert!(FeedState::Stale.can_transition_to(FeedState::Reconnecting));
        assert!(FeedState::Stale.can_transition_to(FeedState::Draining));
    }

    #[test]
    fn marking_stale_touches_only_the_named_datatype() {
        let pair = Instrument::from(TradingPair::new("BTC", "USDC"));
        let mut set = FeedSet::new(vec![
            Feed::new(Exchange::Hyperliquid, pair.clone(), FeedDatatype::Orders),
            Feed::new(
                Exchange::Hyperliquid,
                pair.clone(),
                FeedDatatype::FundingRates,
            ),
        ]);
        set.mark_all_subscribing();
        set.mark_live(&pair, FeedDatatype::Orders);
        set.mark_live(&pair, FeedDatatype::FundingRates);

        set.mark_stale(&pair, FeedDatatype::FundingRates);

        assert_eq!(
            set.get_mut(&pair, FeedDatatype::FundingRates)
                .unwrap()
                .state(),
            FeedState::Stale
        );
        assert_eq!(
            set.get_mut(&pair, FeedDatatype::Orders).unwrap().state(),
            FeedState::Live,
            "the socket and its sibling feeds stay live"
        );

        set.mark_live(&pair, FeedDatatype::FundingRates);
        assert_eq!(
            set.get_mut(&pair, FeedDatatype::FundingRates)
                .unwrap()
                .state(),
            FeedState::Live
        );
    }

    #[test]
    fn a_feed_that_never_reached_live_is_never_marked_stale() {
        let pair = Instrument::from(TradingPair::new("BTC", "USDC"));
        let mut set = FeedSet::new(vec![Feed::new(
            Exchange::Hyperliquid,
            pair.clone(),
            FeedDatatype::FundingRates,
        )]);
        set.mark_all_subscribing();
        set.mark_stale(&pair, FeedDatatype::FundingRates);
        assert_eq!(
            set.get_mut(&pair, FeedDatatype::FundingRates)
                .unwrap()
                .state(),
            FeedState::Subscribing
        );
    }
    use super::*;
    use aetelier_types::onchain::ChainAddress;
    use aetelier_types::trading_pair::TradingPair;

    #[test]
    fn feed_allocates_id_and_walks_to_live() {
        let mut f = Feed::new(
            Exchange::Binance,
            TradingPair::new("BTC", "USDT"),
            FeedDatatype::Orders,
        );
        assert_eq!(f.state(), FeedState::Requested);
        let id = f.id();
        f.to_subscribing();
        f.to_live();
        assert_eq!(f.state(), FeedState::Live);
        assert_eq!(f.id(), id); // FeedId stable across transitions
        assert!(!f.state().is_terminal());
    }

    #[test]
    fn feed_ids_are_unique() {
        let a = FeedId::new();
        let b = FeedId::new();
        assert_ne!(a, b);
    }

    fn set() -> FeedSet {
        let pair = TradingPair::new("BTC", "USDT");
        FeedSet::new(vec![
            Feed::new(Exchange::Binance, pair.clone(), FeedDatatype::Orders),
            Feed::new(Exchange::Binance, pair, FeedDatatype::Trades),
        ])
    }

    fn state_of(fs: &FeedSet, dt: FeedDatatype) -> FeedState {
        fs.snapshot()
            .iter()
            .find(|f| f.datatype == dt)
            .unwrap()
            .state
    }

    #[test]
    fn feed_set_walks_the_happy_lifecycle() {
        let pair = Instrument::from(TradingPair::new("BTC", "USDT"));
        let mut fs = set();
        fs.mark_all_subscribing();
        fs.mark_live(&pair, FeedDatatype::Orders);
        assert_eq!(state_of(&fs, FeedDatatype::Orders), FeedState::Live);
        assert_eq!(state_of(&fs, FeedDatatype::Trades), FeedState::Subscribing);
        fs.mark_resubscribing(&pair, FeedDatatype::Orders);
        assert_eq!(
            state_of(&fs, FeedDatatype::Orders),
            FeedState::Resubscribing
        );
        fs.mark_all_reconnecting();
        fs.mark_all_subscribing();
        fs.mark_live(&pair, FeedDatatype::Trades);
        fs.mark_all_draining();
        fs.mark_all_closed(false);
        assert_eq!(state_of(&fs, FeedDatatype::Orders), FeedState::Closed);
        assert!(!fs.snapshot()[0].partial);
    }

    #[test]
    fn terminal_states_absorb_every_marking() {
        let pair = Instrument::from(TradingPair::new("BTC", "USDT"));
        let mut fs = set();
        fs.mark_all_subscribing();
        fs.mark_all_reconnecting();
        fs.mark_all_rejected("test rejection");
        fs.mark_all_subscribing();
        fs.mark_live(&pair, FeedDatatype::Orders);
        fs.mark_all_closed(true);
        assert_eq!(state_of(&fs, FeedDatatype::Orders), FeedState::Rejected);
        assert_eq!(state_of(&fs, FeedDatatype::Trades), FeedState::Rejected);
        assert!(!fs.snapshot()[0].partial);
    }

    #[test]
    fn edge_table_matches_the_wired_paths() {
        use FeedState::*;
        let legal = [
            (Requested, Subscribing),
            (Requested, Draining),
            (Subscribing, Live),
            (Subscribing, Resubscribing),
            (Subscribing, Reconnecting),
            (Subscribing, Draining),
            (Live, Resubscribing),
            (Live, Reconnecting),
            (Live, Draining),
            (Resubscribing, Live),
            (Resubscribing, Reconnecting),
            (Resubscribing, Draining),
            (Reconnecting, Subscribing),
            (Reconnecting, Rejected),
            (Reconnecting, Failed),
            (Reconnecting, Draining),
            (Draining, Closed),
        ];
        for (from, to) in legal {
            assert!(
                from.can_transition_to(to),
                "{from:?} -> {to:?} must be legal"
            );
        }
        let all = [
            Requested,
            Subscribing,
            Live,
            Resubscribing,
            Reconnecting,
            Draining,
            Closed,
            Rejected,
            Failed,
        ];
        for terminal in [Closed, Rejected, Failed] {
            for next in all {
                assert!(
                    !terminal.can_transition_to(next),
                    "terminal {terminal:?} must have no outgoing edges"
                );
            }
        }
        for state in all {
            assert!(
                !state.can_transition_to(state),
                "{state:?} self-loop must be illegal"
            );
        }
        assert!(!Draining.can_transition_to(Live));
        assert!(!Requested.can_transition_to(Live));
        assert!(!Reconnecting.can_transition_to(Live));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "illegal Feed transition")]
    fn illegal_jump_panics_under_debug() {
        let mut f = Feed::new(
            Exchange::Binance,
            TradingPair::new("BTC", "USDT"),
            FeedDatatype::Orders,
        );
        f.to_live();
    }

    #[test]
    fn partial_close_records_the_timed_out_drain() {
        let mut fs = set();
        fs.mark_all_subscribing();
        fs.mark_all_draining();
        fs.mark_all_closed(true);
        assert_eq!(state_of(&fs, FeedDatatype::Orders), FeedState::Closed);
        assert!(fs.snapshot().iter().all(|f| f.partial));
    }

    #[test]
    fn feed_datatype_wire_names_are_the_variant_names() {
        let wire: Vec<String> = FeedDatatype::ALL
            .iter()
            .map(|dt| serde_json::to_string(dt).unwrap())
            .collect();
        assert_eq!(
            wire,
            [
                "\"Orders\"",
                "\"Trades\"",
                "\"FundingRates\"",
                "\"OpenInterest\"",
                "\"Transactions\"",
            ]
        );
        for (dt, json) in FeedDatatype::ALL.into_iter().zip(&wire) {
            assert_eq!(serde_json::from_str::<FeedDatatype>(json).unwrap(), dt);
        }
    }

    #[test]
    fn declared_datatypes_map_to_feeds_and_liquidations_stay_feedless() {
        let expected = [
            (DeclaredDatatype::Orderbook, Some(FeedDatatype::Orders)),
            (DeclaredDatatype::Trades, Some(FeedDatatype::Trades)),
            (DeclaredDatatype::Liquidations, None),
            (
                DeclaredDatatype::FundingRates,
                Some(FeedDatatype::FundingRates),
            ),
            (
                DeclaredDatatype::OpenInterest,
                Some(FeedDatatype::OpenInterest),
            ),
            (
                DeclaredDatatype::Transactions,
                Some(FeedDatatype::Transactions),
            ),
        ];
        assert_eq!(expected.len(), DeclaredDatatype::ALL.len());
        for (declared, feed) in expected {
            assert_eq!(FeedDatatype::for_declared(declared), feed, "{declared:?}");
        }
    }

    #[test]
    fn cex_feed_list_is_unchanged() {
        use aetelier_types::config::markets::market_config::DataTypesSection;
        let section: DataTypesSection = toml::from_str(
            r#"
[orderbook]
enabled = true

[trades]
enabled = true

[liquidations]
enabled = true

[funding_rates]
enabled = true

[open_interest]
enabled = true
"#,
        )
        .unwrap();
        let feeds: Vec<FeedDatatype> = section
            .declared_set()
            .iter()
            .filter_map(FeedDatatype::for_declared)
            .collect();
        assert_eq!(
            feeds,
            [
                FeedDatatype::Orders,
                FeedDatatype::Trades,
                FeedDatatype::FundingRates,
                FeedDatatype::OpenInterest,
            ]
        );
    }

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
    const WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

    #[test]
    fn pair_feed_snapshot_keeps_the_canonical_pair_string() {
        for (venue, base, quote, canonical) in [
            (Exchange::Binance, "btc", "usdt", "BTC/USDT"),
            (Exchange::Upbit, "BTC", "KRW", "BTC/KRW"),
            (Exchange::Hyperliquid, "xyz:tsla", "usdc", "xyz:TSLA/USDC"),
        ] {
            let pair = TradingPair::new(base, quote);
            let snapshot =
                FeedSet::new(vec![Feed::new(venue, pair.clone(), FeedDatatype::Trades)])
                    .snapshot();
            assert_eq!(snapshot[0].instrument, canonical);
            assert_eq!(snapshot[0].instrument, pair.to_canonical());
        }
    }

    #[test]
    fn feed_set_matches_a_pair_by_canonical_identity_only() {
        let mut fs = FeedSet::new(vec![Feed::new(
            Exchange::Binance,
            TradingPair::new("BTC", "USDT"),
            FeedDatatype::Trades,
        )]);
        fs.mark_all_subscribing();
        for other in [
            TradingPair::new("BTCU", "SDT"),
            TradingPair::new("BTC", "USDC"),
            TradingPair::new("USDT", "BTC"),
        ] {
            let other = Instrument::from(other);
            fs.mark_live(&other, FeedDatatype::Trades);
            assert_eq!(state_of(&fs, FeedDatatype::Trades), FeedState::Subscribing);
        }
        let same = Instrument::from(TradingPair::new(" btc ", "usdt"));
        fs.mark_live(&same, FeedDatatype::Orders);
        assert_eq!(state_of(&fs, FeedDatatype::Trades), FeedState::Subscribing);
        fs.mark_live(&same, FeedDatatype::Trades);
        assert_eq!(state_of(&fs, FeedDatatype::Trades), FeedState::Live);
    }

    #[test]
    fn feed_snapshot_json_shape_is_unchanged_for_pairs() {
        let pair = Instrument::from(TradingPair::new("BTC", "USDT"));
        let mut fs = set();
        fs.mark_all_subscribing();
        fs.mark_live(&pair, FeedDatatype::Orders);
        let snapshot = fs.snapshot();
        let json = serde_json::to_string(&snapshot).unwrap();
        assert_eq!(
            json,
            format!(
                r#"[{{"id":"{}","instrument":"BTC/USDT","datatype":"Orders","state":"Live","partial":false,"reason":null}},{{"id":"{}","instrument":"BTC/USDT","datatype":"Trades","state":"Subscribing","partial":false,"reason":null}}]"#,
                snapshot[0].id, snapshot[1].id
            )
        );
        assert_eq!(
            serde_json::from_str::<Vec<FeedSnapshot>>(&json).unwrap(),
            snapshot
        );

        fs.mark_all_reconnecting();
        fs.mark_all_rejected("venue refused");
        let rejected = serde_json::to_value(&fs.snapshot()[0]).unwrap();
        assert_eq!(
            rejected,
            serde_json::json!({
                "id": snapshot[0].id,
                "instrument": "BTC/USDT",
                "datatype": "Orders",
                "state": "Rejected",
                "partial": false,
                "reason": "venue refused",
            })
        );
    }

    #[test]
    fn pair_and_address_feeds_are_keyed_by_instrument() {
        let pair = Instrument::from(TradingPair::new("SOL", "USDC"));
        let jupiter = Instrument::from(ChainAddress::solana(JUPITER_V6).unwrap());
        let whirlpool = Instrument::from(ChainAddress::solana(WHIRLPOOL).unwrap());
        let mut fs = FeedSet::new(vec![
            Feed::new(Exchange::Hyperliquid, pair.clone(), FeedDatatype::Trades),
            Feed::new(
                Exchange::Solana,
                jupiter.clone(),
                FeedDatatype::Transactions,
            ),
            Feed::new(
                Exchange::Solana,
                whirlpool.clone(),
                FeedDatatype::Transactions,
            ),
        ]);
        fs.mark_all_subscribing();
        fs.mark_live(&jupiter, FeedDatatype::Trades);
        fs.mark_live(&pair, FeedDatatype::Transactions);
        fs.mark_live(&jupiter, FeedDatatype::Transactions);
        fs.mark_live(&pair, FeedDatatype::Trades);
        fs.mark_stale(&pair, FeedDatatype::Trades);
        let states: Vec<(String, FeedState)> = fs
            .snapshot()
            .into_iter()
            .map(|f| (f.instrument, f.state))
            .collect();
        assert_eq!(
            states,
            [
                ("SOL/USDC".to_string(), FeedState::Stale),
                (JUPITER_V6.to_string(), FeedState::Live),
                (WHIRLPOOL.to_string(), FeedState::Subscribing),
            ]
        );
    }

    #[test]
    fn feed_new_accepts_a_trading_pair() {
        let pair = TradingPair::new("ETH", "USDT");
        let from_pair = Feed::new(Exchange::Okx, pair.clone(), FeedDatatype::Trades);
        let from_instrument = Feed::new(
            Exchange::Okx,
            Instrument::Pair(pair.clone()),
            FeedDatatype::Trades,
        );
        assert_eq!(from_pair.instrument(), from_instrument.instrument());
        assert_eq!(from_pair.instrument().as_pair(), Some(&pair));
        assert_ne!(from_pair.id(), from_instrument.id());
    }

    #[test]
    fn feed_datatype_all_holds_each_variant_once() {
        let position = |datatype: FeedDatatype| match datatype {
            FeedDatatype::Orders => 0,
            FeedDatatype::Trades => 1,
            FeedDatatype::FundingRates => 2,
            FeedDatatype::OpenInterest => 3,
            FeedDatatype::Transactions => 4,
        };
        for (index, datatype) in FeedDatatype::ALL.into_iter().enumerate() {
            assert_eq!(position(datatype), index, "{datatype:?}");
        }
    }

    #[test]
    fn a_cex_feed_set_follows_the_declared_order() {
        use aetelier_types::config::markets::market_config::DataTypesSection;
        let section: DataTypesSection = toml::from_str(
            r#"
[open_interest]
enabled = true

[liquidations]
enabled = true

[trades]
enabled = true

[funding_rates]
enabled = true

[orderbook]
enabled = true
"#,
        )
        .unwrap();
        let pair = Instrument::from(TradingPair::new("BTC", "USDT"));
        let snapshot =
            FeedSet::for_declared(Exchange::Bybit, &pair, &section.declared_set())
                .snapshot();
        let datatypes: Vec<FeedDatatype> = snapshot.iter().map(|f| f.datatype).collect();
        assert_eq!(
            datatypes,
            [
                FeedDatatype::Orders,
                FeedDatatype::Trades,
                FeedDatatype::FundingRates,
                FeedDatatype::OpenInterest,
            ]
        );
        let by_name: Vec<FeedDatatype> = section
            .enabled_names()
            .iter()
            .filter_map(|name| {
                DeclaredDatatype::ALL.into_iter().find(|dt| dt.id() == name)
            })
            .filter_map(FeedDatatype::for_declared)
            .collect();
        assert_eq!(datatypes, by_name);
        assert!(
            snapshot
                .iter()
                .all(|f| f.instrument == "BTC/USDT" && f.state == FeedState::Requested)
        );
    }

    #[test]
    fn a_transactions_feed_is_built_for_an_address() {
        let address = Instrument::from(ChainAddress::solana(JUPITER_V6).unwrap());
        let fs = FeedSet::for_declared(
            Exchange::Solana,
            &address,
            &DeclaredSet::only(DeclaredDatatype::Transactions),
        );
        assert_eq!(fs.feeds.len(), 1);
        assert_eq!(fs.feeds[0].venue(), Exchange::Solana);
        assert_eq!(fs.feeds[0].instrument(), &address);
        let snapshot = fs.snapshot();
        assert_eq!(snapshot[0].datatype, FeedDatatype::Transactions);
        assert_eq!(snapshot[0].instrument, JUPITER_V6);
        assert_eq!(snapshot[0].state, FeedState::Requested);
        let feedless = FeedSet::for_declared(
            Exchange::Solana,
            &address,
            &DeclaredSet::only(DeclaredDatatype::Liquidations),
        );
        assert!(feedless.snapshot().is_empty());
    }
}
