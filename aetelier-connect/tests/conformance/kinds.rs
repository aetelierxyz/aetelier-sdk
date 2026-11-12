//! The conformance kinds — generic checks over a `VenueConformance`, each
//! reading the reconstruction model / codec / decode path from the adapter
//! registry so it cannot drift from code.
//!
//! Registered in `KINDS`; the `conformance_suite!` macro instantiates every
//! entry for every venue, so adding a kind here ratchets it to all venues.

use aetelier_connect::framework::budget::SourceMetricsSnapshot;
use aetelier_connect::framework::model::{
    DomainEvent, OrderBookState, ReconstructionModel, SourcedOrderbook,
};
use aetelier_connect::framework::registry::{ExchangeAdapter, ReplayFrame, registry};
use aetelier_connect::framework::runtime::{ReconstructedEvent, RuntimeOutcome};
use aetelier_types::Instrument;
use aetelier_types::config::markets::market_config::{
    DataTypesSection, DeclaredSet, TransactionsConfig,
};
use aetelier_types::onchain::Commitment;
use aetelier_types::orderbooks::NormalizedDelta;
use aetelier_types::trading_pair::TradingPair;
use rust_decimal::Decimal;

use super::harness::{
    CAPTURES_NOT_DISTRIBUTED, Class, ExpectInstrument, Kind, Outcome, StreamFixture,
    VenueConformance, fixture_lines, fixture_present, fixture_raw, stream_frames,
};

fn stateless_replay_applies(model: &ReconstructionModel) -> Result<(), Outcome> {
    match model {
        ReconstructionModel::FullRefresh
        | ReconstructionModel::SeqDelta { .. }
        | ReconstructionModel::ChecksumDelta { .. }
        | ReconstructionModel::L3 { .. } => Ok(()),
        ReconstructionModel::Stream => Err(Outcome::NotApplicable(
            "stream model: frames replay through replay_stream, not replay_frame".into(),
        )),
    }
}

/// Replay every fixture frame through the adapter's real decode+normalize
/// path; collect the DomainEvents. Errors surface the venue id + the frame.
fn replay_all(desc: &VenueConformance) -> Result<Vec<DomainEvent>, Outcome> {
    replay_fixture(*registry().get(desc.venue).unwrap(), desc.ws_fixture)
}

fn replay_fixture(
    adapter: &dyn ExchangeAdapter,
    ws_fixture: &str,
) -> Result<Vec<DomainEvent>, Outcome> {
    stateless_replay_applies(&adapter.book_model("orders"))?;
    let lines = fixture_lines(ws_fixture).map_err(Outcome::Fail)?;
    if lines.is_empty() {
        return Err(Outcome::Fail(format!("fixture {ws_fixture} has no frames")));
    }
    let mut events = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        match adapter.replay_frame(line) {
            Ok(mut evs) => events.append(&mut evs),
            Err(e) => {
                return Err(Outcome::Fail(format!("frame {i} failed to decode: {e}")));
            }
        }
    }
    Ok(events)
}

fn derivative_invariants(desc: &VenueConformance) -> Outcome {
    let wants_fr = desc.expect_classes.contains(&Class::FundingRate);
    let wants_oi = desc.expect_classes.contains(&Class::OpenInterest);
    if !wants_fr && !wants_oi {
        return Outcome::NotApplicable(
            "venue declares no derivative message classes".into(),
        );
    }
    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(o) => return o,
    };
    let mut fr_seen = 0usize;
    let mut oi_seen = 0usize;
    let one = Decimal::ONE;
    for ev in &events {
        match ev {
            DomainEvent::FundingRate(fr) => {
                fr_seen += 1;
                if fr.funding_rate.abs() >= one {
                    return Outcome::Fail(format!(
                        "funding rate {} outside (-1, 1)",
                        fr.funding_rate
                    ));
                }
                if fr.interval_hours == 0 {
                    return Outcome::Fail("funding interval_hours is 0".into());
                }
            }
            DomainEvent::OpenInterest(oi) => {
                oi_seen += 1;
                if oi.open_interest.is_sign_negative() {
                    return Outcome::Fail(format!(
                        "negative open interest {}",
                        oi.open_interest
                    ));
                }
                if let Some(px) = oi.mark_px
                    && px <= Decimal::ZERO
                {
                    return Outcome::Fail(format!("non-positive mark price {px}"));
                }
            }
            _ => {}
        }
    }
    if wants_fr && fr_seen == 0 {
        return Outcome::Fail(
            "FundingRate class declared but no samples in the fixture".into(),
        );
    }
    if wants_oi && oi_seen == 0 {
        return Outcome::Fail(
            "OpenInterest class declared but no samples in the fixture".into(),
        );
    }
    Outcome::Pass
}

fn class_of(ev: &DomainEvent) -> Option<Class> {
    match ev {
        DomainEvent::Book(_) => Some(Class::Book),
        DomainEvent::Trade { .. } => Some(Class::Trade),
        // Control signals (connection-gap etc.) are not a documented message
        // class — they never satisfy or violate an expect_classes entry.
        DomainEvent::ConnectionGap { .. } => None,
        DomainEvent::FundingRate(_) => Some(Class::FundingRate),
        DomainEvent::OpenInterest(_) => Some(Class::OpenInterest),
        DomainEvent::FundingSettlement(_) => Some(Class::FundingSettlement),
        DomainEvent::Transaction(_) => Some(Class::Transaction),
        DomainEvent::SlotTick(_)
        | DomainEvent::SlotGap(_)
        | DomainEvent::ConnectionLost { .. }
        | DomainEvent::ConnectionResumed => None,
    }
}

/// KIND: decode surface. Every fixture frame decodes without error, and every
/// message class the venue docs declare for our channels actually appears in
/// the fixture (no silently-missing class). Anchors DAT-CN decode + the
/// normalize boundary.
fn decode_surface(desc: &VenueConformance) -> Outcome {
    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(o) => return o,
    };
    if events.is_empty() {
        return Outcome::Fail("no DomainEvents produced from the fixture".into());
    }
    let seen: Vec<Class> = {
        let mut v = Vec::new();
        for ev in &events {
            let Some(c) = class_of(ev) else { continue };
            if !v.contains(&c) {
                v.push(c);
            }
        }
        v
    };
    for want in desc.expect_classes {
        if !seen.contains(want) {
            return Outcome::Fail(format!(
                "documented class {want:?} never appears in the fixture (saw {seen:?})"
            ));
        }
    }
    Outcome::Pass
}

/// KIND: seeding taxonomy. The adapter's declared model derives the seeding
/// need + source the venue docs specify. The single doc-vs-code check;
/// everything is read from the registry. Anchors DAT-RT seeding + the Book
/// family / SnapshotSource SSoT.
fn seeding_taxonomy(desc: &VenueConformance) -> Outcome {
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    if !model.reconstructs_books() {
        return Outcome::NotApplicable("stream model: no order book to seed".into());
    }
    if !desc.expect_source.matches(model.snapshot_source()) {
        return Outcome::Fail(format!(
            "snapshot source: doc expects {:?}, model has {:?}",
            desc.expect_source,
            model.snapshot_source()
        ));
    }
    if model.needs_rest() != desc.expect_needs_rest {
        return Outcome::Fail(format!(
            "needs_rest: doc expects {}, model derives {}",
            desc.expect_needs_rest,
            model.needs_rest()
        ));
    }
    // Coherence: a REST-seeded model must ship a seeder (the worker bails
    // otherwise). Read from the registry, not the descriptor.
    if model.needs_rest() && adapter.rest_seeder().is_none() {
        return Outcome::Fail(
            "REST-seeded model but adapter provides no rest_seeder".into(),
        );
    }
    let expect_out_of_band = matches!(
        model.snapshot_source(),
        Some(aetelier_connect::framework::model::SnapshotSource::RestSnapshot)
            | Some(aetelier_connect::framework::model::SnapshotSource::ReqOnSocket)
    );
    if model.seeds_out_of_band() != expect_out_of_band {
        return Outcome::Fail(format!(
            "seeds_out_of_band: source {:?} implies {}, model derives {}",
            model.snapshot_source(),
            expect_out_of_band,
            model.seeds_out_of_band()
        ));
    }
    if model.supports_in_band_reseed() && !model.seeds_out_of_band() {
        return Outcome::Fail(
            "in-band reseed declared on a model whose seed is not out-of-band".into(),
        );
    }
    if model.seq_is_connection_scoped() && model.replay_dedup_sound() {
        return Outcome::Fail(
            "connection-scoped sequence cannot make buffered-replay dedup sound".into(),
        );
    }
    if model.replay_dedup_sound() && model.snapshot_source().is_none() {
        return Outcome::Fail(
            "replay dedup declared sound on a model that never seeds".into(),
        );
    }
    Outcome::Pass
}

/// KIND: symbol canonicalization. The venue's wire symbol round-trips through
/// the profile codec to the documented canonical pair. Anchors venue
/// variation (symbol codec) + DAT-RT symbol decode.
fn symbol_canonicalization(desc: &VenueConformance) -> Outcome {
    let adapter = *registry().get(desc.venue).unwrap();
    let codec = &adapter.profile().symbol_codec;
    let want = match desc.expect_instrument {
        ExpectInstrument::Pair => {
            TradingPair::new(desc.canonical_base, desc.canonical_quote).to_canonical()
        }
        ExpectInstrument::Address(chain) => {
            if let Some(pair) = codec.decode(desc.wire_symbol) {
                return Outcome::Fail(format!(
                    "codec decoded the address {} to the pair {}",
                    desc.wire_symbol,
                    pair.to_canonical()
                ));
            }
            if let Some(Instrument::Address(address)) =
                codec.decode_instrument(desc.wire_symbol)
                && address.chain() != chain
            {
                return Outcome::Fail(format!(
                    "codec decoded {} on chain {:?}, doc expects {chain:?}",
                    desc.wire_symbol,
                    address.chain()
                ));
            }
            desc.wire_symbol.to_string()
        }
    };
    match codec.decode_instrument(desc.wire_symbol) {
        Some(found) if found.canonical() == want => Outcome::Pass,
        Some(found) => Outcome::Fail(format!(
            "codec decoded {} to {}, doc expects {}",
            desc.wire_symbol,
            found.canonical(),
            want
        )),
        None => Outcome::Fail(format!(
            "codec could not decode wire symbol {}",
            desc.wire_symbol
        )),
    }
}

/// KIND: trade continuity. Trades replay from the fixture, and where the venue
/// supplies a monotonic trade sequence, it is non-decreasing (the input to the
/// SourcedTradebook loss accounting). Skips if the fixture carries no trades
/// and the venue does not declare the Trade class. Anchors DAT-TB.
fn trade_continuity(desc: &VenueConformance) -> Outcome {
    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(o) => return o,
    };
    let seqs: Vec<u64> = events
        .iter()
        .filter_map(|ev| match ev {
            DomainEvent::Trade { sequence, .. } => *sequence,
            _ => None,
        })
        .collect();
    let trade_count = events
        .iter()
        .filter(|ev| matches!(ev, DomainEvent::Trade { .. }))
        .count();

    if !desc.expect_classes.contains(&Class::Trade) {
        return Outcome::Skip("Trade class not in this venue's fixture scope yet".into());
    }
    if trade_count == 0 {
        return Outcome::Fail("Trade class declared but no trades in the fixture".into());
    }
    if seqs.is_empty() {
        return Outcome::NotApplicable(
            "venue supplies no trade sequence (best-effort)".into(),
        );
    }
    for w in seqs.windows(2) {
        if w[1] < w[0] {
            return Outcome::Fail(format!(
                "trade sequence went backwards: {} then {}",
                w[0], w[1]
            ));
        }
    }
    Outcome::Pass
}

/// Reconstruct a venue's order book from its fixture — replay frames, seed per
/// the reconstruction model, apply deltas — returning the finished book plus the
/// count of applied deltas. On the way it errors as an `Outcome`:
/// `NotApplicable` when the venue declares no Book class, `Fail` on a
/// reconstruction gap. Shared by every book-level kind so they all certify the
/// SAME reconstruction rather than each rolling its own.
fn reconstruct_book(desc: &VenueConformance) -> Result<(SourcedOrderbook, u64), Outcome> {
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    let events = replay_all(desc)?;
    let deltas: Vec<_> = events
        .into_iter()
        .filter_map(|ev| match ev {
            DomainEvent::Book(d) => Some(d),
            _ => None,
        })
        .collect();
    if deltas.is_empty() {
        if desc.expect_classes.contains(&Class::Book) {
            return Err(Outcome::Fail(
                "Book class declared but no book deltas in the fixture".into(),
            ));
        }
        return Err(Outcome::NotApplicable(
            "venue declares no Book class".into(),
        ));
    }

    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let mut book =
        SourcedOrderbook::new(canonical.clone(), model.clone(), model.recovery_action());
    let mut applied = 0u64;

    // FullRefresh: every frame is a complete book — apply each in wire order,
    // no seed/delta distinction.
    if matches!(model, ReconstructionModel::FullRefresh) {
        for mut d in deltas {
            d.symbol = canonical.to_canonical();
            match book.apply(d) {
                Ok(_) => applied += 1,
                Err(e) => {
                    return Err(Outcome::Fail(format!(
                        "full-refresh frame rejected after {applied}: {}",
                        e.reason
                    )));
                }
            }
        }
    } else if matches!(
        model.snapshot_source(),
        Some(aetelier_connect::framework::model::SnapshotSource::ReqOnSocket)
    ) {
        let mut buffered: Vec<NormalizedDelta> = Vec::new();
        let mut seeded = false;
        for mut d in deltas {
            d.symbol = canonical.to_canonical();
            if !seeded {
                if d.is_snapshot {
                    let snap_id = d.update_id;
                    if let Err(e) = book.apply(d) {
                        return Err(Outcome::Fail(format!(
                            "REQ-seed apply failed: {}",
                            e.reason
                        )));
                    }
                    for bd in buffered.drain(..) {
                        if bd.update_id <= snap_id {
                            continue;
                        }
                        match book.apply(bd) {
                            Ok(_) => applied += 1,
                            Err(e) => {
                                return Err(Outcome::Fail(format!(
                                    "book gapped after {applied} clean applies replaying the buffer past the REQ seed: {}",
                                    e.reason
                                )));
                            }
                        }
                    }
                    seeded = true;
                } else {
                    buffered.push(d);
                }
            } else {
                match book.apply(d) {
                    Ok(_) => applied += 1,
                    Err(e) => {
                        return Err(Outcome::Fail(format!(
                            "book gapped after {applied} clean applies past the REQ seed: {}",
                            e.reason
                        )));
                    }
                }
            }
        }
        if !seeded {
            return Err(Outcome::Fail(
                "ReqOnSocket venue: no REQ-reply snapshot in the fixture".into(),
            ));
        }
    } else {
        // Incremental (SeqDelta / L3), snapshot-first or REST-seeded: apply in
        // wire order, discarding deltas the seed covers.
        let seed_update_id = if model.needs_rest() {
            let Some(rest_rel) = desc.rest_fixture else {
                return Err(Outcome::Fail(
                    "REST-seeded venue has no rest_fixture in the descriptor".into(),
                ));
            };
            let raw = fixture_raw(rest_rel).map_err(Outcome::Fail)?;
            let mut seed = match adapter.replay_seed(&raw, desc.wire_symbol) {
                Ok(Some(s)) => s,
                Ok(None) => {
                    return Err(Outcome::Fail(
                        "REST-model adapter returned no seed from the fixture".into(),
                    ));
                }
                Err(e) => return Err(Outcome::Fail(format!("seed parse failed: {e}"))),
            };
            seed.symbol = canonical.to_canonical();
            let id = seed.update_id;
            if let Err(e) = book.apply(seed) {
                return Err(Outcome::Fail(format!("seed apply failed: {}", e.reason)));
            }
            Some(id)
        } else {
            None
        };

        let mut started = seed_update_id.is_some();
        for mut d in deltas {
            if let Some(seed_id) = seed_update_id
                && d.update_id <= seed_id
            {
                continue;
            }
            if !started {
                if d.is_snapshot {
                    started = true;
                } else {
                    continue;
                }
            }
            d.symbol = canonical.to_canonical();
            match book.apply(d) {
                Ok(_) => applied += 1,
                Err(e) => {
                    return Err(Outcome::Fail(format!(
                        "book gapped after {applied} clean applies: {}",
                        e.reason
                    )));
                }
            }
        }
    }
    if applied == 0 {
        return Err(Outcome::Fail("no book deltas applied".into()));
    }
    Ok((book, applied))
}

/// KIND: book delta application. The fixture's book deltas apply in order
/// (self-seeded, REST-seeded, or in-band-REQ-seeded per the model) without a
/// spurious gap, and the reconstructed book is non-degenerate — it reaches
/// `Synced` and is non-empty. Anchors DAT-OB apply + OrderBookState.
fn book_delta_application(desc: &VenueConformance) -> Outcome {
    let (book, _applied) = match reconstruct_book(desc) {
        Ok(x) => x,
        Err(outcome) => return outcome,
    };
    if book.state() != OrderBookState::Synced {
        return Outcome::Fail(format!(
            "book not Synced after replay: {:?}",
            book.state()
        ));
    }
    if book.book().bid_depth() == 0 && book.book().ask_depth() == 0 {
        return Outcome::Fail("reconstructed book is empty".into());
    }
    Outcome::Pass
}

/// KIND: book invariants. An INDEPENDENT oracle — it walks the reconstructed L2
/// book and asserts physical/logical truths that no `SeqPredicate` checks, so a
/// systematic apply error (wrong side, wrong decimal scale, a zero level) that
/// still reaches `Synced` is caught here where `book_delta_application` — which
/// only asks the code's own state machine — cannot see it. Asserts: the book is
/// not crossed (best_bid < best_ask), every level carries a strictly-positive
/// price and size, and levels are ordered (bids descending, asks ascending).
/// Anchors DAT-OB-INV-16.
fn book_invariants(desc: &VenueConformance) -> Outcome {
    let (book, _) = match reconstruct_book(desc) {
        Ok(x) => x,
        Err(outcome) => return outcome,
    };
    let ob = book.book();

    // Not crossed: the best bid must not exceed the best ask.
    if let (Some((bid, _)), Some((ask, _))) = (ob.best_bid(), ob.best_ask())
        && bid > ask
    {
        return Outcome::Fail(format!("crossed book: best_bid {bid} > best_ask {ask}"));
    }

    // Bids: strictly-positive levels, strictly descending in price.
    let bids = ob.top_bids(ob.bid_depth());
    for (p, s) in &bids {
        if *p <= Decimal::ZERO || *s <= Decimal::ZERO {
            return Outcome::Fail(format!("non-positive bid level: price {p}, size {s}"));
        }
    }
    for w in bids.windows(2) {
        if w[0].0 <= w[1].0 {
            return Outcome::Fail(format!(
                "bids not strictly descending: {} then {}",
                w[0].0, w[1].0
            ));
        }
    }

    // Asks: strictly-positive levels, strictly ascending in price.
    let asks = ob.top_asks(ob.ask_depth());
    for (p, s) in &asks {
        if *p <= Decimal::ZERO || *s <= Decimal::ZERO {
            return Outcome::Fail(format!("non-positive ask level: price {p}, size {s}"));
        }
    }
    for w in asks.windows(2) {
        if w[0].0 >= w[1].0 {
            return Outcome::Fail(format!(
                "asks not strictly ascending: {} then {}",
                w[0].0, w[1].0
            ));
        }
    }

    Outcome::Pass
}

/// KIND: trade invariants. An INDEPENDENT oracle on the replayed trades: every
/// trade carries a strictly-positive price and amount, and a microsecond-scale
/// venue timestamp (guards the µs unit contract at trade scope — a seconds/ms
/// value falls below the 2020 floor, a nanosecond value above the 2100 ceiling).
/// The taker side is a real enum by construction (unknown sides are dropped at
/// decode), so nothing to assert there. Anchors DAT-TB-INV-12.
fn trade_invariants(desc: &VenueConformance) -> Outcome {
    // UTC epoch microseconds for 2020-01-01 and 2100-01-01 — any real market
    // timestamp expressed in µs lies between them; other units do not.
    const US_FLOOR_2020: u64 = 1_577_836_800_000_000;
    const US_CEIL_2100: u64 = 4_102_444_800_000_000;

    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(o) => return o,
    };
    let mut seen = 0u64;
    for ev in &events {
        let DomainEvent::Trade { trade, .. } = ev else {
            continue;
        };
        seen += 1;
        if trade.price <= Decimal::ZERO || trade.amount <= Decimal::ZERO {
            return Outcome::Fail(format!(
                "non-positive trade: price {}, amount {}",
                trade.price, trade.amount
            ));
        }
        let ts = trade.source_trade_ts_us;
        if !(US_FLOOR_2020..US_CEIL_2100).contains(&ts) {
            return Outcome::Fail(format!(
                "trade timestamp {ts} is not microsecond-scale (outside [2020, 2100) µs)"
            ));
        }
    }
    if seen == 0 {
        return Outcome::NotApplicable("no trades in the fixture".into());
    }
    Outcome::Pass
}

/// KIND: checksum validation. For a ChecksumDelta venue, the CRC32 checksum is
/// the ONLY continuity guarantee. This kind certifies it end-to-end: every
/// book DELTA in the fixture carries a checksum (absence would fail closed per
/// the fail-closed rule), and replaying the fixture through the real ChecksumDelta apply
/// path validates every one (the book stays Synced, so at least one live CRC32
/// matched the reconstructed book). Non-checksum venues skip. Anchors DAT-OB
/// ChecksumDelta + the fail-closed rule.
fn checksum_validation(desc: &VenueConformance) -> Outcome {
    use aetelier_connect::framework::model::ReconstructionModel;
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    if !matches!(model, ReconstructionModel::ChecksumDelta { .. }) {
        return Outcome::NotApplicable("not a checksum-delta venue".into());
    }
    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(o) => return o,
    };
    let deltas: Vec<_> = events
        .into_iter()
        .filter_map(|ev| match ev {
            DomainEvent::Book(d) => Some(d),
            _ => None,
        })
        .collect();
    if deltas.is_empty() {
        return Outcome::Fail("checksum venue but no book deltas in the fixture".into());
    }

    // Every non-snapshot delta must carry a checksum — a checksum venue that
    // omits one would fail closed, so the fixture must not.
    let mut checksummed = 0u64;
    for d in &deltas {
        if !d.is_snapshot {
            match d.checksum {
                Some(_) => checksummed += 1,
                None => {
                    return Outcome::Fail(
                        "a book delta carries no checksum (would fail closed)".into(),
                    );
                }
            }
        }
    }
    if checksummed == 0 {
        return Outcome::Fail("fixture exercises no delta checksum".into());
    }

    // Replay through the real ChecksumDelta apply: each validated frame either
    // matches (book stays Synced) or gaps on a CRC32 mismatch (Err). A clean
    // Synced end means every live checksum validated the reconstruction.
    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let mut book =
        SourcedOrderbook::new(canonical.clone(), model.clone(), model.recovery_action());
    let mut started = false;
    for mut d in deltas {
        if !started {
            if d.is_snapshot {
                started = true;
            } else {
                continue;
            }
        }
        d.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(d) {
            return Outcome::Fail(format!("checksum validation gapped: {}", e.reason));
        }
    }
    if book.state() != OrderBookState::Synced {
        return Outcome::Fail(format!(
            "book not Synced after checksum replay: {:?}",
            book.state()
        ));
    }
    Outcome::Pass
}

/// Seed a venue's book (per its model) and return the seeded book plus the
/// ORDERED post-seed non-snapshot deltas that would apply next. The building
/// block for `gap_detection`: it needs a clean Synced baseline plus the
/// contiguous delta run to inject a drop into. Only the sequence-seeded models
/// reach here (`gap_detection` screens out FullRefresh / L3 / Monotonic /
/// Checksum first), so it handles ReqOnSocket, REST-seeded, and self-seeded
/// snapshots. Deltas are returned in seqNum order (ReqOnSocket wire order can
/// differ) and filtered to those strictly past the seed.
fn seeded_book_and_deltas(
    desc: &VenueConformance,
) -> Result<(SourcedOrderbook, Vec<NormalizedDelta>), Outcome> {
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    let events = replay_all(desc)?;
    let deltas: Vec<NormalizedDelta> = events
        .into_iter()
        .filter_map(|ev| match ev {
            DomainEvent::Book(d) => Some(d),
            _ => None,
        })
        .collect();
    if deltas.is_empty() {
        return Err(Outcome::NotApplicable(
            "no book deltas in the fixture".into(),
        ));
    }
    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let mut book =
        SourcedOrderbook::new(canonical.clone(), model.clone(), model.recovery_action());

    let seed_id = if matches!(
        model.snapshot_source(),
        Some(aetelier_connect::framework::model::SnapshotSource::ReqOnSocket)
    ) {
        let Some(seed_pos) = deltas.iter().rposition(|d| d.is_snapshot) else {
            return Err(Outcome::Fail(
                "ReqOnSocket venue: no REQ-reply snapshot".into(),
            ));
        };
        let mut seed = deltas[seed_pos].clone();
        let id = seed.update_id;
        seed.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(seed) {
            return Err(Outcome::Fail(format!(
                "REQ-seed apply failed: {}",
                e.reason
            )));
        }
        id
    } else if model.needs_rest() {
        let Some(rest_rel) = desc.rest_fixture else {
            return Err(Outcome::Fail(
                "REST-seeded venue has no rest_fixture".into(),
            ));
        };
        let raw = fixture_raw(rest_rel).map_err(Outcome::Fail)?;
        let mut seed = match adapter.replay_seed(&raw, desc.wire_symbol) {
            Ok(Some(s)) => s,
            Ok(None) => {
                return Err(Outcome::Fail("no seed from the REST fixture".into()));
            }
            Err(e) => return Err(Outcome::Fail(format!("seed parse failed: {e}"))),
        };
        let id = seed.update_id;
        seed.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(seed) {
            return Err(Outcome::Fail(format!("seed apply failed: {}", e.reason)));
        }
        id
    } else {
        let Some(seed_pos) = deltas.iter().position(|d| d.is_snapshot) else {
            return Err(Outcome::Fail("self-seed venue: no snapshot frame".into()));
        };
        let mut seed = deltas[seed_pos].clone();
        let id = seed.update_id;
        seed.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(seed) {
            return Err(Outcome::Fail(format!("seed apply failed: {}", e.reason)));
        }
        id
    };

    let mut post: Vec<NormalizedDelta> = deltas
        .into_iter()
        .filter(|d| !d.is_snapshot && d.update_id > seed_id)
        .collect();
    post.sort_by_key(|d| d.update_id);
    Ok((book, post))
}

/// KIND: gap detection (negative path). A DROPPED delta must be DETECTED — the
/// book transitions to Gapped (DAT-OB-T-4), never silently stays Synced. Seeds a
/// clean baseline from the real stream, drops one delta mid-run, and asserts the
/// next apply gaps. `book_delta_application` proving the run reconstructs clean
/// is the precondition: a contiguous run means any single drop MUST break
/// continuity. Only sequence-continuity models can detect a mid-stream drop;
/// FullRefresh (full books), L3 (order-keyed, no sequence), and Monotonic
/// (connection counter — DAT-OB-INV-10) structurally cannot, and ChecksumDelta
/// drop-detection is already certified by `checksum_validation` — so all four
/// are NotApplicable. Anchors DAT-OB-T-4 + DAT-OB-INV-9.
fn gap_detection(desc: &VenueConformance) -> Outcome {
    use aetelier_connect::framework::model::SeqPredicate;
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    match &model {
        ReconstructionModel::FullRefresh => {
            return Outcome::NotApplicable(
                "FullRefresh: every frame is a full book — a dropped delta is invisible"
                    .into(),
            );
        }
        ReconstructionModel::L3 { .. } => {
            return Outcome::NotApplicable(
                "L3 order-keyed: no sequence continuity to break".into(),
            );
        }
        ReconstructionModel::ChecksumDelta { .. } => {
            return Outcome::NotApplicable(
                "checksum venue: drop detection is certified by checksum_validation"
                    .into(),
            );
        }
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::Monotonic,
            ..
        } => {
            return Outcome::NotApplicable(
                "Monotonic connection-counter cannot prove per-book contiguity (DAT-OB-INV-10)"
                    .into(),
            );
        }
        ReconstructionModel::SeqDelta { .. } => {}
        ReconstructionModel::Stream => {
            return Outcome::NotApplicable("stream model: no order book to gap".into());
        }
    }

    let (mut book, post) = match seeded_book_and_deltas(desc) {
        Ok(x) => x,
        Err(outcome) => return outcome,
    };
    if post.len() < 3 {
        return Outcome::NotApplicable(
            "too few post-seed deltas to inject a drop".into(),
        );
    }
    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let k = post.len() / 2;

    // Establish a Synced baseline: apply the contiguous run up to the drop
    // point. book_delta_application already proves this run reconstructs clean,
    // so a gap here means the run is not a single contiguous stream (re-snapshot).
    for d in &post[..k] {
        let mut d = d.clone();
        d.symbol = canonical.to_canonical();
        if book.apply(d).is_err() {
            return Outcome::NotApplicable(
                "post-seed run is not a single contiguous delta stream (re-snapshot?)"
                    .into(),
            );
        }
    }
    if book.state() != OrderBookState::Synced {
        return Outcome::NotApplicable("baseline not Synced before injection".into());
    }

    // DROP post[k]; apply post[k+1]. Its continuity pointer references the
    // dropped delta, so a continuity-tracking model MUST gap here.
    let mut injected = post[k + 1].clone();
    injected.symbol = canonical.to_canonical();
    match book.apply(injected) {
        Err(_) if book.state() == OrderBookState::Gapped => Outcome::Pass,
        Err(_) => Outcome::Fail(format!(
            "drop raised an error but state is {:?}, not Gapped",
            book.state()
        )),
        Ok(_) => Outcome::Fail(
            "a dropped delta was NOT detected: the book stayed Synced after skipping a delta".into(),
        ),
    }
}

fn book_runtime_replay(desc: &VenueConformance) -> Outcome {
    use aetelier_connect::framework::budget::SourceMetrics;
    use aetelier_connect::framework::rest::RestSnapshot;
    use aetelier_connect::framework::runtime::{
        ReconstructedEvent, RuntimeOutcome, SourceRuntime,
    };

    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(o) => return o,
    };
    if !events.iter().any(|ev| matches!(ev, DomainEvent::Book(_))) {
        if desc.expect_classes.contains(&Class::Book) {
            return Outcome::Fail(
                "Book class declared but no book deltas in the fixture".into(),
            );
        }
        return Outcome::NotApplicable("venue declares no Book class".into());
    }

    struct FixedSeeder(NormalizedDelta);

    #[async_trait::async_trait]
    impl RestSnapshot for FixedSeeder {
        async fn fetch_snapshot(
            &self,
            _symbol: &str,
        ) -> Result<NormalizedDelta, aetelier_connect::errors::ExchangeError> {
            Ok(self.0.clone())
        }
    }

    let seeder: Option<std::sync::Arc<dyn RestSnapshot>> = if model.needs_rest() {
        let Some(rest_rel) = desc.rest_fixture else {
            return Outcome::Fail(
                "REST-seeded venue has no rest_fixture in the descriptor".into(),
            );
        };
        let raw = match fixture_raw(rest_rel) {
            Ok(r) => r,
            Err(e) => return Outcome::Fail(e),
        };
        match adapter.replay_seed(&raw, desc.wire_symbol) {
            Ok(Some(s)) => Some(std::sync::Arc::new(FixedSeeder(s))),
            Ok(None) => {
                return Outcome::Fail(
                    "REST-model adapter returned no seed from the fixture".into(),
                );
            }
            Err(e) => return Outcome::Fail(format!("seed parse failed: {e}")),
        }
    } else {
        None
    };

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return Outcome::Fail(format!("tokio runtime: {e}")),
    };
    rt.block_on(async move {
        let metrics = SourceMetrics::default();
        let runtime = SourceRuntime::new(
            desc.venue,
            adapter.profile().symbol_codec.clone(),
            vec![desc.wire_symbol.to_string()],
            model.clone(),
            model.recovery_action(),
            metrics.clone(),
            aetelier_connect::framework::protocol::DeclaredSet::all(),
        );
        let (ev_tx, ev_rx) = tokio::sync::mpsc::channel(events.len() + 1);
        for ev in events {
            if ev_tx.send(ev).await.is_err() {
                return Outcome::Fail("runtime event channel closed early".into());
            }
        }
        drop(ev_tx);
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(4096);
        let (_sd_tx, sd_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn(runtime.run(ev_rx, seeder, out_tx, sd_rx));
        let mut last_book = None;
        let mut books = 0u64;
        while let Some(ev) = out_rx.recv().await {
            if let ReconstructedEvent::Book { book, .. } = ev {
                books += 1;
                last_book = Some(book);
            }
        }
        let outcome = match run.await {
            Ok(o) => o,
            Err(e) => return Outcome::Fail(format!("runtime task panicked: {e}")),
        };
        if !matches!(outcome, RuntimeOutcome::Finished) {
            return Outcome::Fail(format!(
                "real runtime ended {outcome:?} on the committed capture — the fixture must replay gap-free"
            ));
        }
        let m = metrics.snapshot();
        if m.gaps != 0 || m.resyncs != 0 {
            return Outcome::Fail(format!(
                "real runtime counted gaps={} resyncs={} replaying the committed capture",
                m.gaps, m.resyncs
            ));
        }
        if books == 0 {
            return Outcome::Fail("real runtime emitted no reconstructed books".into());
        }
        match last_book {
            Some(b) if !b.bids.is_empty() || !b.asks.is_empty() => Outcome::Pass,
            Some(_) => Outcome::Fail("final reconstructed book is empty".into()),
            None => Outcome::Fail("no final book".into()),
        }
    })
}

fn canon_frame(raw: &str) -> Option<serde_json::Value> {
    fn strip(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(m) => {
                m.remove("time");
                for x in m.values_mut() {
                    strip(x);
                }
            }
            serde_json::Value::Array(a) => {
                for x in a.iter_mut() {
                    strip(x);
                }
            }
            _ => {}
        }
    }
    let mut v: serde_json::Value = serde_json::from_str(raw).ok()?;
    strip(&mut v);
    Some(v)
}

fn run_fixture_runtime(
    desc: &VenueConformance,
    declared: aetelier_connect::framework::protocol::DeclaredSet,
    feeds: Option<aetelier_connect::framework::feed::SharedFeedSet>,
) -> Result<
    (
        u64,
        u64,
        aetelier_connect::framework::runtime::RuntimeOutcome,
    ),
    Outcome,
> {
    use aetelier_connect::framework::budget::SourceMetrics;
    use aetelier_connect::framework::rest::RestSnapshot;
    use aetelier_connect::framework::runtime::{ReconstructedEvent, SourceRuntime};

    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    let events = replay_all(desc)?;

    struct FixedSeeder2(NormalizedDelta);

    #[async_trait::async_trait]
    impl RestSnapshot for FixedSeeder2 {
        async fn fetch_snapshot(
            &self,
            _symbol: &str,
        ) -> Result<NormalizedDelta, aetelier_connect::errors::ExchangeError> {
            Ok(self.0.clone())
        }
    }

    let seeder: Option<std::sync::Arc<dyn RestSnapshot>> = match desc.rest_fixture {
        Some(rest_rel) if model.needs_rest() => {
            let raw = fixture_raw(rest_rel).map_err(Outcome::Fail)?;
            match adapter.replay_seed(&raw, desc.wire_symbol) {
                Ok(Some(seed)) => Some(std::sync::Arc::new(FixedSeeder2(seed))),
                Ok(None) => None,
                Err(e) => return Err(Outcome::Fail(format!("seed parse failed: {e}"))),
            }
        }
        _ => None,
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|e| Outcome::Fail(format!("tokio runtime: {e}")))?;
    rt.block_on(async move {
        let metrics = SourceMetrics::default();
        let runtime = SourceRuntime::new(
            desc.venue,
            adapter.profile().symbol_codec.clone(),
            vec![desc.wire_symbol.to_string()],
            model.clone(),
            model.recovery_action(),
            metrics.clone(),
            declared,
        );
        let runtime = match feeds {
            Some(feeds) => runtime.with_feeds(feeds),
            None => runtime,
        };
        let (ev_tx, ev_rx) = tokio::sync::mpsc::channel(events.len() + 1);
        for ev in events {
            if ev_tx.send(ev).await.is_err() {
                return Err(Outcome::Fail("runtime event channel closed early".into()));
            }
        }
        drop(ev_tx);
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(4096);
        let (_sd_tx, sd_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn(runtime.run(ev_rx, seeder, out_tx, sd_rx));
        let mut books = 0u64;
        let mut trades = 0u64;
        while let Some(ev) = out_rx.recv().await {
            match ev {
                ReconstructedEvent::Book { .. } => books += 1,
                ReconstructedEvent::Trade(_) => trades += 1,
                _ => {}
            }
        }
        let outcome = run
            .await
            .map_err(|e| Outcome::Fail(format!("runtime task panicked: {e}")))?;
        Ok((books, trades, outcome))
    })
}

pub fn stream_fixtures(
    desc: &VenueConformance,
) -> Result<&'static [StreamFixture], Outcome> {
    let adapter = *registry().get(desc.venue).unwrap();
    if !matches!(adapter.book_model("orders"), ReconstructionModel::Stream) {
        return Err(Outcome::NotApplicable(
            "book venue: no on-chain stream to replay".into(),
        ));
    }
    if desc.stream_fixtures.is_empty() {
        return Err(Outcome::Fail(
            "stream venue declares no stream fixture".into(),
        ));
    }
    if !desc
        .stream_fixtures
        .iter()
        .any(|fixture| fixture_present(fixture.file))
    {
        return Err(Outcome::NotApplicable(format!(
            "the {} {CAPTURES_NOT_DISTRIBUTED}: place them under datasets/{}/ to replay them",
            desc.venue, desc.venue
        )));
    }
    Ok(desc.stream_fixtures)
}

fn stream_datatypes(transactions: TransactionsConfig) -> DataTypesSection {
    DataTypesSection {
        transactions: TransactionsConfig {
            enabled: true,
            ..transactions
        },
        ..DataTypesSection::default()
    }
}

pub fn replay_stream_frames(
    desc: &VenueConformance,
    symbols: &[String],
    commitment: Commitment,
    frames: &[ReplayFrame],
    transactions: TransactionsConfig,
) -> Result<(Vec<DomainEvent>, SourceMetricsSnapshot), Outcome> {
    use aetelier_connect::framework::budget::SourceMetrics;
    let adapter = *registry().get(desc.venue).unwrap();
    let metrics = SourceMetrics::default();
    let events = adapter
        .replay_stream(
            symbols,
            &stream_datatypes(transactions),
            commitment,
            frames,
            &metrics,
        )
        .map_err(|e| Outcome::Fail(format!("stream replay failed: {e}")))?;
    Ok((events, metrics.snapshot()))
}

fn replay_stream_fixture(
    desc: &VenueConformance,
    fixture: &StreamFixture,
    transactions: TransactionsConfig,
) -> Result<(Vec<DomainEvent>, SourceMetricsSnapshot), Outcome> {
    let frames = stream_frames(fixture.file).map_err(Outcome::Fail)?;
    replay_stream_frames(
        desc,
        &[fixture.watched.to_string()],
        fixture.commitment,
        &frames,
        transactions,
    )
    .map_err(|outcome| match outcome {
        Outcome::Fail(reason) => Outcome::Fail(format!("{}: {reason}", fixture.file)),
        other => other,
    })
}

pub fn run_stream_runtime(
    desc: &VenueConformance,
    symbols: Vec<String>,
    events: Vec<DomainEvent>,
    declared: DeclaredSet,
    feeds: Option<aetelier_connect::framework::feed::SharedFeedSet>,
    mut on_event: impl FnMut(ReconstructedEvent),
) -> Result<(RuntimeOutcome, SourceMetricsSnapshot), Outcome> {
    use aetelier_connect::framework::budget::SourceMetrics;
    use aetelier_connect::framework::runtime::SourceRuntime;

    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|e| Outcome::Fail(format!("tokio runtime: {e}")))?;
    rt.block_on(async move {
        let metrics = SourceMetrics::default();
        let runtime = SourceRuntime::new(
            desc.venue,
            adapter.profile().symbol_codec.clone(),
            symbols,
            model.clone(),
            model.recovery_action(),
            metrics.clone(),
            declared,
        );
        let runtime = match feeds {
            Some(feeds) => runtime.with_feeds(feeds),
            None => runtime,
        };
        let (ev_tx, ev_rx) = tokio::sync::mpsc::channel(events.len() + 1);
        for ev in events {
            if ev_tx.send(ev).await.is_err() {
                return Err(Outcome::Fail("runtime event channel closed early".into()));
            }
        }
        drop(ev_tx);
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel(4096);
        let (_sd_tx, sd_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn(runtime.run(ev_rx, None, out_tx, sd_rx));
        while let Some(ev) = out_rx.recv().await {
            on_event(ev);
        }
        let outcome = run
            .await
            .map_err(|e| Outcome::Fail(format!("runtime task panicked: {e}")))?;
        Ok((outcome, metrics.snapshot()))
    })
}

fn stream_feed_lifecycle(
    desc: &VenueConformance,
    venue: aetelier_types::exchanges::Exchange,
    declared: &DeclaredSet,
) -> Outcome {
    use aetelier_connect::framework::feed::{FeedSet, FeedState};

    let fixtures = match stream_fixtures(desc) {
        Ok(f) => f,
        Err(o) => return o,
    };
    let adapter = *registry().get(desc.venue).unwrap();
    for fixture in fixtures {
        let Some(instrument) = adapter
            .profile()
            .symbol_codec
            .decode_instrument(fixture.watched)
        else {
            return Outcome::Fail(format!(
                "{}: codec could not decode the watched address {}",
                fixture.file, fixture.watched
            ));
        };
        let feeds = FeedSet::for_declared(venue, &instrument, declared).into_shared();
        let expected = match feeds.lock() {
            Ok(mut fs) => {
                fs.mark_all_subscribing();
                fs.snapshot().len()
            }
            Err(e) => return Outcome::Fail(format!("feed set lock poisoned: {e}")),
        };
        if expected == 0 {
            return Outcome::Fail("the declared classes build no feed".into());
        }
        let events =
            match replay_stream_fixture(desc, fixture, TransactionsConfig::default()) {
                Ok((events, _)) => events,
                Err(o) => return o,
            };
        match run_stream_runtime(
            desc,
            vec![fixture.watched.to_string()],
            events,
            declared.clone(),
            Some(feeds.clone()),
            |_| {},
        ) {
            Ok((RuntimeOutcome::Finished, _)) => {}
            Ok((outcome, _)) => {
                return Outcome::Fail(format!(
                    "{}: runtime ended {outcome:?} replaying the committed capture",
                    fixture.file
                ));
            }
            Err(o) => return o,
        }
        let snapshot = match feeds.lock() {
            Ok(fs) => fs.snapshot(),
            Err(e) => return Outcome::Fail(format!("feed set lock poisoned: {e}")),
        };
        let canonical = instrument.canonical();
        for feed in &snapshot {
            if feed.state != FeedState::Live {
                return Outcome::Fail(format!(
                    "{}: {:?} feed ended {:?}, not Live",
                    fixture.file, feed.datatype, feed.state
                ));
            }
            if feed.instrument != canonical {
                return Outcome::Fail(format!(
                    "{}: {:?} feed reports instrument {}, decoded {canonical}",
                    fixture.file, feed.datatype, feed.instrument
                ));
            }
        }
    }
    Outcome::Pass
}

fn datatype_isolation(desc: &VenueConformance) -> Outcome {
    use aetelier_connect::framework::protocol::{DeclaredDatatype, DeclaredSet};
    if registry()
        .get(desc.venue)
        .is_some_and(|adapter| adapter.supported_datatypes().len() < 2)
    {
        return Outcome::NotApplicable(
            "the venue supports a single datatype: nothing to isolate".into(),
        );
    }
    if !desc.datatype_isolation_done {
        return Outcome::NotApplicable(
            "declared-datatype subscribe filtering (C1) not yet enforced for this venue"
                .into(),
        );
    }
    let adapter = *registry().get(desc.venue).unwrap();
    let symbols = vec![desc.wire_symbol.to_string()];
    let canon_all = |frames: Vec<String>| -> Result<Vec<serde_json::Value>, Outcome> {
        frames
            .into_iter()
            .map(|f| {
                canon_frame(&f).ok_or_else(|| {
                    Outcome::Fail(format!("unparseable subscribe frame: {f}"))
                })
            })
            .collect()
    };

    let full = match canon_all(
        adapter.subscribe_frames_preview(&symbols, &DeclaredSet::all()),
    ) {
        Ok(v) => v,
        Err(o) => return o,
    };
    if full.is_empty() {
        return Outcome::Fail(
            "subscribe_frames_preview returned nothing for the full declared set".into(),
        );
    }

    let mut only_sets: Vec<(DeclaredDatatype, Vec<serde_json::Value>)> = Vec::new();
    for dt in DeclaredDatatype::ALL {
        let only = match canon_all(
            adapter.subscribe_frames_preview(&symbols, &DeclaredSet::only(dt)),
        ) {
            Ok(v) => v,
            Err(o) => return o,
        };
        only_sets.push((dt, only));
    }
    let mut supported = Vec::new();
    for (dt, only) in &only_sets {
        let without = match canon_all(
            adapter.subscribe_frames_preview(&symbols, &DeclaredSet::all().without(*dt)),
        ) {
            Ok(v) => v,
            Err(o) => return o,
        };
        for f in only {
            // A frame another declared datatype also subscribes is SHARED
            // (Hyperliquid activeAssetCtx serves funding + open interest); it
            // legitimately survives removal of one sharer and is checked below
            // against removal of ALL sharers.
            let shared = only_sets.iter().any(|(e, set)| e != dt && set.contains(f));
            if !shared && without.contains(f) {
                return Outcome::Fail(format!(
                    "a frame exclusive to {dt:?} is still subscribed when {dt:?} is disabled: {f}"
                ));
            }
        }
        if !only.is_empty() {
            supported.push(*dt);
        }
    }
    for f in &full {
        let sharers: Vec<DeclaredDatatype> = only_sets
            .iter()
            .filter(|(_, set)| set.contains(f))
            .map(|(dt, _)| *dt)
            .collect();
        if sharers.is_empty() {
            continue;
        }
        let mut removed = DeclaredSet::all();
        for s in &sharers {
            removed = removed.without(*s);
        }
        let remaining =
            match canon_all(adapter.subscribe_frames_preview(&symbols, &removed)) {
                Ok(v) => v,
                Err(o) => return o,
            };
        if remaining.contains(f) {
            return Outcome::Fail(format!(
                "frame survives removal of every datatype that subscribes it \
                 ({sharers:?}): {f}"
            ));
        }
    }
    for must in [DeclaredDatatype::Orderbook, DeclaredDatatype::Trades] {
        if !supported.contains(&must) {
            return Outcome::Fail(format!(
                "venue must expose a {must:?}-only subscription, preview produced none"
            ));
        }
    }

    match run_fixture_runtime(desc, DeclaredSet::only(DeclaredDatatype::Orderbook), None)
    {
        Ok((_books, 0, _)) => {}
        Ok((_, trades, _)) => {
            return Outcome::Fail(format!(
                "orderbook-only runtime still emitted {trades} trades"
            ));
        }
        Err(o) => return o,
    }
    match run_fixture_runtime(desc, DeclaredSet::only(DeclaredDatatype::Trades), None) {
        Ok((books, _trades, outcome)) => {
            if books != 0 {
                return Outcome::Fail(format!(
                    "trades-only runtime still emitted {books} books"
                ));
            }
            if !matches!(
                outcome,
                aetelier_connect::framework::runtime::RuntimeOutcome::Finished
            ) {
                return Outcome::Fail(format!(
                    "trades-only runtime must finish clean (no seed machinery), got {outcome:?}"
                ));
            }
        }
        Err(o) => return o,
    }
    Outcome::Pass
}

fn feed_lifecycle(desc: &VenueConformance) -> Outcome {
    use aetelier_connect::framework::feed::{FeedSet, FeedState};
    use aetelier_connect::framework::protocol::{DeclaredDatatype, DeclaredSet};
    use aetelier_connect::framework::runtime::RuntimeOutcome;
    use aetelier_types::exchanges::Exchange;

    let adapter = *registry().get(desc.venue).unwrap();
    let Ok(venue) = desc.venue.parse::<Exchange>() else {
        return Outcome::Fail(format!("venue id {} names no exchange", desc.venue));
    };
    let declared = DeclaredDatatype::ALL
        .into_iter()
        .filter(|dt| {
            let class = match dt {
                DeclaredDatatype::Orderbook => Some(Class::Book),
                DeclaredDatatype::Trades => Some(Class::Trade),
                DeclaredDatatype::FundingRates => Some(Class::FundingRate),
                DeclaredDatatype::OpenInterest => Some(Class::OpenInterest),
                DeclaredDatatype::Transactions => Some(Class::Transaction),
                DeclaredDatatype::Liquidations => None,
            };
            !class.is_some_and(|c| desc.expect_classes.contains(&c))
        })
        .fold(DeclaredSet::all(), |set, dt| set.without(dt));
    if matches!(adapter.book_model("orders"), ReconstructionModel::Stream) {
        return stream_feed_lifecycle(desc, venue, &declared);
    }
    let Some(instrument) = adapter
        .profile()
        .symbol_codec
        .decode_instrument(desc.wire_symbol)
    else {
        return Outcome::Fail(format!(
            "codec could not decode wire symbol {}",
            desc.wire_symbol
        ));
    };
    let feeds = FeedSet::for_declared(venue, &instrument, &declared).into_shared();
    let expected = match feeds.lock() {
        Ok(mut fs) => {
            fs.mark_all_subscribing();
            fs.snapshot().len()
        }
        Err(e) => return Outcome::Fail(format!("feed set lock poisoned: {e}")),
    };
    if expected == 0 {
        return Outcome::Fail("the declared classes build no feed".into());
    }
    match run_fixture_runtime(desc, declared, Some(feeds.clone())) {
        Ok((_, _, RuntimeOutcome::Finished)) => {}
        Ok((_, _, outcome)) => {
            return Outcome::Fail(format!(
                "runtime ended {outcome:?} replaying the committed capture"
            ));
        }
        Err(o) => return o,
    }
    let snapshot = match feeds.lock() {
        Ok(fs) => fs.snapshot(),
        Err(e) => return Outcome::Fail(format!("feed set lock poisoned: {e}")),
    };
    let canonical = instrument.canonical();
    for feed in &snapshot {
        if feed.state != FeedState::Live {
            return Outcome::Fail(format!(
                "{:?} feed ended {:?}, not Live",
                feed.datatype, feed.state
            ));
        }
        if feed.instrument != canonical {
            return Outcome::Fail(format!(
                "{:?} feed reports instrument {}, decoded {canonical}",
                feed.datatype, feed.instrument
            ));
        }
    }
    Outcome::Pass
}

/// The kinds registry. Adding an entry here ratchets the kind to every venue
/// via `conformance_suite!`.
pub const KINDS: &[Kind] = &[
    Kind {
        name: "book_runtime_replay",
        atlas: "DAT-OB-S-5/S-6 seed race + Buffering, replayed through the REAL SourceRuntime",
        invariants: &[],
        run: book_runtime_replay,
    },
    Kind {
        name: "datatype_isolation",
        atlas: "declared-datatype isolation (C1/C6): disabled datatypes yield no channels and no events",
        invariants: &[],
        run: datatype_isolation,
    },
    Kind {
        name: "decode_surface",
        atlas: "DAT-CN decode, normalize boundary",
        invariants: &[],
        run: decode_surface,
    },
    Kind {
        name: "seeding_taxonomy",
        atlas: "DAT-RT seeding, SnapshotSource SSoT",
        invariants: &[],
        run: seeding_taxonomy,
    },
    Kind {
        name: "symbol_canonicalization",
        atlas: "venue variation (codec), DAT-RT symbol decode",
        invariants: &[],
        run: symbol_canonicalization,
    },
    Kind {
        name: "trade_continuity",
        atlas: "DAT-TB apply, gap accounting",
        invariants: &["DAT-TB-INV-6"],
        run: trade_continuity,
    },
    Kind {
        name: "trade_invariants",
        atlas: "DAT-TB-INV-12 (independent: positivity + µs-scale)",
        invariants: &["DAT-TB-INV-12"],
        run: trade_invariants,
    },
    Kind {
        name: "book_delta_application",
        atlas: "DAT-OB apply, OrderBookState",
        invariants: &["DAT-OB-INV-2"],
        run: book_delta_application,
    },
    Kind {
        name: "book_invariants",
        atlas: "DAT-OB-INV-16 (independent: not-crossed + positivity + ordering)",
        invariants: &["DAT-OB-INV-16"],
        run: book_invariants,
    },
    Kind {
        name: "gap_detection",
        atlas: "DAT-OB-T-4 gap, DAT-OB-INV-9 (negative path: a dropped delta must Gap)",
        invariants: &["DAT-OB-T-4", "DAT-OB-INV-9"],
        run: gap_detection,
    },
    Kind {
        name: "checksum_validation",
        atlas: "DAT-OB ChecksumDelta, fail-closed on absent checksum",
        invariants: &[],
        run: checksum_validation,
    },
    Kind {
        name: "derivative_invariants",
        atlas: "funding/open-interest sample physics: bounded rate, positive OI, \
                declared interval, decimal-string parse discipline",
        invariants: &[],
        run: derivative_invariants,
    },
    Kind {
        name: "feed_lifecycle",
        atlas: "DAT-F feed lifecycle: every declared feed reaches Live under the \
                decoded instrument, replayed through the real SourceRuntime",
        invariants: &["DAT-F-T-3", "DAT-F-INV-4"],
        run: feed_lifecycle,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "tx_decode_surface",
        atlas: "DAT-TX-INV-1: every captured SubscribeUpdate replays through replay_stream; \
                each row matches its raw update: version (v1 iff a message config, v0 iff \
                versioned), requested compute (leading ComputeBudget bytes on legacy and v0, \
                the message config on v1), fee_lamports = 5000 per signature (precompile \
                signatures included) plus the priority fee, failure columns from the \
                TransactionError and its instruction, logs stored only with logs = true; \
                stands in for decode_surface and seeding_taxonomy on stream venues; no capture \
                holds an undecodable error, so that path stays with the unit tests",
        invariants: &["DAT-TX-INV-1"],
        run: super::stream::tx_decode_surface,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "slot_assembly",
        atlas: "DAT-SB-INV-1: rows come only from banks sealed by their blocks_meta, carry \
                that bank's slot, height, blockhash and block time, run in (slot, tx_index) \
                order and precede the bank's SlotTick; coverage opens two slots after a \
                segment's first input slot; dropping one captured blocks_meta frame leaves \
                its bank without rows and one exact SlotGap",
        invariants: &["DAT-SB-INV-1"],
        run: super::stream::slot_assembly,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "slot_reorder",
        atlas: "DAT-SB-T-1: no capture holds a child bank before its parent, so captured \
                frames are moved: a child sent ahead of its parent is Held, then released \
                after it in height order exactly as captured; a parent sent past the reorder \
                window lets the child release GappedFinalized with one SlotGap and is then a \
                Duplicate without rows; SlotTick strictly in height order",
        invariants: &["DAT-SB-T-1"],
        run: super::stream::slot_reorder,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "tx_dedup",
        atlas: "DAT-SB-INV-2: no capture overlaps released slots after a reconnect, so \
                captured frames are repeated: every update delivered twice replays exactly \
                as captured, and the banks up to the last released one, re-sent after the \
                reconnect marker, are Duplicate and add no row; (address, signature) unique",
        invariants: &["DAT-SB-INV-2"],
        run: super::stream::tx_dedup,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "slot_gap_detection",
        atlas: "DAT-SB-T-2, DAT-SK-T-16: a reconnect at tip yields exactly one SlotGap whose \
                exclusive bounds are the last bank released before it and the first released \
                after it, blocks_lost = next_height - prev_height - 1, counted once by the \
                runtime and appended once to the gap ledger as GapCause::SlotGap; neither the \
                subscribe request nor a ping carries from_slot",
        invariants: &["DAT-SB-T-2", "DAT-SK-T-16"],
        run: super::stream::slot_gap_detection,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "slot_tick_grid",
        atlas: "DAT-SY-INV-25: the slot-driven grid advances only on SlotTick chain time less \
                the emission delay: two replays of one capture build identical rows, every \
                transaction lands in the row of its block time, and a quiet address ticks \
                rows without transactions, so no transactions file is written",
        invariants: &["DAT-SY-INV-25"],
        run: super::stream::slot_tick_grid,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "shared_connection_fanout",
        atlas: "shared gRPC connection fan-out (attach set, per-attach queues, one \
                conn_epoch_us per physical connection): a live-connection property the geyser \
                mock tests certify; offline replay feeds one consumer",
        invariants: &[],
        run: super::stream::shared_connection_fanout,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "address_match_roles",
        atlas: "DAT-TX-INV-2: match_role is the address's strongest role (fee_payer > signer \
                > account_key, loaded keys account_key only), account_keys keep static, \
                loaded writable, loaded readonly order, signers are the header's signer keys, \
                match keeps the rows whose role satisfies it, a program never signs, and the \
                unfiltered capture replayed for an address yields its server-filtered rows",
        invariants: &["DAT-TX-INV-2"],
        run: super::stream::address_match_roles,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "failed_inclusion",
        atlas: "DAT-TX-INV-3: include_failed = true leaves the server failed filter unset and \
                keeps failed rows with their failure columns; false sends failed = false and \
                drops failed rows locally, counted as filtered_out",
        invariants: &["DAT-TX-INV-3"],
        run: super::stream::failed_inclusion,
    },
    #[cfg(feature = "grpc")]
    Kind {
        name: "tx_invariants",
        atlas: "DAT-TX-INV-4: Transaction::check_invariants and check_transactions (unique \
                (address, signature), rising (slot, tx_index), record invariants) hold on \
                every replayed row and on its transactions.v1 Parquet round trip",
        invariants: &["DAT-TX-INV-4"],
        run: super::stream::tx_invariants,
    },
];

#[test]
fn stream_models_never_reach_replay_frame() {
    use aetelier_connect::framework::budget::{ConnectionBudget, SourceMetrics};
    use aetelier_connect::framework::protocol::DeclaredSet;
    use aetelier_connect::framework::registry::{ExchangeProfile, TaskExit};
    use aetelier_connect::framework::symbol::SymbolCodec;
    use aetelier_types::onchain::Chain;

    struct StreamVenue(ExchangeProfile);

    impl ExchangeAdapter for StreamVenue {
        fn id(&self) -> &'static str {
            "solana"
        }

        fn profile(&self) -> &ExchangeProfile {
            &self.0
        }

        fn book_model(&self, _channel: &str) -> ReconstructionModel {
            ReconstructionModel::Stream
        }

        fn spawn(
            &self,
            _symbols: Vec<String>,
            _declared: DeclaredSet,
            _tx: tokio::sync::mpsc::Sender<DomainEvent>,
            _shutdown: tokio::sync::watch::Receiver<bool>,
            _metrics: SourceMetrics,
        ) -> tokio::task::JoinHandle<TaskExit> {
            unreachable!("conformance replays offline")
        }

        fn replay_frame(
            &self,
            raw: &str,
        ) -> Result<Vec<DomainEvent>, Box<aetelier_connect::errors::ExchangeError>>
        {
            panic!("a stream model reached replay_frame with {raw}")
        }
    }

    let stream = StreamVenue(ExchangeProfile {
        id: "solana",
        symbol_codec: SymbolCodec::ChainAddress {
            chain: Chain::Solana,
        },
        budget: ConnectionBudget::default(),
        schema_version: 1,
        protocol_revision: "solana-stream",
    });
    match replay_fixture(&stream, "hyperliquid/btc_book_trade.jsonl") {
        Err(Outcome::NotApplicable(reason)) => {
            assert!(reason.contains("replay_stream"), "{reason}");
        }
        Err(other) => panic!("expected NotApplicable, got {other:?}"),
        Ok(events) => panic!("a stream model replayed {} events", events.len()),
    }
    for (id, adapter) in registry() {
        let model = adapter.book_model("orders");
        assert_eq!(
            stateless_replay_applies(&model).is_ok(),
            model.reconstructs_books(),
            "{id}"
        );
    }
    let hyperliquid = *registry().get("hyperliquid").unwrap();
    match replay_fixture(hyperliquid, "hyperliquid/btc_book_trade.jsonl") {
        Ok(events) => assert!(!events.is_empty()),
        Err(other) => panic!("a book venue must replay its frames: {other:?}"),
    }
}
