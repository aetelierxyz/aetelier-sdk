//! Conformance harness core: the per-venue descriptor, the kind-outcome
//! model, and the runner + matrix report.
//!
//! Design (per tests-v0.11/conformance-methodology.md): a `VenueConformance`
//! descriptor carries ONLY fixtures + doc-derived expectations; everything
//! else (reconstruction model, seeding taxonomy, symbol codec, the decode +
//! normalize path) is read from the adapter registry, so the descriptor
//! cannot drift from code. Kinds are generic functions over the descriptor; a
//! macro instantiates every kind for every venue, so a new kind ratchets to
//! all venues at once and a venue missing a required fixture is a hard test
//! failure.

use aetelier_connect::framework::model::SnapshotSource;
use aetelier_connect::framework::registry::{ReplayFrame, registry};
use aetelier_types::onchain::{Chain, Commitment};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

/// The seeding-snapshot source a venue's documentation declares. This is the
/// one expectation the harness cannot read from code — it is what the code is
/// checked against (kind `seeding_taxonomy`). Non-Rest variants are wired as
/// the self-seed / checksum / L3 venues reach their cycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ExpectSource {
    Rest,
    WssSelfSeed,
    ReqOnSocket,
    /// FullRefresh / ChecksumDelta reconstruct without a seed.
    None,
}

impl ExpectSource {
    pub fn matches(self, actual: Option<SnapshotSource>) -> bool {
        matches!(
            (self, actual),
            (ExpectSource::Rest, Some(SnapshotSource::RestSnapshot))
                | (ExpectSource::WssSelfSeed, Some(SnapshotSource::WssSelfSeed))
                | (ExpectSource::ReqOnSocket, Some(SnapshotSource::ReqOnSocket))
                | (ExpectSource::None, Option::None)
        )
    }
}

/// A wire message class the venue's docs declare for our subscribed channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Book,
    Trade,
    FundingRate,
    OpenInterest,
    FundingSettlement,
    Transaction,
}

/// Per-venue conformance descriptor. Fixtures + doc-derived expectations only.
pub struct VenueConformance {
    /// Registered venue id (the registry key).
    pub venue: &'static str,
    /// A wire symbol that appears in the fixture, and the canonical pair the
    /// codec must decode it to.
    pub wire_symbol: &'static str,
    pub canonical_base: &'static str,
    pub canonical_quote: &'static str,
    /// Doc-derived seeding expectation (checked against the adapter's model).
    pub expect_source: ExpectSource,
    pub expect_needs_rest: bool,
    /// Committed WS-frames fixture, path relative to the `tests/` directory.
    pub ws_fixture: &'static str,
    /// Committed REST seed-snapshot fixture for REST-model venues (path
    /// relative to `tests/`); `None` for self-seeding venues.
    pub rest_fixture: Option<&'static str>,
    /// The message classes the venue doc declares for our subscribed channels.
    pub expect_classes: &'static [Class],
    /// Whether this venue is flagged DONE: its full kinds matrix must be green
    /// (no Fail, and every applicable kind Pass) for the meta-test to pass.
    pub done: bool,
    /// Whether the adapter's subscribe path honors the declared-datatype set
    /// (C1 of the datatype-isolation plan). Until true, the
    /// `datatype_isolation` kind reports NotApplicable for the venue.
    pub datatype_isolation_done: bool,
    pub expect_instrument: ExpectInstrument,
    pub stream_fixtures: &'static [StreamFixture],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(feature = "grpc"), allow(dead_code))]
pub enum ExpectInstrument {
    Pair,
    Address(Chain),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectGap {
    pub prev_slot: u64,
    pub prev_height: u64,
    pub next_slot: u64,
    pub next_height: u64,
}

#[cfg_attr(not(feature = "grpc"), allow(dead_code))]
pub struct StreamFixture {
    pub file: &'static str,
    pub watched: &'static str,
    pub commitment: Commitment,
    pub unfiltered: Option<&'static str>,
    pub rows: usize,
    pub ticks: usize,
    pub gaps: &'static [ExpectGap],
}

/// The outcome of running one kind against one venue.
#[derive(Debug)]
pub enum Outcome {
    Pass,
    /// This kind structurally does not apply to this venue (e.g. a checksum
    /// kind on a non-checksum venue). Always acceptable, DONE venues included —
    /// it is not a coverage gap.
    NotApplicable(String),
    /// The kind applies but is deferred — a fixture or capability the venue
    /// will have. Acceptable only on a non-DONE venue; a DONE venue with a
    /// Skip has an un-closed coverage gap.
    Skip(String),
    /// A conformance violation, or a required fixture is missing — the ratchet
    /// hard failure.
    Fail(String),
}

pub type KindFn = fn(&VenueConformance) -> Outcome;

/// A conformance kind: a named generic check anchored to atlas IDs.
pub struct Kind {
    pub name: &'static str,
    /// Prose description of the atlas surface this kind exercises.
    pub atlas: &'static str,
    /// The structured atlas invariant/transition id(s) this kind CERTIFIES —
    /// the emitter-join key. Every id must be registered in
    /// `framework::atlas::ENFORCED` (the coverage meta-test enforces both
    /// directions). Empty for surface/taxonomy kinds that certify no single
    /// spec id.
    pub invariants: &'static [&'static str],
    pub run: KindFn,
}

/// Load a fixture's non-empty lines. Path is relative to `datasets/`.
pub fn fixture_lines(rel: &str) -> Result<Vec<String>, String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("datasets")
        .join(rel);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("fixture {rel} unreadable: {e}"))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// Read a whole fixture file (e.g. a REST snapshot JSON body). Path relative
/// to `datasets/`.
pub fn fixture_raw(rel: &str) -> Result<String, String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("datasets")
        .join(rel);
    std::fs::read_to_string(&path).map_err(|e| format!("fixture {rel} unreadable: {e}"))
}

pub fn stream_frames(rel: &str) -> Result<Vec<ReplayFrame>, String> {
    let lines = fixture_lines(rel)?;
    if lines.is_empty() {
        return Err(format!("fixture {rel} has no frames"));
    }
    lines
        .iter()
        .enumerate()
        .map(|(i, line)| {
            stream_frame(line).map_err(|e| format!("fixture {rel} line {}: {e}", i + 1))
        })
        .collect()
}

pub fn stream_frame(line: &str) -> Result<ReplayFrame, String> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FrameLine {
        t_us: u64,
        upd: Option<String>,
        reconnect: Option<bool>,
    }
    let parsed: FrameLine = serde_json::from_str(line).map_err(|e| e.to_string())?;
    match (parsed.upd, parsed.reconnect) {
        (Some(upd), None) => STANDARD
            .decode(upd)
            .map(|bytes| ReplayFrame::Update {
                t_us: parsed.t_us,
                bytes,
            })
            .map_err(|e| format!("upd is not standard base64: {e}")),
        (None, Some(true)) => Ok(ReplayFrame::Reconnect { t_us: parsed.t_us }),
        _ => Err(
            "a frame line carries exactly one of \"upd\" or \"reconnect\": true"
                .to_string(),
        ),
    }
}

/// Assert a kind's outcome for a venue: Pass and Skip are acceptable; Fail
/// panics with the reason (the failing venue::kind names the nextest).
pub fn assert_kind(desc: &VenueConformance, kind: &Kind) {
    // A venue with no registered adapter is always a hard failure.
    if registry().get(desc.venue).is_none() {
        panic!("venue '{}' is not registered", desc.venue);
    }
    match (kind.run)(desc) {
        Outcome::Pass => {}
        Outcome::NotApplicable(reason) => {
            eprintln!("n/a  {}::{} — {reason}", desc.venue, kind.name);
        }
        Outcome::Skip(reason) => {
            if desc.done {
                panic!(
                    "DONE venue '{}' may not skip kind '{}' (un-closed coverage gap): {reason}",
                    desc.venue, kind.name
                );
            }
            eprintln!("skip {}::{} — {reason}", desc.venue, kind.name);
        }
        Outcome::Fail(reason) => {
            panic!("{}::{} FAILED — {reason}", desc.venue, kind.name);
        }
    }
}
