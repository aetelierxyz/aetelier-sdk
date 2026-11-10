//! Configuration for [`MarketWorker`](crate::workers::market_worker::MarketWorker).
//!
//! [`MarketWorkerConfig`] extends [`CommonWorkerFields`] with synchronisation
//! parameters (clock mode, grid spacing, flush threshold).
//!
//! [`MarketWorkerManifest`] is the multi-worker TOML manifest that resolves
//! into a `Vec<MarketWorkerConfig>`.

use serde::Deserialize;
use std::path::Path;

use super::common::{
    CommonWorkerFields, ManifestMetadata, OutputSinkConfig, ReconnectSection,
};
use aetelier_types::config::markets::market_config::{
    DataTypesSection, MarketSnapshotConfig, SyncMode, UpdateFrequency,
};
use aetelier_types::exchanges::{Exchange, MarketType, VenueEnvironment};
use aetelier_types::onchain::{Chain, ChainAddress, Commitment};

use crate::errors::ConnectError;
use crate::framework::registry::{
    admit_datatypes, admit_environment, admit_market_type, admit_registered,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportKind {
    #[default]
    Wss,
    Entrepot,
    Grpc,
}

impl TransportKind {
    pub const fn forces_framework(self) -> bool {
        match self {
            TransportKind::Wss => false,
            TransportKind::Entrepot | TransportKind::Grpc => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntrepotSourceKind {
    #[default]
    Local,
    S3,
}

/// `[collect.entrepot]` — the object-store replay window. `source = "local"`
/// reads `root`, a directory tree in the bucket's key layout; `source = "s3"`
/// reads the live bucket (`bucket` + `region`, credentials from the
/// environment unless `anonymous`).
#[derive(Debug, Clone, Deserialize)]
pub struct EntrepotSection {
    #[serde(default)]
    pub source: EntrepotSourceKind,
    pub root: Option<std::path::PathBuf>,
    pub start: chrono::NaiveDate,
    pub end: chrono::NaiveDate,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub requester_pays: Option<bool>,
    #[serde(default)]
    pub anonymous: bool,
    pub endpoint: Option<String>,
    pub cursor: Option<std::path::PathBuf>,
    pub fetch_concurrency: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcSection {
    pub endpoint: String,
    #[serde(default)]
    pub x_token_env: Option<String>,
    #[serde(default)]
    pub commitment: Commitment,
    #[serde(default = "default_server_filter")]
    pub server_filter: bool,
    #[serde(default = "default_accept_zstd")]
    pub accept_zstd: bool,
    #[serde(default = "default_max_decoding_message_size_mib")]
    pub max_decoding_message_size_mib: usize,
    #[serde(default = "default_ping_interval_secs")]
    pub ping_interval_secs: u64,
    #[serde(default = "default_max_pending_slots")]
    pub max_pending_slots: u64,
    #[serde(default = "default_max_addresses_per_connection")]
    pub max_addresses_per_connection: usize,
    #[serde(default)]
    pub account_include_max: Option<usize>,
    #[serde(default)]
    pub account_include_reject: Vec<ChainAddress>,
}

fn default_server_filter() -> bool {
    true
}

fn default_accept_zstd() -> bool {
    true
}

fn default_max_decoding_message_size_mib() -> usize {
    64
}

fn default_ping_interval_secs() -> u64 {
    15
}

fn default_max_pending_slots() -> u64 {
    32
}

fn default_max_addresses_per_connection() -> usize {
    50
}

impl GrpcSection {
    pub fn validate(&self) -> Result<(), String> {
        if !(self.endpoint.starts_with("http://")
            || self.endpoint.starts_with("https://"))
        {
            return Err(format!(
                "[collect.grpc] endpoint = \"{}\" is not a URL: set it to an http:// or \
                 https:// address",
                self.endpoint
            ));
        }
        if let Some(name) = &self.x_token_env
            && (name.is_empty() || name.contains('=') || name.contains('\0'))
        {
            return Err(format!(
                "[collect.grpc] x_token_env = {name:?} is not an environment variable \
                 name: set a non-empty name without '=' or NUL"
            ));
        }
        let zero_limit = [
            (
                "max_decoding_message_size_mib",
                self.max_decoding_message_size_mib == 0,
            ),
            ("ping_interval_secs", self.ping_interval_secs == 0),
            ("max_pending_slots", self.max_pending_slots == 0),
            (
                "max_addresses_per_connection",
                self.max_addresses_per_connection == 0,
            ),
            ("account_include_max", self.account_include_max == Some(0)),
        ]
        .into_iter()
        .find_map(|(key, zero)| zero.then_some(key));
        match zero_limit {
            Some(key) => Err(format!(
                "[collect.grpc] {key} = 0 admits nothing: set it to 1 or more"
            )),
            None => Ok(()),
        }
    }

    pub fn admit_token(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<(), String> {
        let Some(name) = &self.x_token_env else {
            return Ok(());
        };
        match lookup(name) {
            Some(token) if !token.is_empty() => Ok(()),
            _ => Err(format!(
                "[collect.grpc] x_token_env = \"{name}\" names an unset or empty \
                 variable: export {name} with the provider token"
            )),
        }
    }

    pub fn shard_size(&self) -> usize {
        match self.account_include_max {
            Some(max) => self.max_addresses_per_connection.min(max),
            None => self.max_addresses_per_connection,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────────────
// SyncSection
// ──────────────────────────────────────────────────────────────────────────────────────

/// Synchronisation parameters for the MarketWorker.
#[derive(Debug, Clone, Deserialize)]
pub struct SyncSection {
    /// Which event drives the grid clock.
    pub sync_mode: SyncMode,
    /// Grid spacing (value + unit).
    pub update_frequency: UpdateFrequency,
    /// Grid ticks before flushing / emitting a batch.
    pub flush_threshold: usize,
}

impl SyncSection {
    /// Grid period in microseconds (the platform timestamp standard;
    /// sub-microsecond configs round down).
    pub fn period_us(&self) -> u64 {
        self.update_frequency.as_micros()
    }
}

// ──────────────────────────────────────────────────────────────────────────────────────
// ReconcileSection
// ──────────────────────────────────────────────────────────────────────────────────────

/// Live-reconciliation parameters (`[collect.reconcile]`, optional; a metered
/// Pro feature). Only meaningful for the synchronized MarketWorker at
/// cadence ≥ 50ms — validated loudly at worker construction.
#[derive(Debug, Clone, Deserialize)]
pub struct ReconcileSection {
    /// Master switch. `false` behaves as if the section were absent.
    #[serde(default)]
    pub enabled: bool,
    /// Emission hold-back window W (value + unit). Rows emit W after their
    /// boundary so REST-recovered prints land in their true rows. Default 1s
    /// (≥ venue REST RTT + margin) when omitted.
    #[serde(default)]
    pub emission_delay: Option<UpdateFrequency>,
    /// Periodic sweep cadence in seconds (0 disables the sweep; incident-
    /// triggered fetches still run). Default 60.
    #[serde(default = "default_sweep_secs")]
    pub sweep_secs: u64,
}

fn default_sweep_secs() -> u64 {
    60
}

impl ReconcileSection {
    /// The hold-back window in microseconds (default 1s).
    pub fn emission_delay_us(&self) -> u64 {
        match &self.emission_delay {
            None => 1_000_000,
            Some(f) => f.as_micros(),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────────────
// MarketWorkerConfig — per-worker resolved config
// ──────────────────────────────────────────────────────────────────────────────────────

/// Fully-resolved configuration for a single [`MarketWorker`](crate::workers::market_worker::MarketWorker).
#[derive(Debug, Clone)]
pub struct MarketWorkerConfig {
    /// Shared worker fields (exchange, symbol, datatypes, tuning).
    pub common: CommonWorkerFields,
    /// Synchronisation parameters.
    pub sync: SyncSection,
    /// Output sinks.
    pub output: Vec<OutputSinkConfig>,
    /// Reconstruct the book through the framework adapter + runtime instead of
    /// the legacy per-exchange ingestion path. Only takes effect when the venue
    /// is registered *and* REST-seeded and the enabled datatypes are a subset of
    /// {orderbook, trades}; otherwise the worker falls back to the legacy path.
    /// Default `false`.
    pub framework_ingest: bool,
    /// Live-reconciliation settings (`None` = feature off).
    pub reconcile: Option<ReconcileSection>,
    pub emission_delay: Option<UpdateFrequency>,
    /// Source transport: live WSS (default), object-store replay, or gRPC.
    pub transport: TransportKind,
    /// Replay window; required when `transport = "entrepot"`.
    pub entrepot: Option<EntrepotSection>,
    pub grpc: Option<GrpcSection>,
}

impl MarketWorkerConfig {
    /// Create from a legacy [`MarketSnapshotConfig`] with a channel-only
    /// sink.  Used for backward compatibility.
    pub fn from_legacy(cfg: &MarketSnapshotConfig) -> Self {
        Self {
            common: CommonWorkerFields::from(cfg),
            sync: SyncSection {
                sync_mode: cfg.symbol.sync_mode,
                update_frequency: cfg.update_frequency.clone(),
                flush_threshold: cfg.pipeline.flush_threshold,
            },
            output: vec![OutputSinkConfig::Channel],
            framework_ingest: false,
            reconcile: None,
            emission_delay: None,
            transport: TransportKind::default(),
            entrepot: None,
            grpc: None,
        }
    }

    pub fn admit(&self) -> Result<(), ConnectError> {
        let Ok(exchange) = self.common.exchange.parse::<Exchange>() else {
            return Ok(());
        };
        match exchange.chain() {
            None => self.admit_cex_venue(exchange),
            Some(chain) => self.admit_chain_venue(exchange, chain),
        }
        .map_err(ConnectError::Build)
    }

    fn admit_cex_venue(&self, exchange: Exchange) -> Result<(), String> {
        if self.transport == TransportKind::Grpc {
            return Err(
                "transport = \"grpc\" is only available for exchange = \"solana\""
                    .to_string(),
            );
        }
        if self.sync.sync_mode == SyncMode::OnSlot {
            return Err(format!(
                "sync_mode = \"on_slot\" is only available for exchange = \"solana\": \
                 venue '{exchange}' produces no slots"
            ));
        }
        if let Ok(adapter) = admit_registered(exchange) {
            admit_market_type(
                &exchange.to_string(),
                adapter.supported_market_types(),
                self.common.market_type,
            )?;
            admit_datatypes(
                exchange,
                adapter.supported_datatypes(),
                &self.common.datatypes.declared_set(),
            )?;
        }
        Ok(())
    }

    fn admit_chain_venue(&self, exchange: Exchange, chain: Chain) -> Result<(), String> {
        if self.transport != TransportKind::Grpc {
            return Err(format!(
                "exchange = \"{exchange}\" streams over gRPC: set transport = \
                 \"grpc\" and add a [collect.grpc] section"
            ));
        }
        let Some(grpc) = &self.grpc else {
            return Err(
                "transport = \"grpc\" requires a [collect.grpc] section".to_string()
            );
        };
        grpc.validate()?;
        grpc.admit_token(|name| std::env::var(name).ok())?;
        let address = ChainAddress::parse(chain, &self.common.symbol).map_err(|e| {
            format!(
                "{e}: set symbol to the account's {chain} address in base58, case kept"
            )
        })?;
        if grpc.account_include_reject.contains(&address) {
            return Err(format!(
                "symbol = \"{address}\" is in [collect.grpc] account_include_reject: the \
                 provider refuses to filter on it, so choose another address or provider"
            ));
        }
        match self.sync.sync_mode {
            SyncMode::OnSlot => {}
            SyncMode::OnTime => {
                return Err(format!(
                    "sync_mode = \"on_time\" is not available for exchange = \
                     \"{exchange}\": a wall-clock grid closes each row before its \
                     finalized transactions arrive and drops them as late; set \
                     sync_mode = \"on_slot\""
                ));
            }
            SyncMode::OnOrderbook | SyncMode::OnTrade | SyncMode::OnLiquidation => {
                return Err(format!(
                    "sync_mode = \"{}\" is not available for exchange = \"{exchange}\": \
                     set sync_mode = \"on_slot\"",
                    self.sync.sync_mode
                ));
            }
        }
        if self.reconcile.as_ref().is_some_and(|r| r.enabled) {
            return Err(format!(
                "[collect.reconcile] is not available for exchange = \"{exchange}\": \
                 remove the section or set enabled = false"
            ));
        }
        let declared = self.common.datatypes.declared_set();
        if declared.is_empty() {
            return Err(format!(
                "exchange = \"{exchange}\" declares no datatype: set enabled = true \
                 under [collect.datatypes.transactions] or the worker's \
                 [workers.datatypes.transactions]"
            ));
        }
        let adapter = admit_registered(exchange)?;
        let venue = exchange.to_string();
        let environment = self.common.environment;
        admit_environment(
            &venue,
            adapter.supports_environment(environment),
            environment,
        )?;
        admit_market_type(
            &venue,
            adapter.supported_market_types(),
            self.common.market_type,
        )?;
        admit_datatypes(exchange, adapter.supported_datatypes(), &declared)
    }
}

// ──────────────────────────────────────────────────────────────────────────────────────
// MarketWorkerManifest — multi-worker TOML manifest
// ──────────────────────────────────────────────────────────────────────────────────────

/// Top-level TOML manifest for spawning multiple [`MarketWorker`](crate::workers::market_worker::MarketWorker)s.
///
/// # Example TOML
///
/// ```toml
/// [collect]
/// exchange = "bybit"
///
/// [collect.datatypes.orderbook]
/// enabled = true
/// depth   = 50
///
/// [collect.datatypes.trades]
/// enabled = true
///
/// [collect.sync]
/// sync_mode = "on_trade"
/// flush_threshold = 36000
///
/// [collect.sync.update_frequency]
/// value = 100
/// unit  = "Millis"
///
/// [[collect.output]]
/// type = "channel"
///
/// [[workers]]
/// symbol = "BTCUSDT"
///
/// [[workers]]
/// symbol = "ETHUSDT"
/// sync_mode = "on_orderbook"
///
/// [session]
/// duration_hours = 8
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct MarketWorkerManifest {
    /// Shared collectors applied to all workers.
    pub collect: MarketWorkerCollect,
    /// Per-symbol worker definitions.
    pub workers: Vec<MarketWorkerEntry>,
    /// Session parameters.
    #[serde(default)]
    pub session: super::data_worker_config::SessionSection,
    /// Platform-injected identity metadata (present when received over the wire).
    #[serde(default)]
    pub metadata: Option<ManifestMetadata>,
}

/// Shared collect instructions for a [`MarketWorkerManifest`].
#[derive(Debug, Clone, Deserialize)]
pub struct MarketWorkerCollect {
    /// Default exchange for all workers.
    pub exchange: String,
    /// Default market type (spot, perpetual, inverse, onchain).
    #[serde(default)]
    pub market_type: MarketType,
    #[serde(default)]
    pub environment: VenueEnvironment,
    /// Which data feeds to subscribe to.
    pub datatypes: DataTypesSection,
    /// Synchronisation parameters.
    pub sync: SyncSection,
    /// Output sinks.
    #[serde(default = "default_output_sinks")]
    pub output: Vec<OutputSinkConfig>,
    /// Broadcast channel capacity per topic.
    #[serde(default)]
    pub channel_capacity: Option<usize>,
    /// Staleness timeout in seconds.
    #[serde(default)]
    pub staleness_timeout_secs: Option<u64>,
    /// Gap detection silence threshold in seconds.
    #[serde(default)]
    pub gap_threshold_secs: Option<u64>,
    /// Reconnection tuning.
    #[serde(default)]
    pub reconnect: Option<ReconnectSection>,
    /// Live-reconciliation section (`[collect.reconcile]`, optional).
    #[serde(default)]
    pub reconcile: Option<ReconcileSection>,
    #[serde(default)]
    pub emission_delay: Option<UpdateFrequency>,
    /// Ingest via the framework engine instead of the legacy raw path.
    /// Lives at `[collect]` (not `[collect.sync]`) so the platform's single
    /// `[collect]`-level injector reaches both Data and Market manifests.
    /// Default `false` so every existing manifest stays on the legacy path.
    #[serde(default)]
    pub framework_ingest: bool,
    /// Source transport for every worker; `entrepot` and `grpc` force the
    /// framework path and require `[collect.entrepot]` or `[collect.grpc]`.
    #[serde(default)]
    pub transport: TransportKind,
    #[serde(default)]
    pub entrepot: Option<EntrepotSection>,
    #[serde(default)]
    pub grpc: Option<GrpcSection>,
}

/// A single worker entry in a [`MarketWorkerManifest`].
#[derive(Debug, Clone, Deserialize)]
pub struct MarketWorkerEntry {
    /// Trading pair (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Override the default exchange.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Override market type for this worker.
    #[serde(default)]
    pub market_type: Option<MarketType>,
    /// Override sync_mode for this worker.
    #[serde(default)]
    pub sync_mode: Option<SyncMode>,
    /// Override datatypes for this worker.
    #[serde(default)]
    pub datatypes: Option<DataTypesSection>,
    /// Override output sinks for this worker.
    #[serde(default)]
    pub output: Option<Vec<OutputSinkConfig>>,
    /// Override framework ingest for this worker.
    #[serde(default)]
    pub framework_ingest: Option<bool>,
}

fn default_output_sinks() -> Vec<OutputSinkConfig> {
    vec![OutputSinkConfig::Channel]
}

const SYNC_KEYS: [&str; 3] = ["sync_mode", "update_frequency", "flush_threshold"];

const WORKER_KEYS: [&str; 7] = [
    "symbol",
    "exchange",
    "market_type",
    "sync_mode",
    "datatypes",
    "output",
    "framework_ingest",
];

impl MarketWorkerManifest {
    /// Load and parse from a TOML file on disk.
    pub fn from_toml(path: &Path) -> Result<Self, ConnectError> {
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConnectError::Read {
                path: format!("{path:?}"),
                source,
            })?;
        Self::from_str(&contents)
    }

    /// Parse from a TOML string (e.g. received over the wire from the control
    /// plane).
    // Intentionally not the `FromStr` trait: this parses TOML content, not a
    // canonical string round-trip.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(toml_content: &str) -> Result<Self, ConnectError> {
        let manifest: Self = toml::from_str(toml_content)
            .map_err(|e| ConnectError::Parse(format!("MarketWorkerManifest: {e}")))?;
        if manifest.declares_chain_venue() {
            manifest.reject_duplicate_addresses()?;
            manifest.reject_unread_keys(toml_content)?;
        }
        Ok(manifest)
    }

    /// Resolve every [`MarketWorkerEntry`] into a fully-specified
    /// [`MarketWorkerConfig`].
    pub fn resolve_all(&self) -> Vec<MarketWorkerConfig> {
        self.workers.iter().map(|w| self.resolve_entry(w)).collect()
    }

    pub fn resolve_admitted(&self) -> Result<Vec<MarketWorkerConfig>, ConnectError> {
        let configs = self.resolve_all();
        for config in &configs {
            config.admit()?;
        }
        Ok(configs)
    }

    fn worker_chain(&self, entry: &MarketWorkerEntry) -> Option<Chain> {
        entry
            .exchange
            .as_deref()
            .unwrap_or(&self.collect.exchange)
            .parse::<Exchange>()
            .ok()
            .and_then(Exchange::chain)
    }

    fn declares_chain_venue(&self) -> bool {
        self.workers
            .iter()
            .any(|entry| self.worker_chain(entry).is_some())
    }

    fn reject_duplicate_addresses(&self) -> Result<(), ConnectError> {
        let mut seen = std::collections::HashSet::new();
        for entry in &self.workers {
            let Some(chain) = self.worker_chain(entry) else {
                continue;
            };
            let Ok(address) = ChainAddress::parse(chain, &entry.symbol) else {
                continue;
            };
            if !seen.insert(address) {
                return Err(ConnectError::Parse(format!(
                    "MarketWorkerManifest: symbol \"{address}\" appears in more than one \
                     [[workers]] entry: keep one worker per address"
                )));
            }
        }
        Ok(())
    }

    fn reject_unread_keys(&self, toml_content: &str) -> Result<(), ConnectError> {
        let table: toml::Table = toml::from_str(toml_content)
            .map_err(|e| ConnectError::Parse(format!("MarketWorkerManifest: {e}")))?;
        let sync = table
            .get("collect")
            .and_then(|collect| collect.get("sync"))
            .and_then(toml::Value::as_table)
            .into_iter()
            .flat_map(|sync| sync.keys())
            .map(|key| ("[collect.sync]", key, SYNC_KEYS.as_slice()));
        let workers = table
            .get("workers")
            .and_then(toml::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(toml::Value::as_table)
            .flat_map(|worker| worker.keys())
            .map(|key| ("[[workers]]", key, WORKER_KEYS.as_slice()));
        let unread = sync
            .chain(workers)
            .find(|(_, key, read)| !read.contains(&key.as_str()));
        match unread {
            Some((section, key, read)) => Err(ConnectError::Parse(format!(
                "MarketWorkerManifest: unknown field `{key}` in {section}, expected one \
                 of {}",
                read.iter()
                    .map(|k| format!("`{k}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
            None => Ok(()),
        }
    }

    /// Session duration in seconds, if specified.
    pub fn duration_secs(&self) -> Option<f64> {
        self.session.duration_secs()
    }

    fn resolve_entry(&self, entry: &MarketWorkerEntry) -> MarketWorkerConfig {
        let d = &self.collect;
        MarketWorkerConfig {
            common: CommonWorkerFields {
                exchange: entry.exchange.clone().unwrap_or_else(|| d.exchange.clone()),
                symbol: entry.symbol.clone(),
                market_type: entry.market_type.unwrap_or(d.market_type),
                environment: d.environment,
                datatypes: entry
                    .datatypes
                    .clone()
                    .unwrap_or_else(|| d.datatypes.clone()),
                channel_capacity: d.channel_capacity,
                staleness_timeout_secs: d.staleness_timeout_secs,
                gap_threshold_secs: d.gap_threshold_secs,
                reconnect: d.reconnect.clone(),
            },
            sync: SyncSection {
                sync_mode: entry.sync_mode.unwrap_or(d.sync.sync_mode),
                update_frequency: d.sync.update_frequency.clone(),
                flush_threshold: d.sync.flush_threshold,
            },
            output: entry.output.clone().unwrap_or_else(|| d.output.clone()),
            framework_ingest: entry.framework_ingest.unwrap_or(d.framework_ingest),
            reconcile: d.reconcile.clone(),
            emission_delay: d.emission_delay.clone(),
            transport: d.transport,
            entrepot: d.entrepot.clone(),
            grpc: d.grpc.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::config::markets::market_config::{DeclaredDatatype, DeclaredSet};
    use aetelier_types::onchain::MatchRole;

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

    const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

    const PUBLICNODE: &str = "https://solana-yellowstone-grpc.publicnode.com:443";

    #[cfg(not(feature = "grpc"))]
    const GRPC_FEATURE: &str =
        "venue 'solana' needs the `grpc` feature: this build registers no solana adapter";

    const SOLANA_PLATFORM_TOML: &str = r#"
[collect]
exchange = "solana"
market_type = "onchain"
environment = "mainnet"
transport = "grpc"

[collect.grpc]
endpoint = "https://solana-yellowstone-grpc.publicnode.com:443"
x_token_env = "GRPC_X_TOKEN"
commitment = "finalized"
server_filter = true
accept_zstd = true
max_decoding_message_size_mib = 64
ping_interval_secs = 15
max_pending_slots = 32

[collect.reconnect]
initial_delay_ms = 1000
max_delay_ms = 30000
max_attempts = 10

[collect.emission_delay]
value = 2
unit = "Secs"

[collect.datatypes.transactions]
enabled = true
match = "account_key"
include_failed = true
logs = false

[collect.sync]
sync_mode = "on_slot"
flush_threshold = 60

[collect.sync.update_frequency]
value = 1
unit = "Secs"

[[collect.output]]
type = "parquet"
dir = "./datasets/recorded/solana/onchain/{data_type}/bnd_solana_001"

[[workers]]
symbol = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

[session]
duration_hours = 24
"#;

    fn solana_manifest(extra: &str) -> String {
        format!(
            r#"
[collect]
exchange = "solana"
market_type = "onchain"
transport = "grpc"
{extra}

[collect.grpc]
endpoint = "{PUBLICNODE}"

[collect.datatypes.transactions]
enabled = true

[collect.sync]
sync_mode = "on_slot"
flush_threshold = 60

[collect.sync.update_frequency]
value = 1
unit = "Secs"

[[workers]]
symbol = "{JUPITER_V6}"
"#
        )
    }

    fn resolve(toml: &str) -> MarketWorkerConfig {
        MarketWorkerManifest::from_str(toml)
            .unwrap()
            .resolve_all()
            .remove(0)
    }

    fn refusal(config: &MarketWorkerConfig) -> String {
        match config.admit() {
            Err(ConnectError::Build(reason)) => reason,
            other => panic!("expected a Build refusal, got {other:?}"),
        }
    }

    fn assert_admitted_or_names_the_grpc_feature(config: &MarketWorkerConfig) {
        #[cfg(feature = "grpc")]
        assert!(config.admit().is_ok(), "{:?}", config.admit());
        #[cfg(not(feature = "grpc"))]
        assert_eq!(refusal(config), GRPC_FEATURE);
    }

    fn grpc_defaults() -> GrpcSection {
        GrpcSection {
            endpoint: PUBLICNODE.to_string(),
            x_token_env: None,
            commitment: Commitment::Finalized,
            server_filter: true,
            accept_zstd: true,
            max_decoding_message_size_mib: 64,
            ping_interval_secs: 15,
            max_pending_slots: 32,
            max_addresses_per_connection: 50,
            account_include_max: None,
            account_include_reject: Vec::new(),
        }
    }

    fn cex_manifest(exchange: &str, collect: &str, sync: &str, tail: &str) -> String {
        format!(
            r#"
[collect]
exchange = "{exchange}"
{collect}

[collect.datatypes.orderbook]
enabled = true
depth = 25

[collect.datatypes.trades]
enabled = true

[collect.sync]
{sync}
flush_threshold = 600

[collect.sync.update_frequency]
value = 100
unit = "Millis"
{tail}"#
        )
    }

    #[test]
    fn transport_defaults_to_wss_and_entrepot_parses_its_window() {
        let base = r#"
[collect]
exchange = "hyperliquid"
{extra}

[collect.datatypes.orderbook]
enabled = true
depth = 20

[collect.sync]
sync_mode = "on_orderbook"
flush_threshold = 600

[collect.sync.update_frequency]
value = 100
unit = "Millis"

[[workers]]
symbol = "BTC"
"#;
        let default_cfg = MarketWorkerManifest::from_str(&base.replace("{extra}", ""))
            .unwrap()
            .resolve_all()
            .remove(0);
        assert_eq!(default_cfg.transport, TransportKind::Wss);
        assert!(default_cfg.entrepot.is_none());

        let entrepot_cfg = MarketWorkerManifest::from_str(&base.replace(
            "{extra}",
            r#"transport = "entrepot"

[collect.entrepot]
root = "/data/hl-archive"
start = "2023-09-16"
end = "2023-09-17"
"#,
        ))
        .unwrap()
        .resolve_all()
        .remove(0);
        assert_eq!(entrepot_cfg.transport, TransportKind::Entrepot);
        let section = entrepot_cfg.entrepot.unwrap();
        assert_eq!(section.source, EntrepotSourceKind::Local);
        assert_eq!(
            section.root,
            Some(std::path::PathBuf::from("/data/hl-archive"))
        );
        assert_eq!(
            section.start,
            chrono::NaiveDate::from_ymd_opt(2023, 9, 16).unwrap()
        );
        assert_eq!(
            section.end,
            chrono::NaiveDate::from_ymd_opt(2023, 9, 17).unwrap()
        );
        assert!(!section.anonymous);
        assert_eq!(section.cursor, None);
        assert_eq!(section.fetch_concurrency, None);

        let s3_cfg = MarketWorkerManifest::from_str(&base.replace(
            "{extra}",
            r#"transport = "entrepot"

[collect.entrepot]
source = "s3"
bucket = "hyperliquid-archive"
region = "us-east-1"
requester_pays = true
start = "2023-09-16"
end = "2023-09-17"
cursor = "/var/lib/aetelier/entrepot/btc.cursor"
fetch_concurrency = 4
"#,
        ))
        .unwrap()
        .resolve_all()
        .remove(0);
        let section = s3_cfg.entrepot.unwrap();
        assert_eq!(section.source, EntrepotSourceKind::S3);
        assert_eq!(section.bucket.as_deref(), Some("hyperliquid-archive"));
        assert_eq!(section.region.as_deref(), Some("us-east-1"));
        assert_eq!(section.requester_pays, Some(true));
        assert_eq!(section.root, None);
        assert_eq!(
            section.cursor,
            Some(std::path::PathBuf::from(
                "/var/lib/aetelier/entrepot/btc.cursor"
            ))
        );
        assert_eq!(section.fetch_concurrency, Some(4));

        let anon_cfg = MarketWorkerManifest::from_str(&base.replace(
            "{extra}",
            r#"transport = "entrepot"

[collect.entrepot]
source = "s3"
bucket = "public-mirror"
region = "eu-west-1"
anonymous = true
endpoint = "https://s3-eu-west-1.amazonaws.com/public-mirror"
start = "2023-09-16"
end = "2023-09-16"
"#,
        ))
        .unwrap()
        .resolve_all()
        .remove(0);
        let section = anon_cfg.entrepot.unwrap();
        assert!(section.anonymous);
        assert_eq!(
            section.endpoint.as_deref(),
            Some("https://s3-eu-west-1.amazonaws.com/public-mirror")
        );
    }

    #[test]
    fn wss_and_entrepot_manifests_carry_no_grpc_section() {
        let entrepot = r#"
[collect]
exchange = "hyperliquid"
transport = "entrepot"

[collect.entrepot]
root = "/data/hl-archive"
start = "2023-09-16"
end = "2023-09-17"

[collect.datatypes.orderbook]
enabled = true
depth = 20

[collect.sync]
sync_mode = "on_orderbook"
flush_threshold = 600

[collect.sync.update_frequency]
value = 100
unit = "Millis"

[[workers]]
symbol = "BTC"
"#;
        for (toml, transport) in [
            (MINIMAL_MARKET_TOML, TransportKind::Wss),
            (entrepot, TransportKind::Entrepot),
        ] {
            let manifest = MarketWorkerManifest::from_str(toml).unwrap();
            assert!(manifest.collect.grpc.is_none());
            let cfg = manifest.resolve_all().remove(0);
            assert_eq!(cfg.transport, transport);
            assert!(cfg.grpc.is_none());
        }
    }

    #[test]
    fn transport_forces_the_framework_path_for_entrepot_and_grpc_only() {
        assert!(!TransportKind::Wss.forces_framework());
        assert!(TransportKind::Entrepot.forces_framework());
        assert!(TransportKind::Grpc.forces_framework());
        assert_eq!(TransportKind::default(), TransportKind::Wss);
    }

    #[test]
    fn grpc_transport_parses_its_section_with_defaults() {
        let minimal = resolve(&solana_manifest(""));
        assert_eq!(minimal.transport, TransportKind::Grpc);
        assert_eq!(minimal.grpc, Some(grpc_defaults()));

        let platform = resolve(SOLANA_PLATFORM_TOML);
        assert_eq!(platform.transport, TransportKind::Grpc);
        assert_eq!(
            platform.grpc,
            Some(GrpcSection {
                x_token_env: Some("GRPC_X_TOKEN".to_string()),
                ..grpc_defaults()
            })
        );
        assert_eq!(platform.common.exchange, "solana");
        assert_eq!(platform.common.symbol, JUPITER_V6);
        assert_eq!(platform.common.market_type, MarketType::Onchain);
        assert_eq!(platform.common.environment, VenueEnvironment::Production);
        assert_eq!(platform.sync.sync_mode, SyncMode::OnSlot);
        assert_eq!(platform.sync.period_us(), 1_000_000);
        assert_eq!(
            platform
                .emission_delay
                .as_ref()
                .map(UpdateFrequency::as_micros),
            Some(2_000_000)
        );
        let transactions = &platform.common.datatypes.transactions;
        assert!(transactions.enabled);
        assert_eq!(transactions.match_role, MatchRole::AccountKey);
        assert!(transactions.include_failed);
        assert!(!transactions.logs);
        assert_eq!(
            platform.common.datatypes.declared_set(),
            DeclaredSet::only(DeclaredDatatype::Transactions)
        );
    }

    #[test]
    fn grpc_section_rejects_unknown_keys_preset_and_processed() {
        for (line, named) in [
            ("endpiont = \"https://example.com\"", "endpiont"),
            ("preset = \"publicnode\"", "preset"),
            ("commitment = \"processed\"", "processed"),
        ] {
            let toml = solana_manifest("")
                .replace("[collect.grpc]\n", &format!("[collect.grpc]\n{line}\n"));
            let err = MarketWorkerManifest::from_str(&toml).unwrap_err();
            assert!(matches!(err, ConnectError::Parse(_)), "{line}: {err}");
            assert!(err.to_string().contains(named), "{line}: {err}");
        }
        let unrewritten = solana_manifest("").replace(
            &format!("endpoint = \"{PUBLICNODE}\""),
            "preset = \"publicnode\"",
        );
        let err = MarketWorkerManifest::from_str(&unrewritten).unwrap_err();
        assert!(err.to_string().contains("unknown field `preset`"), "{err}");
        let confirmed = solana_manifest("").replace(
            "[collect.grpc]\n",
            "[collect.grpc]\ncommitment = \"confirmed\"\n",
        );
        assert_eq!(
            resolve(&confirmed).grpc.map(|g| g.commitment),
            Some(Commitment::Confirmed)
        );
    }

    #[test]
    fn transactions_config_rejects_unknown_keys() {
        let with_line = |line: &str| {
            solana_manifest("").replace(
                "[collect.datatypes.transactions]\nenabled = true\n",
                &format!("[collect.datatypes.transactions]\nenabled = true\n{line}\n"),
            )
        };
        let err =
            MarketWorkerManifest::from_str(&with_line("mach = \"signer\"")).unwrap_err();
        assert!(err.to_string().contains("unknown field `mach`"), "{err}");
        let cfg = resolve(&with_line("match = \"signer\""));
        assert_eq!(
            cfg.common.datatypes.transactions.match_role,
            MatchRole::Signer
        );
    }

    #[test]
    fn grpc_section_validate_rejects_zero_limits_bad_endpoint_and_bad_env_names() {
        assert_eq!(grpc_defaults().validate(), Ok(()));

        let mut section = grpc_defaults();
        section.endpoint = "solana-yellowstone-grpc.publicnode.com:443".to_string();
        let err = section.validate().unwrap_err();
        assert!(err.contains("endpoint"), "{err}");
        section.endpoint = "http://127.0.0.1:10000".to_string();
        assert_eq!(section.validate(), Ok(()));

        for name in ["", "GRPC=X_TOKEN", "GRPC\0X_TOKEN"] {
            let mut section = grpc_defaults();
            section.x_token_env = Some(name.to_string());
            let err = section.validate().unwrap_err();
            assert!(err.contains("x_token_env"), "{name:?}: {err}");
        }

        let zeroed = [
            (
                "max_decoding_message_size_mib",
                GrpcSection {
                    max_decoding_message_size_mib: 0,
                    ..grpc_defaults()
                },
            ),
            (
                "ping_interval_secs",
                GrpcSection {
                    ping_interval_secs: 0,
                    ..grpc_defaults()
                },
            ),
            (
                "max_pending_slots",
                GrpcSection {
                    max_pending_slots: 0,
                    ..grpc_defaults()
                },
            ),
            (
                "max_addresses_per_connection",
                GrpcSection {
                    max_addresses_per_connection: 0,
                    ..grpc_defaults()
                },
            ),
            (
                "account_include_max",
                GrpcSection {
                    account_include_max: Some(0),
                    ..grpc_defaults()
                },
            ),
        ];
        for (key, section) in zeroed {
            let err = section.validate().unwrap_err();
            assert!(err.contains(key), "{key}: {err}");
        }
    }

    #[test]
    fn grpc_token_env_must_be_set_and_non_empty() {
        let mut section = grpc_defaults();
        assert_eq!(section.admit_token(|_| None), Ok(()), "no variable named");
        section.x_token_env = Some("GRPC_X_TOKEN".to_string());
        let exported = |name: &str| (name == "GRPC_X_TOKEN").then(|| "token".to_string());
        assert_eq!(section.admit_token(exported), Ok(()));
        for value in [None, Some(String::new())] {
            let err = section.admit_token(|_| value.clone()).unwrap_err();
            assert!(err.contains("GRPC_X_TOKEN"), "{err}");
            assert!(err.contains("unset or empty"), "{err}");
        }
    }

    #[test]
    fn shard_size_is_capped_by_account_include_max() {
        let mut section = grpc_defaults();
        assert_eq!(section.shard_size(), 50);
        section.account_include_max = Some(10);
        assert_eq!(section.shard_size(), 10);
        section.account_include_max = Some(500);
        assert_eq!(section.shard_size(), 50);
        section.max_addresses_per_connection = 5;
        assert_eq!(section.shard_size(), 5);
    }

    #[test]
    fn environment_parses_testnet_and_defaults_production() {
        let manifest = |env: &str| {
            format!(
                r#"
[collect]
exchange = "hyperliquid"
{env}

[collect.datatypes.orderbook]
enabled = true
depth = 20

[collect.sync]
sync_mode = "on_time"
flush_threshold = 600

[collect.sync.update_frequency]
value = 100
unit = "Millis"

[[workers]]
symbol = "BTC"
"#
            )
        };
        let resolve = |env: &str| {
            MarketWorkerManifest::from_str(&manifest(env))
                .unwrap()
                .resolve_all()
                .remove(0)
                .common
                .environment
        };
        assert_eq!(resolve(""), VenueEnvironment::Production);
        assert_eq!(
            resolve("environment = \"testnet\""),
            VenueEnvironment::Testnet
        );
        assert_eq!(
            resolve("environment = \"mainnet\""),
            VenueEnvironment::Production,
            "mainnet is the documented alias for production"
        );
    }

    #[test]
    fn reconcile_section_parses_with_defaults_and_resolves() {
        let toml = r#"
[collect]
exchange = "binance"
framework_ingest = true

[collect.datatypes.orderbook]
enabled = true
depth = 25

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_time"
flush_threshold = 600

[collect.sync.update_frequency]
value = 100
unit = "Millis"

[collect.reconcile]
enabled = true

[[workers]]
symbol = "BTCUSDT"
"#;
        let m = MarketWorkerManifest::from_str(toml).unwrap();
        let cfgs = m.resolve_all();
        let r = cfgs[0].reconcile.as_ref().expect("section resolved");
        assert!(r.enabled);
        assert_eq!(r.emission_delay_us(), 1_000_000, "default W = 1s");
        assert_eq!(r.sweep_secs, 60, "default sweep");
    }

    #[test]
    fn reconcile_absent_resolves_none() {
        let m = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        assert!(m.resolve_all()[0].reconcile.is_none());
    }

    const MINIMAL_MARKET_TOML: &str = r#"
[collect]
exchange = "bybit"

[collect.datatypes.orderbook]
enabled = true
depth   = 50

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_trade"
flush_threshold = 36000

[collect.sync.update_frequency]
value = 100
unit  = "Millis"

[[workers]]
symbol = "BTCUSDT"
"#;

    const WITH_METADATA_MARKET_TOML: &str = r#"
[metadata]
manifest_id = "mfst_mkt_001"
binding_id  = "bnd_mkt_001"
service_id  = "svc_market"

[collect]
exchange = "bybit"

[collect.datatypes.orderbook]
enabled = true
depth   = 50

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_trade"
flush_threshold = 36000

[collect.sync.update_frequency]
value = 100
unit  = "Millis"

[[workers]]
symbol = "BTCUSDT"

[[workers]]
symbol = "ETHUSDT"
sync_mode = "on_orderbook"
"#;

    #[test]
    fn from_str_minimal_market() {
        let manifest = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        assert_eq!(manifest.collect.exchange, "bybit");
        assert_eq!(manifest.workers.len(), 1);
        assert_eq!(manifest.workers[0].symbol, "BTCUSDT");
        assert!(manifest.metadata.is_none());
    }

    #[test]
    fn from_str_with_metadata_market() {
        let manifest = MarketWorkerManifest::from_str(WITH_METADATA_MARKET_TOML).unwrap();
        let meta = manifest.metadata.as_ref().unwrap();
        assert_eq!(meta.manifest_id.as_deref(), Some("mfst_mkt_001"));
        assert_eq!(meta.binding_id.as_deref(), Some("bnd_mkt_001"));
        assert_eq!(meta.service_id.as_deref(), Some("svc_market"));
        assert_eq!(manifest.workers.len(), 2);
    }

    #[test]
    fn from_str_resolves_with_sync_override() {
        let manifest = MarketWorkerManifest::from_str(WITH_METADATA_MARKET_TOML).unwrap();
        let configs = manifest.resolve_all();
        assert_eq!(configs.len(), 2);
        // First worker inherits sync_mode from collect
        assert_eq!(configs[0].sync.sync_mode, SyncMode::OnTrade);
        // Second worker overrides sync_mode
        assert_eq!(configs[1].sync.sync_mode, SyncMode::OnOrderbook);
    }

    #[test]
    fn from_str_invalid_toml() {
        let result = MarketWorkerManifest::from_str("not valid toml");
        assert!(result.is_err());
    }

    #[test]
    fn period_us_millis() {
        let manifest = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        // 100 Millis = 100_000 us
        assert_eq!(manifest.collect.sync.period_us(), 100_000);
    }

    #[test]
    fn framework_ingest_reads_from_collect_and_resolves_overrides() {
        // Absent in TOML → false on collect and every resolved worker.
        let m = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        assert!(!m.collect.framework_ingest);
        assert!(!m.resolve_all()[0].framework_ingest);

        // `framework_ingest = true` at the `[collect]` level (exactly where the
        // the platform manifest producer injects it) is read by the Market
        // manifest; a per-worker entry overrides it back to false.
        let toml = r#"
[collect]
exchange = "binance"
framework_ingest = true

[collect.datatypes.orderbook]
enabled = true
depth = 50

[collect.sync]
sync_mode = "on_trade"
flush_threshold = 36000

[collect.sync.update_frequency]
value = 100
unit  = "Millis"

[[workers]]
symbol = "BTCUSDT"

[[workers]]
symbol = "ETHUSDT"
framework_ingest = false
"#;
        let m = MarketWorkerManifest::from_str(toml).unwrap();
        assert!(m.collect.framework_ingest);
        let cfgs = m.resolve_all();
        assert!(cfgs[0].framework_ingest, "inherits collect");
        assert!(!cfgs[1].framework_ingest, "per-worker override wins");
    }

    /// Regression guard: a stray `framework_ingest` under `[collect.sync]` (the
    /// old, broken location) must NOT bind — `SyncSection` ignores the unknown
    /// key and the flag stays at its `[collect]`-level default of false.
    #[test]
    fn framework_ingest_under_sync_is_ignored() {
        let toml = r#"
[collect]
exchange = "binance"

[collect.datatypes.orderbook]
enabled = true
depth = 50

[collect.sync]
sync_mode = "on_trade"
flush_threshold = 36000
framework_ingest = true

[collect.sync.update_frequency]
value = 100
unit  = "Millis"

[[workers]]
symbol = "BTCUSDT"
"#;
        let m = MarketWorkerManifest::from_str(toml).unwrap();
        assert!(
            !m.collect.framework_ingest,
            "framework_ingest under [collect.sync] is no longer a recognized key"
        );
        assert!(!m.resolve_all()[0].framework_ingest);
    }

    #[cfg(not(feature = "grpc"))]
    #[test]
    fn valid_solana_worker_without_an_adapter_names_the_grpc_feature() {
        let config = resolve(&solana_manifest(""));
        assert_eq!(refusal(&config), GRPC_FEATURE);
        assert_eq!(
            config.admit().unwrap_err().to_string(),
            format!("worker build failed: {GRPC_FEATURE}")
        );
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn valid_solana_worker_is_admitted_with_the_grpc_feature() {
        let manifest = MarketWorkerManifest::from_str(&solana_manifest("")).unwrap();
        let admitted = manifest.resolve_admitted().unwrap();
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].common.symbol, JUPITER_V6);
        assert_eq!(admitted[0].transport, TransportKind::Grpc);
    }

    #[test]
    fn solana_rejects_on_time_with_the_late_row_reason() {
        let on_time = solana_manifest("")
            .replace("sync_mode = \"on_slot\"", "sync_mode = \"on_time\"");
        let reason = refusal(&resolve(&on_time));
        assert!(reason.contains("drops them as late"), "{reason}");
        assert!(reason.contains("set sync_mode = \"on_slot\""), "{reason}");
        for mode in ["on_orderbook", "on_trade", "on_liquidation"] {
            let toml = solana_manifest("").replace(
                "sync_mode = \"on_slot\"",
                &format!("sync_mode = \"{mode}\""),
            );
            let reason = refusal(&resolve(&toml));
            assert_eq!(
                reason,
                format!(
                    "sync_mode = \"{mode}\" is not available for exchange = \"solana\": \
                     set sync_mode = \"on_slot\""
                )
            );
        }
    }

    #[test]
    fn solana_rejects_folded_or_pair_shaped_symbols() {
        for symbol in [JUPITER_V6.to_lowercase(), "SOLUSDC".to_string()] {
            let toml = solana_manifest("").replace(JUPITER_V6, &symbol);
            let reason = refusal(&resolve(&toml));
            assert!(
                reason.starts_with(&format!("solana address '{symbol}' is not base58")),
                "{reason}"
            );
            assert!(reason.contains("set symbol"), "{reason}");
        }
        let short = solana_manifest("").replace(JUPITER_V6, "JUP6");
        assert!(refusal(&resolve(&short)).contains("expected 32"));
        let padded = solana_manifest("").replace(JUPITER_V6, &format!(" {JUPITER_V6}"));
        assert!(refusal(&resolve(&padded)).contains("is not base58"));
    }

    #[test]
    fn solana_rejects_a_reject_listed_address() {
        let toml = solana_manifest("").replace(
            "[collect.grpc]\n",
            &format!("[collect.grpc]\naccount_include_reject = [\"{JUPITER_V6}\"]\n"),
        );
        let reason = refusal(&resolve(&toml));
        assert!(reason.contains("account_include_reject"), "{reason}");
        assert!(reason.contains(JUPITER_V6), "{reason}");
        let other = solana_manifest("").replace(
            "[collect.grpc]\n",
            &format!("[collect.grpc]\naccount_include_reject = [\"{SYSTEM_PROGRAM}\"]\n"),
        );
        assert_admitted_or_names_the_grpc_feature(&resolve(&other));
    }

    #[test]
    fn solana_requires_the_grpc_transport_and_section() {
        let wss = solana_manifest("").replace("transport = \"grpc\"\n", "");
        let reason = refusal(&resolve(&wss));
        assert!(reason.contains("set transport = \"grpc\""), "{reason}");
        let entrepot = solana_manifest("")
            .replace("transport = \"grpc\"", "transport = \"entrepot\"");
        assert!(refusal(&resolve(&entrepot)).contains("set transport = \"grpc\""));
        let no_section = solana_manifest("").replace(
            &format!("[collect.grpc]\nendpoint = \"{PUBLICNODE}\"\n"),
            "",
        );
        assert_eq!(
            refusal(&resolve(&no_section)),
            "transport = \"grpc\" requires a [collect.grpc] section"
        );
    }

    #[test]
    fn solana_rejects_enabled_reconcile_before_cadence_checks() {
        let toml = solana_manifest("\n[collect.reconcile]\nenabled = true\n").replace(
            "value = 1\nunit = \"Secs\"",
            "value = 20\nunit = \"Millis\"",
        );
        let config = resolve(&toml);
        let reason = refusal(&config);
        assert_eq!(
            reason,
            "[collect.reconcile] is not available for exchange = \"solana\": remove the \
             section or set enabled = false"
        );
        match crate::workers::market_worker::MarketWorker::from_config(config) {
            Ok(_) => panic!("reconcile must not build for a chain venue"),
            Err(e) => assert_eq!(e.to_string(), format!("worker build failed: {reason}")),
        }
        let disabled = solana_manifest("\n[collect.reconcile]\nenabled = false\n");
        assert_admitted_or_names_the_grpc_feature(&resolve(&disabled));
    }

    #[test]
    fn solana_rejects_an_unset_token_env() {
        let name = format!("AETELIER_UNSET_GRPC_TOKEN_{}", std::process::id());
        assert!(std::env::var_os(&name).is_none());
        let toml = solana_manifest("").replace(
            "[collect.grpc]\n",
            &format!("[collect.grpc]\nx_token_env = \"{name}\"\n"),
        );
        assert_eq!(
            refusal(&resolve(&toml)),
            format!(
                "[collect.grpc] x_token_env = \"{name}\" names an unset or empty \
                 variable: export {name} with the provider token"
            )
        );
    }

    #[test]
    fn solana_requires_a_declared_datatype() {
        let none = solana_manifest("").replace(
            "[collect.datatypes.transactions]\nenabled = true\n",
            "[collect.datatypes.transactions]\nenabled = false\n",
        );
        let override_without_enabled = format!(
            "{}\n[workers.datatypes.transactions]\nmatch = \"signer\"\n",
            solana_manifest("")
        );
        for toml in [none, override_without_enabled] {
            let reason = refusal(&resolve(&toml));
            assert!(reason.contains("declares no datatype"), "{reason}");
        }
        let override_restating_enabled = format!(
            "{}\n[workers.datatypes.transactions]\nenabled = true\nmatch = \"signer\"\n",
            solana_manifest("")
        );
        let config = resolve(&override_restating_enabled);
        assert_eq!(
            config.common.datatypes.transactions.match_role,
            MatchRole::Signer
        );
        assert_admitted_or_names_the_grpc_feature(&config);
    }

    #[test]
    fn solana_environment_accepts_mainnet_and_testnet_and_devnet_fails_parse() {
        for (environment, expected) in [
            ("mainnet", VenueEnvironment::Production),
            ("production", VenueEnvironment::Production),
            ("testnet", VenueEnvironment::Testnet),
        ] {
            let config = resolve(&solana_manifest(&format!(
                "environment = \"{environment}\""
            )));
            assert_eq!(config.common.environment, expected);
            assert_admitted_or_names_the_grpc_feature(&config);
        }
        let err =
            MarketWorkerManifest::from_str(&solana_manifest("environment = \"devnet\""))
                .unwrap_err();
        assert!(matches!(err, ConnectError::Parse(_)), "{err}");
        assert!(err.to_string().contains("devnet"), "{err}");
    }

    #[test]
    fn cex_rejects_grpc_on_slot_onchain_and_transactions() {
        let bybit =
            |from: &str, to: &str| resolve(&MINIMAL_MARKET_TOML.replace(from, to));
        let exchange = "exchange = \"bybit\"";
        assert_eq!(
            refusal(&bybit(
                exchange,
                "exchange = \"bybit\"\ntransport = \"grpc\""
            )),
            "transport = \"grpc\" is only available for exchange = \"solana\""
        );
        assert_eq!(
            refusal(&bybit(
                "sync_mode = \"on_trade\"",
                "sync_mode = \"on_slot\""
            )),
            "sync_mode = \"on_slot\" is only available for exchange = \"solana\": venue \
             'bybit' produces no slots"
        );
        assert_eq!(
            refusal(&bybit(
                exchange,
                "exchange = \"bybit\"\nmarket_type = \"onchain\""
            )),
            "venue 'bybit' has no onchain market — set `market_type` to \"spot\", \
             \"perpetual\" or \"inverse\""
        );
        let transactions = format!(
            "{MINIMAL_MARKET_TOML}\n[collect.datatypes.transactions]\nenabled = true\n"
        );
        let reason = refusal(&resolve(&transactions));
        assert!(
            reason.contains("venue 'bybit' delivers no transactions"),
            "{reason}"
        );
    }

    #[test]
    fn cex_manifests_admit_unchanged() {
        let on_orderbook = "sync_mode = \"on_orderbook\"";
        let on_trade = "sync_mode = \"on_trade\"";
        let on_time = "sync_mode = \"on_time\"";
        let hl_worker = "\n[[workers]]\nsymbol = \"BTC\"\n";
        let btc_worker = "\n[[workers]]\nsymbol = \"BTCUSDT\"\n";
        let reconcile = "\n[collect.reconcile]\nenabled = true\n";
        let hold_back = "\n[collect.emission_delay]\nvalue = 250\nunit = \"Millis\"\n";
        let entrepot_local = r#"transport = "entrepot"

[collect.entrepot]
root = "/data/hl-archive"
start = "2023-09-16"
end = "2023-09-17""#;
        let entrepot_s3 = r#"transport = "entrepot"

[collect.entrepot]
source = "s3"
bucket = "hyperliquid-archive"
region = "us-east-1"
requester_pays = true
start = "2023-09-16"
end = "2023-09-17"
cursor = "/var/lib/aetelier/entrepot/btc.cursor"
fetch_concurrency = 4"#;
        let entrepot_anonymous = r#"transport = "entrepot"

[collect.entrepot]
source = "s3"
bucket = "public-mirror"
region = "eu-west-1"
anonymous = true
endpoint = "https://s3-eu-west-1.amazonaws.com/public-mirror"
start = "2023-09-16"
end = "2023-09-16""#;
        let two_workers = "\n[[workers]]\nsymbol = \"BTCUSDT\"\n\n[[workers]]\nsymbol = \
                           \"ETHUSDT\"\nframework_ingest = false\n";
        let manifests = [
            MINIMAL_MARKET_TOML.to_string(),
            WITH_METADATA_MARKET_TOML.to_string(),
            cex_manifest("hyperliquid", "", on_orderbook, hl_worker),
            cex_manifest("hyperliquid", entrepot_local, on_orderbook, hl_worker),
            cex_manifest("hyperliquid", entrepot_s3, on_orderbook, hl_worker),
            cex_manifest("hyperliquid", entrepot_anonymous, on_orderbook, hl_worker),
            cex_manifest("hyperliquid", "", on_time, hl_worker),
            cex_manifest(
                "hyperliquid",
                "environment = \"testnet\"",
                on_time,
                hl_worker,
            ),
            cex_manifest(
                "hyperliquid",
                "environment = \"mainnet\"",
                on_time,
                hl_worker,
            ),
            cex_manifest(
                "binance",
                "framework_ingest = true",
                on_time,
                &format!("{reconcile}{btc_worker}"),
            ),
            cex_manifest("binance", "framework_ingest = true", on_trade, two_workers),
            cex_manifest(
                "binance",
                "",
                &format!("{on_trade}\nframework_ingest = true"),
                btc_worker,
            ),
            cex_manifest(
                "binance",
                "framework_ingest = true",
                on_time,
                &format!("{reconcile}{btc_worker}"),
            )
            .replace("value = 100", "value = 20"),
            cex_manifest(
                "binance",
                "framework_ingest = false",
                on_time,
                &format!("{reconcile}{btc_worker}"),
            ),
            cex_manifest(
                "binance",
                "framework_ingest = true",
                on_time,
                &format!("{hold_back}{btc_worker}"),
            ),
            cex_manifest(
                "binance",
                "framework_ingest = true",
                on_time,
                &format!("{hold_back}{reconcile}{btc_worker}"),
            ),
        ];
        for toml in &manifests {
            let manifest = MarketWorkerManifest::from_str(toml)
                .unwrap_or_else(|e| panic!("{e}\n{toml}"));
            let configs = manifest.resolve_all();
            for config in &configs {
                assert!(config.admit().is_ok(), "{:?}\n{toml}", config.admit());
            }
            let admitted = manifest.resolve_admitted().unwrap();
            assert_eq!(admitted.len(), configs.len());
        }
    }

    #[test]
    fn unknown_exchange_keeps_its_error() {
        let config = resolve(
            &MINIMAL_MARKET_TOML.replace("exchange = \"bybit\"", "exchange = \"ftx\""),
        );
        assert!(config.admit().is_ok());
        match crate::workers::market_worker::MarketWorker::from_config(config) {
            Ok(_) => panic!("an unknown exchange must not build"),
            Err(ConnectError::Exchange(reason)) => {
                assert_eq!(reason, "unsupported exchange: ftx")
            }
            Err(e) => panic!("expected the unknown exchange error, got {e}"),
        }
    }

    #[test]
    fn resolve_admitted_refuses_before_any_worker_is_built() {
        let toml = format!(
            "{MINIMAL_MARKET_TOML}\n[[workers]]\nsymbol = \"ETHUSDT\"\nmarket_type = \
             \"onchain\"\n"
        );
        let manifest = MarketWorkerManifest::from_str(&toml).unwrap();
        let configs = manifest.resolve_all();
        assert_eq!(configs.len(), 2);
        assert!(configs[0].admit().is_ok());
        let err = manifest.resolve_admitted().unwrap_err();
        assert_eq!(err.to_string(), configs[1].admit().unwrap_err().to_string());
        assert!(err.to_string().contains("no onchain market"), "{err}");
        let valid = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        assert_eq!(valid.resolve_admitted().unwrap().len(), 1);
    }

    #[test]
    fn solana_manifest_rejects_duplicate_addresses() {
        let duplicate = format!(
            "{}\n[[workers]]\nsymbol = \"{JUPITER_V6}\"\n",
            solana_manifest("")
        );
        let err = MarketWorkerManifest::from_str(&duplicate).unwrap_err();
        assert!(matches!(err, ConnectError::Parse(_)), "{err}");
        assert!(err.to_string().contains(JUPITER_V6), "{err}");
        assert!(err.to_string().contains("more than one"), "{err}");
        let distinct = format!(
            "{}\n[[workers]]\nsymbol = \"{SYSTEM_PROGRAM}\"\n",
            solana_manifest("")
        );
        assert_eq!(
            MarketWorkerManifest::from_str(&distinct)
                .unwrap()
                .resolve_all()
                .len(),
            2
        );
        let unparseable = solana_manifest("").replace(JUPITER_V6, "SOLUSDC");
        let unparseable = format!("{unparseable}\n[[workers]]\nsymbol = \"SOLUSDC\"\n");
        let manifest = MarketWorkerManifest::from_str(&unparseable).unwrap();
        assert!(manifest.resolve_admitted().is_err());
    }

    #[test]
    fn cex_manifest_keeps_duplicate_symbols() {
        let toml = format!("{MINIMAL_MARKET_TOML}\n[[workers]]\nsymbol = \"BTCUSDT\"\n");
        let manifest = MarketWorkerManifest::from_str(&toml).unwrap();
        let configs = manifest.resolve_admitted().unwrap();
        assert_eq!(configs.len(), 2);
        assert!(configs.iter().all(|c| c.common.symbol == "BTCUSDT"));
    }

    #[test]
    fn solana_manifest_rejects_emission_delay_under_sync() {
        let under_sync = "[collect.sync.emission_delay]\nvalue = 2\nunit = \"Secs\"\n\n\
                          [collect.sync.update_frequency]";
        let toml =
            solana_manifest("").replace("[collect.sync.update_frequency]", under_sync);
        let err = MarketWorkerManifest::from_str(&toml).unwrap_err();
        assert!(
            err.to_string()
                .contains("unknown field `emission_delay` in [collect.sync]"),
            "{err}"
        );
        let at_collect =
            solana_manifest("\n[collect.emission_delay]\nvalue = 2\nunit = \"Secs\"\n");
        let config = resolve(&at_collect);
        assert_eq!(
            config
                .emission_delay
                .as_ref()
                .map(UpdateFrequency::as_micros),
            Some(2_000_000)
        );
        let cex =
            MINIMAL_MARKET_TOML.replace("[collect.sync.update_frequency]", under_sync);
        assert!(MarketWorkerManifest::from_str(&cex).is_ok());
    }

    #[test]
    fn solana_manifest_rejects_match_directly_under_workers() {
        for line in [
            "match = \"signer\"",
            "include_failed = false",
            "logs = true",
        ] {
            let toml = format!("{}{line}\n", solana_manifest(""));
            let err = MarketWorkerManifest::from_str(&toml).unwrap_err();
            let key = line.split(' ').next().unwrap();
            assert!(
                err.to_string()
                    .contains(&format!("unknown field `{key}` in [[workers]]")),
                "{err}"
            );
        }
        let cex = format!("{MINIMAL_MARKET_TOML}match = \"signer\"\n");
        assert!(MarketWorkerManifest::from_str(&cex).is_ok());
    }

    #[test]
    fn solana_manifest_accepts_every_read_key() {
        let toml = format!(
            "{}exchange = \"solana\"\nmarket_type = \"onchain\"\nsync_mode = \
             \"on_slot\"\nframework_ingest = true\n\n[workers.datatypes.transactions]\n\
             enabled = true\n\n[[workers.output]]\ntype = \"channel\"\n",
            solana_manifest("")
        );
        let table: toml::Table = toml::from_str(&toml).unwrap();
        let sync = table["collect"]["sync"].as_table().unwrap();
        let worker = table["workers"][0].as_table().unwrap();
        assert!(SYNC_KEYS.iter().all(|key| sync.contains_key(*key)));
        assert!(WORKER_KEYS.iter().all(|key| worker.contains_key(*key)));
        assert_eq!(sync.len(), SYNC_KEYS.len());
        assert_eq!(worker.len(), WORKER_KEYS.len());
        let config = resolve(&toml);
        assert!(config.framework_ingest);
        assert_eq!(config.sync.sync_mode, SyncMode::OnSlot);
        assert_admitted_or_names_the_grpc_feature(&config);
    }
}
