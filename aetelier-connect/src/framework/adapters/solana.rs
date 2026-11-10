use std::sync::LazyLock;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use aetelier_types::config::markets::market_config::{
    DataTypesSection, DeclaredDatatype, DeclaredSet, TransactionsConfig,
};
use aetelier_types::exchanges::{MarketType, VenueEnvironment};
use aetelier_types::onchain::{Chain, ChainAddress, Commitment};

use crate::config::workers::common::ReconnectSection;
use crate::config::workers::market_worker_config::GrpcSection;
use crate::errors::ExchangeError;
use crate::framework::budget::{ConnectionBudget, RateWindow, SourceMetrics};
use crate::framework::feed::FeedDatatype;
use crate::framework::grpc::{
    ATTACH_QUEUE_BANKS, AttachSpec, GrpcAttach, GrpcConnections, PROTOCOL_REVISION,
    XToken, drive_grpc, rejection, replay_frames,
};
use crate::framework::model::{DomainEvent, ReconstructionModel};
use crate::framework::onchain::WatchedAddress;
use crate::framework::registry::{
    ExchangeAdapter, ExchangeProfile, ReplayFrame, TaskExit,
};
use crate::framework::symbol::SymbolCodec;

pub const SOLANA_EMISSION_DELAY_US: u64 = 2_000_000;

pub const SLOT_TICK_CADENCE: Duration = Duration::from_secs(5);

pub const CONNECT_ATTEMPT_RATE: RateWindow = RateWindow {
    max: 10,
    per: Duration::from_secs(60),
};

static SOLANA_PROFILE: LazyLock<ExchangeProfile> = LazyLock::new(|| ExchangeProfile {
    id: "solana",
    symbol_codec: SymbolCodec::ChainAddress {
        chain: Chain::Solana,
    },
    budget: ConnectionBudget {
        max_connections: None,
        max_streams_per_socket: Some(1),
        subscribe_rate: Vec::new(),
        connect_attempt_rate: Some(CONNECT_ATTEMPT_RATE),
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: PROTOCOL_REVISION,
});

struct SolanaLive {
    section: GrpcSection,
    transactions: TransactionsConfig,
    reconnect: Option<ReconnectSection>,
    connections: GrpcConnections,
}

pub struct SolanaAdapter {
    live: Option<SolanaLive>,
}

pub static SOLANA: SolanaAdapter = SolanaAdapter { live: None };

impl SolanaAdapter {
    pub fn live(
        section: GrpcSection,
        transactions: TransactionsConfig,
        reconnect: Option<ReconnectSection>,
        connections: GrpcConnections,
    ) -> Self {
        Self {
            live: Some(SolanaLive {
                section,
                transactions,
                reconnect,
                connections,
            }),
        }
    }

    fn attach(
        &self,
        symbols: &[String],
        metrics: SourceMetrics,
    ) -> Result<GrpcAttach, String> {
        let Some(live) = &self.live else {
            return Err(
                "venue 'solana' streams only through a [collect.grpc] connection"
                    .to_string(),
            );
        };
        let [symbol] = symbols else {
            return Err("venue 'solana' streams one address per worker".to_string());
        };
        let address =
            ChainAddress::parse(Chain::Solana, symbol).map_err(|e| e.to_string())?;
        live.connections
            .attach(AttachSpec {
                venue: "solana",
                section: live.section.clone(),
                x_token: XToken::from_env(&live.section, |name| std::env::var(name).ok()),
                watch: WatchedAddress::new(address, &live.transactions),
                reconnect: live.reconnect.clone(),
                connect_rate: CONNECT_ATTEMPT_RATE,
                queue_banks: ATTACH_QUEUE_BANKS,
                metrics,
            })
            .map_err(|e| e.to_string())
    }
}

impl ExchangeAdapter for SolanaAdapter {
    fn id(&self) -> &'static str {
        "solana"
    }

    fn profile(&self) -> &ExchangeProfile {
        &SOLANA_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        ReconstructionModel::Stream
    }

    fn supported_datatypes(&self) -> &'static [DeclaredDatatype] {
        &[DeclaredDatatype::Transactions]
    }

    fn supported_market_types(&self) -> &'static [MarketType] {
        &[MarketType::Onchain]
    }

    fn default_emission_delay_us(&self) -> Option<u64> {
        Some(SOLANA_EMISSION_DELAY_US)
    }

    fn supports_environment(&self, environment: VenueEnvironment) -> bool {
        matches!(
            environment,
            VenueEnvironment::Production | VenueEnvironment::Testnet
        )
    }

    fn datatype_cadence(&self, datatype: FeedDatatype) -> Option<Duration> {
        match datatype {
            FeedDatatype::Transactions => Some(SLOT_TICK_CADENCE),
            _ => None,
        }
    }

    fn spawn(
        &self,
        symbols: Vec<String>,
        declared: DeclaredSet,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        self.spawn_env(
            VenueEnvironment::Production,
            symbols,
            declared,
            tx,
            shutdown,
            metrics,
        )
    }

    fn spawn_env(
        &self,
        _environment: VenueEnvironment,
        symbols: Vec<String>,
        _declared: DeclaredSet,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        match self.attach(&symbols, metrics) {
            Ok(attach) => tokio::spawn(drive_grpc(attach, tx, shutdown)),
            Err(reason) => {
                tokio::spawn(async move { TaskExit::Failed(rejection(&reason)) })
            }
        }
    }

    fn replay_stream(
        &self,
        symbols: &[String],
        datatypes: &DataTypesSection,
        commitment: Commitment,
        frames: &[ReplayFrame],
        metrics: &SourceMetrics,
    ) -> Result<Vec<DomainEvent>, Box<ExchangeError>> {
        replay_frames(symbols, datatypes, commitment, frames, metrics)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::clients::disconnect::DisconnectReason;
    use crate::framework::grpc::{
        ConnectionKey, GeyserChannel, GeyserDial, GrpcExitReason, GrpcSettings,
        build_grpc_adapter,
    };
    use crate::framework::registry::{admit_registered, registry};
    use aetelier_types::exchanges::Exchange;

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

    const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

    const ENDPOINT: &str = "http://mock.geyser";

    struct SilentDial;

    #[async_trait::async_trait]
    impl GeyserDial for SilentDial {
        async fn dial(
            &self,
            _settings: &GrpcSettings,
        ) -> Result<GeyserChannel, GrpcExitReason> {
            std::future::pending().await
        }
    }

    fn section() -> GrpcSection {
        GrpcSection {
            endpoint: ENDPOINT.to_string(),
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

    fn silent_pool() -> GrpcConnections {
        let dial: Arc<dyn GeyserDial> = Arc::new(SilentDial);
        GrpcConnections::new(dial)
    }

    fn live(pool: &GrpcConnections) -> SolanaAdapter {
        SolanaAdapter::live(section(), TransactionsConfig::default(), None, pool.clone())
    }

    fn transactions() -> DeclaredSet {
        DeclaredSet::only(DeclaredDatatype::Transactions)
    }

    fn rejected(exit: TaskExit) -> String {
        match exit {
            TaskExit::Failed(DisconnectReason::ProtocolRejection { code: 0, reason }) => {
                reason
            }
            other => panic!("expected a code 0 rejection, got {other:?}"),
        }
    }

    async fn exit_of(
        adapter: &SolanaAdapter,
        environment: VenueEnvironment,
        symbols: &[&str],
    ) -> TaskExit {
        let (tx, _rx) = mpsc::channel(8);
        let (_stop, shutdown) = watch::channel(false);
        adapter
            .spawn_env(
                environment,
                symbols.iter().map(|symbol| symbol.to_string()).collect(),
                transactions(),
                tx,
                shutdown,
                SourceMetrics::default(),
            )
            .await
            .expect("the adapter task joins")
    }

    #[test]
    fn solana_registers_under_the_grpc_feature_with_its_venue_defaults() {
        let adapter = admit_registered(Exchange::Solana).expect("registered");
        assert!(std::ptr::addr_eq(
            *registry().get("solana").expect("solana"),
            &SOLANA
        ));
        assert_eq!(adapter.id(), "solana");
        assert_eq!(adapter.profile().id, "solana");
        assert_eq!(
            adapter.supported_datatypes(),
            &[DeclaredDatatype::Transactions]
        );
        assert_eq!(adapter.supported_market_types(), &[MarketType::Onchain]);
        assert_eq!(adapter.default_emission_delay_us(), Some(2_000_000));
        assert!(adapter.supports_environment(VenueEnvironment::Production));
        assert!(adapter.supports_environment(VenueEnvironment::Testnet));
        assert!(matches!(
            adapter.book_model("transactions"),
            ReconstructionModel::Stream
        ));
        for datatype in FeedDatatype::ALL {
            let expected = (datatype == FeedDatatype::Transactions)
                .then_some(Duration::from_secs(5));
            assert_eq!(adapter.datatype_cadence(datatype), expected, "{datatype:?}");
        }
        assert_eq!(
            adapter.profile().protocol_revision,
            "yellowstone-grpc-proto@14.0.1"
        );
    }

    #[tokio::test]
    async fn the_static_adapter_refuses_to_stream_without_a_grpc_section() {
        for environment in [VenueEnvironment::Production, VenueEnvironment::Testnet] {
            assert_eq!(
                rejected(exit_of(&SOLANA, environment, &[JUPITER_V6]).await),
                "venue 'solana' streams only through a [collect.grpc] connection"
            );
        }
        let (tx, _rx) = mpsc::channel(8);
        let (_stop, shutdown) = watch::channel(false);
        let exit = SOLANA
            .spawn(
                vec![JUPITER_V6.to_string()],
                transactions(),
                tx,
                shutdown,
                SourceMetrics::default(),
            )
            .await
            .expect("the adapter task joins");
        assert_eq!(
            rejected(exit),
            "venue 'solana' streams only through a [collect.grpc] connection"
        );
    }

    #[tokio::test]
    async fn a_live_adapter_refuses_two_symbols_and_an_unparseable_one() {
        let pool = silent_pool();
        let adapter = live(&pool);
        for symbols in [vec![JUPITER_V6, SYSTEM_PROGRAM], Vec::new()] {
            assert_eq!(
                rejected(exit_of(&adapter, VenueEnvironment::Production, &symbols).await),
                "venue 'solana' streams one address per worker"
            );
        }
        let folded = JUPITER_V6.to_lowercase();
        for symbol in ["SOLUSDC", folded.as_str()] {
            assert_eq!(
                rejected(
                    exit_of(&adapter, VenueEnvironment::Production, &[symbol]).await
                ),
                format!("solana address '{symbol}' is not base58")
            );
        }
        assert!(pool.connections().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_live_adapter_attaches_through_its_pool_and_detaches_on_shutdown() {
        let pool = silent_pool();
        let adapter = live(&pool);
        let (tx, _rx) = mpsc::channel(8);
        let (stop, shutdown) = watch::channel(false);
        let handle = adapter.spawn_env(
            VenueEnvironment::Testnet,
            vec![JUPITER_V6.to_string()],
            transactions(),
            tx,
            shutdown,
            SourceMetrics::default(),
        );
        let key = ConnectionKey {
            endpoint: ENDPOINT.to_string(),
            commitment: Commitment::Finalized,
            token_env: None,
            shard: 0,
        };
        assert_eq!(pool.connections(), vec![(key, 1)]);
        assert!(stop.send(true).is_ok());
        assert!(matches!(
            handle.await.expect("the adapter task joins"),
            TaskExit::Completed
        ));
        assert!(pool.connections().is_empty());
    }

    #[test]
    fn build_grpc_adapter_builds_solana_only() {
        let pool = silent_pool();
        let reconnect = ReconnectSection::default();
        let built = build_grpc_adapter(
            Exchange::Solana,
            &section(),
            &TransactionsConfig::default(),
            Some(&reconnect),
            pool.clone(),
        )
        .expect("solana streams over gRPC");
        assert_eq!(built.id(), "solana");
        assert_eq!(built.profile().protocol_revision, PROTOCOL_REVISION);
        assert_eq!(built.supported_market_types(), &[MarketType::Onchain]);
        assert_eq!(
            built.default_emission_delay_us(),
            Some(SOLANA_EMISSION_DELAY_US)
        );
        for exchange in Exchange::all().into_iter().filter(|e| e.chain().is_none()) {
            assert!(
                build_grpc_adapter(
                    exchange,
                    &section(),
                    &TransactionsConfig::default(),
                    None,
                    pool.clone(),
                )
                .is_none(),
                "{exchange}"
            );
        }
        assert!(pool.connections().is_empty());
    }
}
