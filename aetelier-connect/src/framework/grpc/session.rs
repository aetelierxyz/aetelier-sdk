use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::time::{Instant, sleep_until};
use tracing::warn;
use url::Url;
use yellowstone_grpc_client::{
    ClientTlsConfig, GeyserGrpcBuilder, GeyserGrpcBuilderError, GeyserGrpcClient,
    GeyserGrpcClientError, GeyserStream, SubscribeRequestSink,
};
use yellowstone_grpc_proto::prelude::{
    SubscribeRequest, SubscribeUpdate, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

use super::request::ping_request;
use super::status::{GrpcExitReason, PendingRequest, ServerVersion};
use crate::config::workers::market_worker_config::GrpcSection;
use crate::framework::budget::SourceMetrics;
use crate::framework::onchain::ReceiptStamp;
use crate::framework::transport::rtt_ewma_us;

pub const STALE_AFTER: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const PING_WINDOW: usize = 64;
const ACK_GRACE_PINGS: u32 = 2;

#[derive(Clone, PartialEq, Eq)]
pub struct XToken(String);

impl XToken {
    pub fn new(token: String) -> Self {
        Self(token)
    }

    pub fn from_env(
        section: &GrpcSection,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Option<Self> {
        let name = section.x_token_env.as_deref()?;
        lookup(name).filter(|token| !token.is_empty()).map(Self)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for XToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("XToken(..)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcSettings {
    pub endpoint: String,
    pub x_token: Option<XToken>,
    pub accept_zstd: bool,
    pub max_decoding_message_size: usize,
    pub ping_interval: Duration,
    pub stale_after: Duration,
}

impl GrpcSettings {
    pub fn new(section: &GrpcSection, x_token: Option<XToken>) -> Self {
        Self {
            endpoint: section.endpoint.clone(),
            x_token,
            accept_zstd: section.accept_zstd,
            max_decoding_message_size: section
                .max_decoding_message_size_mib
                .saturating_mul(1 << 20),
            ping_interval: Duration::from_secs(section.ping_interval_secs),
            stale_after: STALE_AFTER,
        }
    }

    pub fn builder(&self) -> Result<GeyserGrpcBuilder, GrpcExitReason> {
        let token = self.x_token.as_ref().map(XToken::expose);
        let mut builder = GeyserGrpcBuilder::from_shared(self.endpoint.clone())
            .and_then(|builder| builder.x_token(token))
            .map_err(setup_error)?
            .connect_timeout(CONNECT_TIMEOUT)
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .tcp_nodelay(true)
            .max_decoding_message_size(self.max_decoding_message_size);
        if self.accept_zstd {
            builder = builder.accept_compressed(CompressionEncoding::Zstd);
        }
        if self.endpoint.starts_with("https://") {
            builder = builder
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(setup_error)?;
        }
        Ok(builder)
    }

    pub fn endpoint_host(&self) -> String {
        let Ok(url) = Url::parse(&self.endpoint) else {
            return "unparsable endpoint".to_string();
        };
        let host = url.host_str().unwrap_or_default();
        match url.port() {
            Some(port) => format!("{}://{host}:{port}", url.scheme()),
            None => format!("{}://{host}", url.scheme()),
        }
    }
}

pub enum GeyserChannel {
    Client(GeyserGrpcClient),
    Opened {
        requests: SubscribeRequestSink,
        updates: GeyserStream,
    },
}

#[async_trait::async_trait]
pub trait GeyserDial: Send + Sync + 'static {
    async fn dial(
        &self,
        settings: &GrpcSettings,
    ) -> Result<GeyserChannel, GrpcExitReason>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NetworkDial;

#[async_trait::async_trait]
impl GeyserDial for NetworkDial {
    async fn dial(
        &self,
        settings: &GrpcSettings,
    ) -> Result<GeyserChannel, GrpcExitReason> {
        settings
            .builder()?
            .connect()
            .await
            .map(GeyserChannel::Client)
            .map_err(|error| GrpcExitReason::Connect(error.to_string()))
    }
}

#[derive(Debug)]
pub enum SessionEvent {
    Update {
        update: SubscribeUpdate,
        receipt: ReceiptStamp,
    },
    Acknowledged {
        ack: i32,
        recv_us: u64,
        answered: bool,
    },
}

pub struct GrpcSession {
    sink: SubscribeRequestSink,
    stream: GeyserStream,
    server: ServerVersion,
    client: Option<GeyserGrpcClient>,
    pings: PingClock,
    ping_interval: Duration,
    ping_at: Instant,
    stale_after: Duration,
    stale_at: Instant,
    ack_grace: Duration,
    unacknowledged: VecDeque<(i32, PendingRequest, Instant)>,
    acknowledged: VecDeque<SessionEvent>,
    opening_ack: i32,
    recv_seq: u64,
    conn_epoch_us: u64,
}

impl GrpcSession {
    pub async fn open(
        channel: GeyserChannel,
        request: SubscribeRequest,
        settings: &GrpcSettings,
        epochs: &SourceMetrics,
    ) -> Result<Self, GrpcExitReason> {
        let (sink, stream, server, client) = match channel {
            GeyserChannel::Client(mut client) => {
                let server = match client.get_version().await {
                    Ok(response) => ServerVersion::parse(&response.version),
                    Err(error) => {
                        warn!(
                            endpoint = %settings.endpoint_host(),
                            %error,
                            "framework.grpc.version_unavailable"
                        );
                        ServerVersion::default()
                    }
                };
                let (sink, stream) = client
                    .subscribe_with_request(Some(request))
                    .await
                    .map_err(subscribe_error)?;
                (sink, stream, server, Some(client))
            }
            GeyserChannel::Opened {
                mut requests,
                updates,
            } => {
                send(&mut requests, request)?;
                (requests, updates, ServerVersion::default(), None)
            }
        };
        let conn_epoch_us = epochs.next_conn_epoch_us();
        let now = Instant::now();
        let mut session = Self {
            sink,
            stream,
            server,
            client,
            pings: PingClock::default(),
            ping_interval: settings.ping_interval,
            ping_at: now + settings.ping_interval,
            stale_after: settings.stale_after,
            stale_at: now + settings.stale_after,
            ack_grace: settings.ping_interval * ACK_GRACE_PINGS,
            unacknowledged: VecDeque::new(),
            acknowledged: VecDeque::new(),
            opening_ack: 0,
            recv_seq: 0,
            conn_epoch_us,
        };
        session.opening_ack = session.acknowledge(PendingRequest::Subscribe)?;
        Ok(session)
    }

    pub fn server(&self) -> &ServerVersion {
        &self.server
    }

    pub fn client(&self) -> Option<&GeyserGrpcClient> {
        self.client.as_ref()
    }

    pub fn conn_epoch_us(&self) -> u64 {
        self.conn_epoch_us
    }

    pub fn rtt_us(&self) -> u64 {
        self.pings.rtt_us
    }

    pub fn opening_ack(&self) -> i32 {
        self.opening_ack
    }

    pub fn pending(&self) -> Option<PendingRequest> {
        let waiting = |kind: PendingRequest| {
            self.unacknowledged
                .iter()
                .any(|&(_, request, _)| request == kind)
        };
        [PendingRequest::Update, PendingRequest::Subscribe]
            .into_iter()
            .find(|&kind| waiting(kind))
    }

    pub fn update(&mut self, request: SubscribeRequest) -> Result<i32, GrpcExitReason> {
        send(&mut self.sink, request)?;
        self.acknowledge(PendingRequest::Update)
    }

    pub async fn next(&mut self) -> Result<SessionEvent, GrpcExitReason> {
        loop {
            if let Some(event) = self.acknowledged.pop_front() {
                return Ok(event);
            }
            let now = Instant::now();
            if now >= self.stale_at {
                return Err(GrpcExitReason::Stale {
                    silence: self.stale_after,
                });
            }
            if let Some(&(ack, _, sent_at)) = self.unacknowledged.front()
                && now >= sent_at + self.ack_grace
            {
                self.unacknowledged.pop_front();
                return Ok(SessionEvent::Acknowledged {
                    ack,
                    recv_us: now_us(),
                    answered: false,
                });
            }
            if now >= self.ping_at {
                let id = self.pings.issue(now_us());
                send(&mut self.sink, ping_request(id))?;
                self.ping_at += self.ping_interval;
                if self.ping_at <= now {
                    self.ping_at = now + self.ping_interval;
                }
            }
            let wake = self
                .unacknowledged
                .front()
                .map_or(self.stale_at, |&(_, _, sent_at)| sent_at + self.ack_grace)
                .min(self.ping_at)
                .min(self.stale_at);
            tokio::select! {
                biased;
                item = self.stream.next() => {
                    let recv_us = now_us();
                    match item {
                        None => return Err(GrpcExitReason::Ended),
                        Some(Err(status)) => {
                            return Err(GrpcExitReason::Status {
                                status,
                                pending: self.pending(),
                            });
                        }
                        Some(Ok(update)) => {
                            if let Some(event) = self.receive(update, recv_us)? {
                                return Ok(event);
                            }
                        }
                    }
                }
                () = sleep_until(wake) => {}
            }
        }
    }

    fn acknowledge(&mut self, request: PendingRequest) -> Result<i32, GrpcExitReason> {
        let ack = self.pings.issue(now_us());
        send(&mut self.sink, ping_request(ack))?;
        self.unacknowledged
            .push_back((ack, request, Instant::now()));
        Ok(ack)
    }

    fn receive(
        &mut self,
        update: SubscribeUpdate,
        recv_us: u64,
    ) -> Result<Option<SessionEvent>, GrpcExitReason> {
        match &update.update_oneof {
            Some(UpdateOneof::Ping(_)) => {
                let id = self.pings.issue(recv_us);
                send(&mut self.sink, ping_request(id))?;
                return Ok(None);
            }
            Some(UpdateOneof::Pong(pong)) => {
                self.pings.answer(pong.id, recv_us);
                if let Some(position) = self
                    .unacknowledged
                    .iter()
                    .position(|&(ack, _, _)| ack == pong.id)
                {
                    for (ack, _, _) in self.unacknowledged.drain(..=position) {
                        self.acknowledged.push_back(SessionEvent::Acknowledged {
                            ack,
                            recv_us,
                            answered: true,
                        });
                    }
                }
                return Ok(None);
            }
            Some(UpdateOneof::BlockMeta(_)) => {
                self.stale_at = Instant::now() + self.stale_after;
            }
            _ => {}
        }
        self.recv_seq += 1;
        Ok(Some(SessionEvent::Update {
            update,
            receipt: ReceiptStamp {
                local_tx_ts_us: recv_us,
                source_tx_rtt_us: self.pings.rtt_us,
                recv_seq: self.recv_seq,
                conn_epoch_us: self.conn_epoch_us,
            },
        }))
    }
}

#[derive(Debug, Default)]
struct PingClock {
    last_id: i32,
    sent: VecDeque<(i32, u64)>,
    rtt_us: u64,
}

impl PingClock {
    fn issue(&mut self, sent_us: u64) -> i32 {
        self.last_id = self.last_id.checked_add(1).unwrap_or(1);
        if self.sent.len() == PING_WINDOW {
            self.sent.pop_front();
        }
        self.sent.push_back((self.last_id, sent_us));
        self.last_id
    }

    fn answer(&mut self, id: i32, recv_us: u64) -> Option<u64> {
        let position = self.sent.iter().position(|&(sent, _)| sent == id)?;
        let (_, sent_us) = self.sent.drain(..=position).next_back()?;
        let sample = recv_us.saturating_sub(sent_us).max(1);
        self.rtt_us = rtt_ewma_us(self.rtt_us, sample);
        Some(sample)
    }
}

fn send(
    sink: &mut SubscribeRequestSink,
    request: SubscribeRequest,
) -> Result<(), GrpcExitReason> {
    match sink.send(request).now_or_never() {
        Some(Ok(())) => Ok(()),
        Some(Err(error)) => Err(GrpcExitReason::RequestFailed(error.to_string())),
        None => Err(GrpcExitReason::RequestFailed(
            "request channel full".to_string(),
        )),
    }
}

fn setup_error(error: GeyserGrpcBuilderError) -> GrpcExitReason {
    GrpcExitReason::Setup(error.to_string())
}

fn subscribe_error(error: GeyserGrpcClientError) -> GrpcExitReason {
    match error {
        GeyserGrpcClientError::TonicStatus(status) => GrpcExitReason::Status {
            status,
            pending: Some(PendingRequest::Subscribe),
        },
        GeyserGrpcClientError::TransportError(error) => {
            GrpcExitReason::Connect(error.to_string())
        }
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::disconnect::DisconnectReason;
    use crate::framework::grpc::{
        ExitClass, StatusClass, StreamFilter, TRANSACTIONS_FILTER, classify_status,
    };
    use crate::framework::onchain::WatchedAddress;
    use crate::framework::onchain::decoded::fixtures::key;
    use aetelier_types::onchain::{Commitment, MatchRole};
    use yellowstone_grpc_proto::prelude::{
        SubscribeUpdateBlockMeta, SubscribeUpdatePing, SubscribeUpdatePong,
        SubscribeUpdateTransaction,
    };
    use yellowstone_grpc_proto::tonic::{Code, Status};

    type Requests = futures::channel::mpsc::Receiver<SubscribeRequest>;
    type Updates = tokio::sync::mpsc::Sender<Result<SubscribeUpdate, Status>>;

    const TOKEN: &str = "s3cr3t-t0k3n";
    const TOKEN_ENV: &str = "GRPC_X_TOKEN";

    fn section() -> GrpcSection {
        GrpcSection {
            endpoint: "https://solana-yellowstone-grpc.publicnode.com:443".to_string(),
            x_token_env: Some(TOKEN_ENV.to_string()),
            commitment: Commitment::Finalized,
            server_filter: true,
            accept_zstd: true,
            max_decoding_message_size_mib: 64,
            ping_interval_secs: 4,
            max_pending_slots: 32,
            max_addresses_per_connection: 50,
            account_include_max: None,
            account_include_reject: Vec::new(),
        }
    }

    fn settings() -> GrpcSettings {
        GrpcSettings::new(&section(), None)
    }

    fn subscription(seeds: &[u8]) -> SubscribeRequest {
        let watched: Vec<WatchedAddress> = seeds
            .iter()
            .map(|&seed| WatchedAddress {
                address: key(seed),
                required_role: MatchRole::AccountKey,
                include_failed: false,
                keep_logs: false,
            })
            .collect();
        StreamFilter::new(&watched, &section()).request()
    }

    fn link() -> (GeyserChannel, Requests, Updates) {
        let (requests, written) = futures::channel::mpsc::channel(64);
        let (updates, stream) = tokio::sync::mpsc::channel(64);
        let channel = GeyserChannel::Opened {
            requests: SubscribeRequestSink::mock(requests),
            updates: GeyserStream::mock(stream),
        };
        (channel, written, updates)
    }

    async fn opened(metrics: &SourceMetrics) -> (GrpcSession, Requests, Updates) {
        let (channel, mut requests, updates) = link();
        let session =
            GrpcSession::open(channel, subscription(&[1]), &settings(), metrics)
                .await
                .expect("the session opens");
        assert_eq!(
            written(&mut requests),
            vec![subscription(&[1]), ping_request(1)]
        );
        (session, requests, updates)
    }

    fn written(requests: &mut Requests) -> Vec<SubscribeRequest> {
        let mut sent = Vec::new();
        while let Ok(request) = requests.try_recv() {
            sent.push(request);
        }
        sent
    }

    async fn deliver(updates: &Updates, sent: impl IntoIterator<Item = SubscribeUpdate>) {
        for update in sent {
            updates
                .send(Ok(update))
                .await
                .expect("the session holds the stream");
        }
    }

    fn wire(update_oneof: UpdateOneof) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(update_oneof),
            created_at: None,
        }
    }

    fn meta(slot: u64) -> SubscribeUpdate {
        wire(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            ..Default::default()
        }))
    }

    fn transaction(slot: u64) -> SubscribeUpdate {
        wire(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: None,
            slot,
            bank_id: slot,
        }))
    }

    fn pong(id: i32) -> SubscribeUpdate {
        wire(UpdateOneof::Pong(SubscribeUpdatePong { id }))
    }

    fn server_ping() -> SubscribeUpdate {
        wire(UpdateOneof::Ping(SubscribeUpdatePing {}))
    }

    fn slot_of(update: &SubscribeUpdate) -> Option<u64> {
        match &update.update_oneof {
            Some(UpdateOneof::BlockMeta(meta)) => Some(meta.slot),
            Some(UpdateOneof::Transaction(transaction)) => Some(transaction.slot),
            _ => None,
        }
    }

    fn received(event: SessionEvent) -> (SubscribeUpdate, ReceiptStamp) {
        match event {
            SessionEvent::Update { update, receipt } => (update, receipt),
            other => panic!("expected an update, got {other:?}"),
        }
    }

    fn acknowledged(event: SessionEvent) -> (i32, bool) {
        match event {
            SessionEvent::Acknowledged { ack, answered, .. } => (ack, answered),
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
    }

    fn status_of(exit: GrpcExitReason) -> (Status, Option<PendingRequest>) {
        match exit {
            GrpcExitReason::Status { status, pending } => (status, pending),
            other => panic!("expected a status, got {other:?}"),
        }
    }

    fn retried_as_heartbeat_failure(exit: GrpcExitReason) -> bool {
        matches!(
            exit.classify(&ServerVersion::default()),
            ExitClass::Retry {
                reason: DisconnectReason::HeartbeatFailed { .. },
                cool_down: None,
            }
        )
    }

    #[test]
    fn settings_follow_the_grpc_section() {
        assert_eq!(STALE_AFTER, Duration::from_secs(10));
        let token = XToken::new(TOKEN.to_string());
        assert_eq!(
            GrpcSettings::new(&section(), Some(token.clone())),
            GrpcSettings {
                endpoint: "https://solana-yellowstone-grpc.publicnode.com:443"
                    .to_string(),
                x_token: Some(token),
                accept_zstd: true,
                max_decoding_message_size: 64 * 1024 * 1024,
                ping_interval: Duration::from_secs(4),
                stale_after: STALE_AFTER,
            }
        );
        let local = GrpcSection {
            endpoint: "http://127.0.0.1:10000".to_string(),
            accept_zstd: false,
            max_decoding_message_size_mib: 1,
            ping_interval_secs: 15,
            ..section()
        };
        assert_eq!(
            GrpcSettings::new(&local, None),
            GrpcSettings {
                endpoint: "http://127.0.0.1:10000".to_string(),
                x_token: None,
                accept_zstd: false,
                max_decoding_message_size: 1 << 20,
                ping_interval: Duration::from_secs(15),
                stale_after: STALE_AFTER,
            }
        );
        let unbounded = GrpcSection {
            max_decoding_message_size_mib: usize::MAX,
            ..section()
        };
        assert_eq!(
            GrpcSettings::new(&unbounded, None).max_decoding_message_size,
            usize::MAX
        );
    }

    #[test]
    fn a_token_resolves_only_from_a_set_non_empty_variable() {
        let unnamed = GrpcSection {
            x_token_env: None,
            ..section()
        };
        for (value, resolves) in [(Some(TOKEN), true), (Some(""), false), (None, false)] {
            let lookup = |name: &str| {
                (name == TOKEN_ENV)
                    .then_some(value)
                    .flatten()
                    .map(str::to_string)
            };
            assert_eq!(
                XToken::from_env(&section(), lookup).is_some(),
                resolves,
                "{value:?}"
            );
            assert_eq!(section().admit_token(lookup).is_ok(), resolves, "{value:?}");
            assert_eq!(XToken::from_env(&unnamed, lookup), None, "{value:?}");
        }
        let token = XToken::from_env(&section(), |name| {
            (name == TOKEN_ENV).then(|| TOKEN.to_string())
        });
        assert_eq!(token, Some(XToken::new(TOKEN.to_string())));
        assert_eq!(token.as_ref().map(XToken::expose), Some(TOKEN));
        let elsewhere = GrpcSection {
            x_token_env: Some("OTHER_X_TOKEN".to_string()),
            ..section()
        };
        assert_eq!(
            XToken::from_env(&elsewhere, |name| (name == TOKEN_ENV)
                .then(|| TOKEN.to_string())),
            None
        );
    }

    #[test]
    fn the_token_never_appears_in_debug_output() {
        let token = XToken::new(TOKEN.to_string());
        assert_eq!(format!("{token:?}"), "XToken(..)");
        assert_eq!(format!("{token:#?}"), "XToken(..)");
        let settings = GrpcSettings::new(&section(), Some(token));
        for debug in [format!("{settings:?}"), format!("{settings:#?}")] {
            assert!(!debug.contains(TOKEN), "{debug}");
            assert!(debug.contains("XToken(..)"), "{debug}");
        }
        assert_eq!(settings.x_token.as_ref().map(XToken::expose), Some(TOKEN));
    }

    #[test]
    fn logged_endpoints_drop_paths_and_queries() {
        for (endpoint, logged) in [
            (
                "https://solana-yellowstone-grpc.publicnode.com:443",
                "https://solana-yellowstone-grpc.publicnode.com",
            ),
            (
                "https://geyser.example.com:8443/s3cr3t-t0k3n",
                "https://geyser.example.com:8443",
            ),
            (
                "https://geyser.example.com/v1?api-key=s3cr3t-t0k3n#s3cr3t-t0k3n",
                "https://geyser.example.com",
            ),
            (
                "https://user:s3cr3t-t0k3n@geyser.example.com:10000/",
                "https://geyser.example.com:10000",
            ),
            ("http://127.0.0.1:10000", "http://127.0.0.1:10000"),
            ("http://[::1]:10000/s3cr3t-t0k3n", "http://[::1]:10000"),
        ] {
            let settings = GrpcSettings {
                endpoint: endpoint.to_string(),
                ..settings()
            };
            assert_eq!(settings.endpoint_host(), logged, "{endpoint}");
        }
        for endpoint in ["https://", "http://geyser example.com/s3cr3t-t0k3n"] {
            let settings = GrpcSettings {
                endpoint: endpoint.to_string(),
                ..settings()
            };
            assert_eq!(
                settings.endpoint_host(),
                "unparsable endpoint",
                "{endpoint}"
            );
        }
    }

    #[tokio::test]
    async fn the_builder_accepts_tls_compression_keepalive_and_size_options() {
        let secure = GrpcSettings::new(&section(), Some(XToken::new(TOKEN.to_string())));
        let builder = secure.builder().expect("the https builder");
        assert_eq!(builder.endpoint.uri().scheme_str(), Some("https"));
        assert_eq!(
            builder.endpoint.get_connect_timeout(),
            Some(CONNECT_TIMEOUT)
        );
        assert!(builder.endpoint.get_tcp_nodelay());
        assert_eq!(
            builder
                .x_token
                .as_ref()
                .and_then(|token| token.to_str().ok()),
            Some(TOKEN)
        );
        assert_eq!(builder.accept_compressed, Some(CompressionEncoding::Zstd));
        assert_eq!(builder.send_compressed, None);
        assert_eq!(builder.max_decoding_message_size, Some(64 << 20));
        assert!(!builder.x_request_snapshot);
        assert!(builder.connect_lazy().is_ok());

        let plain = GrpcSettings {
            endpoint: "http://127.0.0.1:10000".to_string(),
            x_token: None,
            accept_zstd: false,
            ..secure
        };
        let builder = plain.builder().expect("the http builder");
        assert_eq!(builder.endpoint.uri().scheme_str(), Some("http"));
        assert!(builder.x_token.is_none());
        assert_eq!(builder.accept_compressed, None);
        assert_eq!(builder.max_decoding_message_size, Some(64 << 20));
        assert!(builder.connect_lazy().is_ok());
    }

    #[test]
    fn a_token_that_is_not_a_header_value_is_a_setup_error() {
        let settings =
            GrpcSettings::new(&section(), Some(XToken::new("bad\ntoken".to_string())));
        let exit = settings
            .builder()
            .expect_err("a newline is not a header value");
        let GrpcExitReason::Setup(message) = &exit else {
            panic!("expected a setup error, got {exit:?}");
        };
        assert!(!message.contains("bad"), "{message}");
        assert!(!exit.to_string().contains("bad"), "{exit}");
        match exit.classify(&ServerVersion::default()) {
            ExitClass::Reject { reason } => assert!(!reason.contains("bad"), "{reason}"),
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn opening_writes_the_full_request_then_the_acknowledgement_ping() {
        let metrics = SourceMetrics::default();
        let (channel, mut requests, _updates) = link();
        let request = subscription(&[3, 1, 2]);
        let session = GrpcSession::open(channel, request.clone(), &settings(), &metrics)
            .await
            .expect("the session opens");
        let sent = written(&mut requests);
        assert_eq!(sent, vec![request, ping_request(1)]);
        assert!(sent[0].from_slot.is_none() && sent[0].ping.is_none());
        assert_eq!(
            sent[0].transactions[TRANSACTIONS_FILTER]
                .account_include
                .len(),
            3
        );
        assert!(sent[1].transactions.is_empty() && sent[1].blocks_meta.is_empty());
        assert_eq!(session.opening_ack(), 1);
        assert_eq!(session.pending(), Some(PendingRequest::Subscribe));
        assert_eq!(session.server(), &ServerVersion::default());
        assert!(session.client().is_none());
        assert_eq!(session.rtt_us(), 0);
        assert!(session.conn_epoch_us() > 0);
        assert_eq!(session.conn_epoch_us(), metrics.conn_epoch_us());
    }

    #[tokio::test(start_paused = true)]
    async fn the_opening_acknowledgement_is_reported_once_and_clears_pending() {
        let (mut session, _requests, updates) = opened(&SourceMetrics::default()).await;
        let before = now_us();
        deliver(&updates, [pong(1), pong(1)]).await;
        match session.next().await.expect("the pong is read") {
            SessionEvent::Acknowledged {
                ack,
                recv_us,
                answered,
            } => {
                assert_eq!((ack, answered), (1, true));
                assert!(recv_us >= before);
            }
            other => panic!("expected the opening acknowledgement, got {other:?}"),
        }
        assert_eq!(session.pending(), None);
        assert_eq!(session.opening_ack(), 1);
        let started = Instant::now();
        for slot in 1..=12 {
            sleep_until(started + Duration::from_secs(slot)).await;
            deliver(&updates, [meta(slot)]).await;
            let (update, _) =
                received(session.next().await.expect("the stream stays open"));
            assert_eq!(slot_of(&update), Some(slot));
            assert_eq!(session.pending(), None);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn server_pings_are_answered_with_a_ping_request() {
        let (mut session, mut requests, updates) =
            opened(&SourceMetrics::default()).await;
        deliver(
            &updates,
            [
                server_ping(),
                meta(5),
                server_ping(),
                server_ping(),
                meta(6),
            ],
        )
        .await;
        let (first, receipt) = received(session.next().await.expect("an update"));
        assert_eq!((slot_of(&first), receipt.recv_seq), (Some(5), 1));
        assert_eq!(written(&mut requests), vec![ping_request(2)]);
        let (second, receipt) = received(session.next().await.expect("an update"));
        assert_eq!((slot_of(&second), receipt.recv_seq), (Some(6), 2));
        assert_eq!(
            written(&mut requests),
            vec![ping_request(3), ping_request(4)]
        );
        assert_eq!(session.pending(), Some(PendingRequest::Subscribe));
        assert_eq!(session.opening_ack(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn client_pings_follow_the_ping_interval_and_pongs_set_the_rtt() {
        let (mut session, mut requests, updates) =
            opened(&SourceMetrics::default()).await;
        let started = Instant::now();
        assert_eq!(session.rtt_us(), 0);
        deliver(&updates, [pong(1)]).await;
        assert_eq!(
            acknowledged(session.next().await.expect("acknowledged")),
            (1, true)
        );
        assert!(session.rtt_us() >= 1);
        let mut pings = Vec::new();
        for slot in 1..=13 {
            sleep_until(started + Duration::from_secs(slot)).await;
            deliver(&updates, [meta(slot)]).await;
            let (_, receipt) = received(session.next().await.expect("an update"));
            assert_eq!(receipt.source_tx_rtt_us, session.rtt_us());
            pings.extend(
                written(&mut requests)
                    .into_iter()
                    .map(|request| (started.elapsed(), request)),
            );
        }
        assert_eq!(
            pings,
            vec![
                (Duration::from_secs(4), ping_request(2)),
                (Duration::from_secs(8), ping_request(3)),
                (Duration::from_secs(12), ping_request(4)),
            ]
        );
        assert_eq!(session.pings.sent.len(), 3);
        deliver(&updates, [pong(2), pong(4), meta(14)]).await;
        let (_, receipt) = received(session.next().await.expect("an update"));
        assert!(session.pings.sent.is_empty());
        assert!(session.rtt_us() >= 1);
        assert_eq!(receipt.source_tx_rtt_us, session.rtt_us());
        assert_eq!(session.pending(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_missing_pong_is_acknowledged_after_two_ping_intervals_and_never_ends_the_session()
     {
        let (mut session, mut requests, updates) =
            opened(&SourceMetrics::default()).await;
        let started = Instant::now();
        let mut acknowledgements = Vec::new();
        let mut update_ack = None;
        for slot in 1..=40 {
            sleep_until(started + Duration::from_secs(slot)).await;
            deliver(&updates, [meta(slot)]).await;
            if slot == 10 {
                update_ack = Some(
                    session
                        .update(subscription(&[1, 2]))
                        .expect("the update is written"),
                );
                assert_eq!(session.pending(), Some(PendingRequest::Update));
            }
            while let SessionEvent::Acknowledged { ack, answered, .. } =
                session.next().await.expect("a missing pong never ends it")
            {
                acknowledgements.push((started.elapsed(), ack, answered));
            }
        }
        let update_ack = update_ack.expect("an update was sent");
        assert_eq!(update_ack, 4);
        assert_eq!(
            acknowledgements,
            vec![
                (Duration::from_secs(8), 1, false),
                (Duration::from_secs(18), update_ack, false),
            ]
        );
        assert_eq!(session.pending(), None);
        let pings: Vec<i32> = written(&mut requests)
            .iter()
            .filter_map(|request| request.ping.as_ref().map(|ping| ping.id))
            .collect();
        assert_eq!(pings, (2..=12).collect::<Vec<_>>());
    }

    #[tokio::test(start_paused = true)]
    async fn no_block_meta_for_stale_after_ends_the_session_as_stale() {
        let (mut session, _requests, updates) = opened(&SourceMetrics::default()).await;
        deliver(&updates, [pong(1)]).await;
        assert_eq!(
            acknowledged(session.next().await.expect("acknowledged")),
            (1, true)
        );
        let started = Instant::now();
        let feeder = tokio::spawn(async move {
            for slot in 1..=40u64 {
                sleep_until(started + Duration::from_millis(500 * slot)).await;
                let update = if slot == 6 {
                    meta(slot)
                } else {
                    transaction(slot)
                };
                if updates.send(Ok(update)).await.is_err() {
                    break;
                }
            }
        });
        let mut slots = Vec::new();
        let exit = loop {
            match session.next().await {
                Ok(event) => slots.extend(slot_of(&received(event).0)),
                Err(exit) => break exit,
            }
        };
        assert_eq!(started.elapsed(), Duration::from_secs(13));
        assert!(
            matches!(exit, GrpcExitReason::Stale { silence } if silence == STALE_AFTER),
            "{exit:?}"
        );
        assert!(slots.len() >= 25, "{slots:?}");
        assert!(matches!(
            exit.classify(session.server()),
            ExitClass::Retry {
                reason: DisconnectReason::StaleConnection { silence_duration },
                cool_down: None,
            } if silence_duration == STALE_AFTER
        ));
        feeder.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn an_update_sends_the_full_request_then_its_acknowledgement_ping() {
        let (mut session, mut requests, updates) =
            opened(&SourceMetrics::default()).await;
        deliver(&updates, [pong(1)]).await;
        assert_eq!(
            acknowledged(session.next().await.expect("acknowledged")),
            (1, true)
        );
        assert_eq!(session.pending(), None);
        let widened = subscription(&[1, 2]);
        let ack = session
            .update(widened.clone())
            .expect("the update is written");
        assert_eq!(ack, 2);
        assert_eq!(written(&mut requests), vec![widened, ping_request(2)]);
        assert_eq!(session.pending(), Some(PendingRequest::Update));
        assert_eq!(session.opening_ack(), 1);
        deliver(&updates, [meta(5), pong(2)]).await;
        let (update, _) = received(session.next().await.expect("an update"));
        assert_eq!(slot_of(&update), Some(5));
        assert_eq!(session.pending(), Some(PendingRequest::Update));
        assert_eq!(
            acknowledged(session.next().await.expect("acknowledged")),
            (2, true)
        );
        assert_eq!(session.pending(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn one_pong_acknowledges_every_earlier_request_in_order() {
        let (mut session, mut requests, updates) =
            opened(&SourceMetrics::default()).await;
        let first = session
            .update(subscription(&[1, 2]))
            .expect("the update is written");
        let second = session
            .update(subscription(&[1, 2, 3]))
            .expect("the update is written");
        assert_eq!((first, second), (2, 3));
        assert_eq!(session.pending(), Some(PendingRequest::Update));
        assert_eq!(
            written(&mut requests),
            vec![
                subscription(&[1, 2]),
                ping_request(2),
                subscription(&[1, 2, 3]),
                ping_request(3),
            ]
        );
        deliver(&updates, [pong(second), meta(9)]).await;
        let mut events = Vec::new();
        for _ in 0..3 {
            match session.next().await.expect("acknowledged") {
                SessionEvent::Acknowledged {
                    ack,
                    recv_us,
                    answered,
                } => events.push((ack, recv_us, answered)),
                other => panic!("expected an acknowledgement, got {other:?}"),
            }
            assert_eq!(session.pending(), None);
        }
        let recv_us = events[0].1;
        assert_eq!(
            events,
            vec![(1, recv_us, true), (2, recv_us, true), (3, recv_us, true)]
        );
        let (update, receipt) = received(session.next().await.expect("an update"));
        assert_eq!((slot_of(&update), receipt.recv_seq), (Some(9), 1));
    }

    #[tokio::test(start_paused = true)]
    async fn stream_end_and_status_errors_carry_the_pending_request() {
        let metrics = SourceMetrics::default();

        let (mut opening, _requests, updates) = opened(&metrics).await;
        updates
            .send(Err(Status::permission_denied("invalid x-token")))
            .await
            .expect("the session holds the stream");
        let (status, pending) =
            status_of(opening.next().await.expect_err("the stream fails"));
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(pending, Some(PendingRequest::Subscribe));
        assert_eq!(classify_status(&status, pending), StatusClass::Reject);

        let (mut updating, _requests, updates) = opened(&metrics).await;
        deliver(&updates, [pong(1)]).await;
        acknowledged(updating.next().await.expect("acknowledged"));
        updating
            .update(subscription(&[1, 2]))
            .expect("the update is written");
        updates
            .send(Err(Status::invalid_argument(
                "account_include exceeds 50 addresses",
            )))
            .await
            .expect("the session holds the stream");
        let (status, pending) =
            status_of(updating.next().await.expect_err("the stream fails"));
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(pending, Some(PendingRequest::Update));
        assert_eq!(classify_status(&status, pending), StatusClass::RejectUpdate);

        let (mut settled, _requests, updates) = opened(&metrics).await;
        deliver(&updates, [pong(1), meta(3)]).await;
        acknowledged(settled.next().await.expect("acknowledged"));
        received(settled.next().await.expect("an update"));
        updates
            .send(Err(Status::unavailable("upstream reset")))
            .await
            .expect("the session holds the stream");
        let (status, pending) =
            status_of(settled.next().await.expect_err("the stream fails"));
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(pending, None);

        let (mut ended, _requests, updates) = opened(&metrics).await;
        deliver(&updates, [meta(4)]).await;
        drop(updates);
        received(ended.next().await.expect("the queued update is read"));
        let exit = ended.next().await.expect_err("the stream ended");
        assert!(matches!(exit, GrpcExitReason::Ended), "{exit:?}");
        assert_eq!(ended.pending(), Some(PendingRequest::Subscribe));
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_request_channel_ends_the_session_as_a_request_failure() {
        let metrics = SourceMetrics::default();

        let (mut updating, requests, _updates) = opened(&metrics).await;
        drop(requests);
        let exit = updating
            .update(subscription(&[1, 2]))
            .expect_err("the request receiver is gone");
        assert!(
            matches!(&exit, GrpcExitReason::RequestFailed(message) if message != "request channel full"),
            "{exit:?}"
        );
        assert!(retried_as_heartbeat_failure(exit));

        let (mut answering, requests, updates) = opened(&metrics).await;
        drop(requests);
        deliver(&updates, [server_ping()]).await;
        let exit = answering
            .next()
            .await
            .expect_err("the ping reply cannot be written");
        assert!(matches!(exit, GrpcExitReason::RequestFailed(_)), "{exit:?}");
        assert!(retried_as_heartbeat_failure(exit));

        let (mut pinging, requests, _updates) = opened(&metrics).await;
        drop(requests);
        let started = Instant::now();
        let exit = pinging
            .next()
            .await
            .expect_err("the client ping cannot be written");
        assert_eq!(started.elapsed(), Duration::from_secs(4));
        assert!(matches!(exit, GrpcExitReason::RequestFailed(_)), "{exit:?}");
        assert!(retried_as_heartbeat_failure(exit));

        let (mut flooded, _requests, _updates) = opened(&metrics).await;
        let exit = (0..100)
            .find_map(|seed| flooded.update(subscription(&[1, seed])).err())
            .expect("an unread request channel fills up");
        assert!(
            matches!(&exit, GrpcExitReason::RequestFailed(message) if message == "request channel full"),
            "{exit:?}"
        );
        assert!(retried_as_heartbeat_failure(exit));
    }

    #[tokio::test(start_paused = true)]
    async fn an_opened_channel_whose_request_receiver_is_gone_fails_to_open() {
        let metrics = SourceMetrics::default();
        let (channel, requests, _updates) = link();
        drop(requests);
        let exit =
            match GrpcSession::open(channel, subscription(&[1]), &settings(), &metrics)
                .await
            {
                Ok(_) => panic!("a session opened without a request receiver"),
                Err(exit) => exit,
            };
        assert!(matches!(exit, GrpcExitReason::RequestFailed(_)), "{exit:?}");
        assert_eq!(metrics.conn_epoch_us(), 0);
        assert!(retried_as_heartbeat_failure(exit));
    }

    #[tokio::test(start_paused = true)]
    async fn next_is_cancel_safe() {
        let (mut session, _requests, updates) = opened(&SourceMetrics::default()).await;
        let feeder = tokio::spawn(async move {
            for slot in 1..=50u64 {
                tokio::time::sleep(Duration::from_millis(7)).await;
                if slot == 20 {
                    deliver(&updates, [pong(1), server_ping()]).await;
                }
                deliver(&updates, [meta(slot)]).await;
            }
        });
        let consumer = tokio::spawn(async move {
            let mut slots = Vec::new();
            let mut seqs = Vec::new();
            let mut acks = Vec::new();
            while slots.len() < 50 {
                tokio::select! {
                    event = session.next() => match event.expect("the stream stays open") {
                        SessionEvent::Update { update, receipt } => {
                            slots.extend(slot_of(&update));
                            seqs.push(receipt.recv_seq);
                        }
                        SessionEvent::Acknowledged { ack, answered, .. } => {
                            acks.push((ack, answered));
                        }
                    },
                    () = tokio::time::sleep(Duration::from_millis(3)) => {}
                }
            }
            (slots, seqs, acks, session.pings.last_id)
        });
        let (slots, seqs, acks, last_id) = consumer.await.expect("the consumer finishes");
        assert_eq!(slots, (1..=50).collect::<Vec<_>>());
        assert_eq!(seqs, (1..=50).collect::<Vec<_>>());
        assert_eq!(acks, vec![(1, true)]);
        assert_eq!(last_id, 2);
        feeder.await.expect("the feeder finishes");
    }

    #[tokio::test(start_paused = true)]
    async fn updates_carry_a_per_connection_receipt() {
        let metrics = SourceMetrics::default();
        let (mut first, _first_requests, first_updates) = opened(&metrics).await;
        let epoch = first.conn_epoch_us();
        assert_eq!(metrics.conn_epoch_us(), epoch);
        deliver(
            &first_updates,
            [
                transaction(5),
                server_ping(),
                meta(5),
                pong(1),
                transaction(6),
            ],
        )
        .await;
        let before = now_us();
        let (update, receipt) = received(first.next().await.expect("an update"));
        let after = now_us();
        assert_eq!(slot_of(&update), Some(5));
        assert!(
            (before..=after).contains(&receipt.local_tx_ts_us),
            "{receipt:?}"
        );
        assert_eq!(
            receipt,
            ReceiptStamp {
                local_tx_ts_us: receipt.local_tx_ts_us,
                source_tx_rtt_us: 0,
                recv_seq: 1,
                conn_epoch_us: epoch,
            }
        );
        let (_, receipt) = received(first.next().await.expect("an update"));
        assert_eq!(
            (
                receipt.recv_seq,
                receipt.conn_epoch_us,
                receipt.source_tx_rtt_us
            ),
            (2, epoch, 0)
        );
        assert_eq!(
            acknowledged(first.next().await.expect("acknowledged")),
            (1, true)
        );
        let (_, receipt) = received(first.next().await.expect("an update"));
        assert_eq!((receipt.recv_seq, receipt.conn_epoch_us), (3, epoch));
        assert!(receipt.source_tx_rtt_us >= 1);
        assert_eq!(receipt.source_tx_rtt_us, first.rtt_us());

        let (mut second, _second_requests, second_updates) = opened(&metrics).await;
        assert!(second.conn_epoch_us() > epoch);
        assert_eq!(metrics.conn_epoch_us(), second.conn_epoch_us());
        deliver(&second_updates, [meta(7)]).await;
        let (_, receipt) = received(second.next().await.expect("an update"));
        assert_eq!(
            (
                receipt.recv_seq,
                receipt.conn_epoch_us,
                receipt.source_tx_rtt_us
            ),
            (1, second.conn_epoch_us(), 0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn ping_ids_increase_and_restart_at_one_after_overflow() {
        let mut clock = PingClock::default();
        assert_eq!(
            [clock.issue(10), clock.issue(20), clock.issue(30)],
            [1, 2, 3]
        );
        clock.last_id = i32::MAX - 1;
        assert_eq!(
            [clock.issue(40), clock.issue(50), clock.issue(60)],
            [i32::MAX, 1, 2]
        );
        let mut window = PingClock::default();
        for sent_us in 1..=100 {
            window.issue(sent_us);
        }
        assert_eq!(window.sent.len(), PING_WINDOW);
        assert_eq!(window.sent.front(), Some(&(37, 37)));
        assert_eq!(window.sent.back(), Some(&(100, 100)));

        let (mut session, mut requests, updates) =
            opened(&SourceMetrics::default()).await;
        deliver(&updates, [pong(1)]).await;
        assert_eq!(
            acknowledged(session.next().await.expect("acknowledged")),
            (1, true)
        );
        session.pings.last_id = i32::MAX - 1;
        let last = session
            .update(subscription(&[1, 2]))
            .expect("the update is written");
        let wrapped = session
            .update(subscription(&[1, 2, 3]))
            .expect("the update is written");
        assert_eq!((last, wrapped), (i32::MAX, 1));
        assert_eq!(
            written(&mut requests),
            vec![
                subscription(&[1, 2]),
                ping_request(i32::MAX),
                subscription(&[1, 2, 3]),
                ping_request(1),
            ]
        );
        deliver(&updates, [pong(1)]).await;
        assert_eq!(
            acknowledged(session.next().await.expect("acknowledged")),
            (i32::MAX, true)
        );
        assert_eq!(
            acknowledged(session.next().await.expect("acknowledged")),
            (1, true)
        );
        assert_eq!(session.pending(), None);
    }

    #[test]
    fn a_pong_answers_its_ping_and_drops_older_unanswered_ones() {
        let mut clock = PingClock::default();
        for sent_us in [1_000, 2_000, 3_000] {
            clock.issue(sent_us);
        }
        assert_eq!(clock.answer(2, 2_500), Some(500));
        assert_eq!(clock.rtt_us, 500);
        assert_eq!(clock.sent, VecDeque::from([(3, 3_000)]));
        assert_eq!(clock.answer(1, 2_600), None);
        assert_eq!(clock.answer(2, 2_700), None);
        assert_eq!(clock.answer(9, 2_800), None);
        assert_eq!(clock.rtt_us, 500);
        assert_eq!(clock.answer(3, 4_000), Some(1_000));
        assert_eq!(clock.rtt_us, rtt_ewma_us(500, 1_000));
        assert_eq!(clock.rtt_us, 600);
        assert!(clock.sent.is_empty());
        let stepped_back = clock.issue(5_000);
        assert_eq!(clock.answer(stepped_back, 4_000), Some(1));
        assert_eq!(clock.rtt_us, 480);
    }
}
