use std::collections::{HashMap, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use aetelier_connect::framework::grpc::{
    GeyserChannel, GeyserDial, GrpcConnections, GrpcExitReason, GrpcSettings,
};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::AbortHandle;
use tokio::time::{Instant, timeout};
use yellowstone_grpc_client::{GeyserStream, SubscribeRequestSink};
use yellowstone_grpc_proto::geyser::geyser_server::{Geyser, GeyserServer};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse,
    GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest,
    GetSlotResponse, GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
    IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeDeshredRequest,
    SubscribeGossipRequest, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse,
    SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred, SubscribeUpdateGossip,
};
use yellowstone_grpc_proto::tonic::codegen::BoxStream;
use yellowstone_grpc_proto::tonic::transport::server::Connected;
use yellowstone_grpc_proto::tonic::transport::{Server, Uri};
use yellowstone_grpc_proto::tonic::{self, Request, Response, Status, Streaming};

use crate::chain::{ping_id, pong};

pub const ENDPOINT: &str = "http://mock.geyser";
const WAIT: Duration = Duration::from_secs(600);
const VERSION: &str = r#"{"package":"mock-geyser","version":"0.0.0","proto":"14.0.1"}"#;

type UpdateSender =
    futures::channel::mpsc::UnboundedSender<Result<SubscribeUpdate, Status>>;

#[derive(Clone, Default)]
pub struct MockGeyser {
    shared: Arc<Shared>,
}

pub enum Admission {
    Accept,
    Refuse(Status),
}

struct Shared {
    admissions: Mutex<VecDeque<Admission>>,
    subscribes: Mutex<Vec<SubscribeRequest>>,
    tokens: Mutex<Vec<Option<String>>>,
    tips: Mutex<HashMap<i32, u64>>,
    auto_pong: Arc<AtomicBool>,
    wires: Mutex<HashMap<usize, AbortHandle>>,
    next_wire: AtomicUsize,
    opened: mpsc::UnboundedSender<MockConnection>,
    connections: tokio::sync::Mutex<mpsc::UnboundedReceiver<MockConnection>>,
}

impl Default for Shared {
    fn default() -> Self {
        let (opened, connections) = mpsc::unbounded_channel();
        Self {
            admissions: Mutex::default(),
            subscribes: Mutex::default(),
            tokens: Mutex::default(),
            tips: Mutex::default(),
            auto_pong: Arc::new(AtomicBool::new(true)),
            wires: Mutex::default(),
            next_wire: AtomicUsize::new(0),
            opened,
            connections: tokio::sync::Mutex::new(connections),
        }
    }
}

impl MockGeyser {
    pub fn auto_pong(&self, answer: bool) {
        self.shared.auto_pong.store(answer, Ordering::SeqCst);
    }

    pub fn admit_next(&self, admission: Admission) {
        lock(&self.shared.admissions).push_back(admission);
    }

    pub fn set_tip(&self, commitment: CommitmentLevel, slot: u64) {
        lock(&self.shared.tips).insert(commitment as i32, slot);
    }

    pub async fn next_connection(&self) -> MockConnection {
        let mut connections = self.shared.connections.lock().await;
        timeout(WAIT, connections.recv())
            .await
            .expect("a subscribe arrives within the wait")
            .expect("the mock keeps serving")
    }

    pub fn subscribes(&self) -> Vec<SubscribeRequest> {
        lock(&self.shared.subscribes).clone()
    }

    pub fn tokens(&self) -> Vec<Option<String>> {
        lock(&self.shared.tokens).clone()
    }

    pub fn dial(&self) -> DuplexDial {
        let (incoming, mut accepted) = mpsc::unbounded_channel::<WireEnd>();
        let service = GeyserServer::new(self.clone());
        tokio::spawn(async move {
            let wires = futures::stream::poll_fn(move |cx| {
                accepted
                    .poll_recv(cx)
                    .map(|wire| wire.map(Ok::<_, io::Error>))
            });
            let _ = Server::builder()
                .add_service(service)
                .serve_with_incoming(wires)
                .await;
        });
        DuplexDial {
            incoming,
            shared: self.shared.clone(),
            refusals: Arc::default(),
            dials: Arc::default(),
        }
    }
}

#[tonic::async_trait]
impl Geyser for MockGeyser {
    type SubscribeStream =
        futures::channel::mpsc::UnboundedReceiver<Result<SubscribeUpdate, Status>>;
    type SubscribeDeshredStream = BoxStream<SubscribeUpdateDeshred>;
    type SubscribeGossipStream = BoxStream<SubscribeUpdateGossip>;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let wire = request.extensions().get::<WireId>().map(|wire| wire.0);
        let token = request
            .metadata()
            .get("x-token")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let mut incoming = request.into_inner();
        let first = incoming.message().await?.unwrap_or_default();
        lock(&self.shared.subscribes).push(first.clone());
        lock(&self.shared.tokens).push(token);
        let admission = lock(&self.shared.admissions).pop_front();
        if let Some(Admission::Refuse(status)) = admission {
            return Err(status);
        }
        let (updates, stream) = futures::channel::mpsc::unbounded();
        let (requests, received) = mpsc::unbounded_channel();
        let (closed, watched) = watch::channel(false);
        let auto_pong = self.shared.auto_pong.clone();
        let pongs = updates.clone();
        tokio::spawn(async move {
            while let Ok(Some(request)) = incoming.message().await {
                if auto_pong.load(Ordering::SeqCst)
                    && let Some(id) = ping_id(&request)
                {
                    let _ = pongs.unbounded_send(Ok(pong(id)));
                }
                let _ = requests.send(request);
            }
            let _ = closed.send(true);
        });
        let connection = MockConnection {
            opened_at: Instant::now(),
            first_request: first,
            requests: received,
            updates,
            wire: wire.and_then(|wire| lock(&self.shared.wires).get(&wire).cloned()),
            closed: watched,
        };
        let _ = self.shared.opened.send(connection);
        Ok(Response::new(stream))
    }

    async fn subscribe_deshred(
        &self,
        _request: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Err(Status::unimplemented("subscribe_deshred"))
    }

    async fn subscribe_gossip(
        &self,
        _request: Request<SubscribeGossipRequest>,
    ) -> Result<Response<Self::SubscribeGossipStream>, Status> {
        Err(Status::unimplemented("subscribe_gossip"))
    }

    async fn subscribe_replay_info(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        Err(Status::unimplemented("subscribe_replay_info"))
    }

    async fn ping(
        &self,
        _request: Request<PingRequest>,
    ) -> Result<Response<PongResponse>, Status> {
        Err(Status::unimplemented("ping"))
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented("get_latest_blockhash"))
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented("get_block_height"))
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        let commitment = request.into_inner().commitment.unwrap_or_default();
        match lock(&self.shared.tips).get(&commitment) {
            Some(&slot) => Ok(Response::new(GetSlotResponse { slot })),
            None => Err(Status::not_found("no tip at this commitment")),
        }
    }

    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented("is_blockhash_valid"))
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: VERSION.to_string(),
        }))
    }
}

pub struct MockConnection {
    opened_at: Instant,
    first_request: SubscribeRequest,
    requests: mpsc::UnboundedReceiver<SubscribeRequest>,
    updates: UpdateSender,
    wire: Option<AbortHandle>,
    closed: watch::Receiver<bool>,
}

impl MockConnection {
    pub fn opened_at(&self) -> Instant {
        self.opened_at
    }

    pub fn first_request(&self) -> &SubscribeRequest {
        &self.first_request
    }

    pub async fn next_request(&mut self) -> SubscribeRequest {
        timeout(WAIT, self.requests.recv())
            .await
            .expect("a request arrives within the wait")
            .expect("the client keeps its request stream")
    }

    pub fn send(&self, update: SubscribeUpdate) {
        let _ = self.updates.unbounded_send(Ok(update));
    }

    pub fn send_all(&self, updates: impl IntoIterator<Item = SubscribeUpdate>) {
        for update in updates {
            self.send(update);
        }
    }

    pub fn fail(&self, status: Status) {
        let _ = self.updates.unbounded_send(Err(status));
        self.updates.close_channel();
    }

    pub fn finish(&self) {
        self.updates.close_channel();
    }

    pub fn reset(&self) {
        if let Some(wire) = &self.wire {
            wire.abort();
        }
    }

    pub async fn closed(&mut self) {
        let closed = timeout(WAIT, self.closed.wait_for(|closed| *closed)).await;
        assert!(
            closed.expect("the client closes within the wait").is_ok(),
            "the request reader ended without reporting"
        );
    }
}

#[derive(Clone)]
pub struct DuplexDial {
    incoming: mpsc::UnboundedSender<WireEnd>,
    shared: Arc<Shared>,
    refusals: Arc<AtomicUsize>,
    dials: Arc<Mutex<Vec<Instant>>>,
}

impl DuplexDial {
    pub fn refuse_next(&self, dials: usize) {
        self.refusals.store(dials, Ordering::SeqCst);
    }

    pub fn dials(&self) -> Vec<Instant> {
        lock(&self.dials).clone()
    }
}

#[async_trait::async_trait]
impl GeyserDial for DuplexDial {
    async fn dial(
        &self,
        settings: &GrpcSettings,
    ) -> Result<GeyserChannel, GrpcExitReason> {
        lock(&self.dials).push(Instant::now());
        let refused = self
            .refusals
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        let incoming = self.incoming.clone();
        let shared = self.shared.clone();
        let connector = tower::service_fn(move |_: Uri| {
            let incoming = incoming.clone();
            let shared = shared.clone();
            async move {
                if refused {
                    return Err(io::Error::other("connection refused"));
                }
                let wire = shared.next_wire.fetch_add(1, Ordering::SeqCst);
                let (client, mut client_end) = tokio::io::duplex(1 << 16);
                let (mut server_end, server) = tokio::io::duplex(1 << 16);
                let task = tokio::spawn(async move {
                    let _ =
                        tokio::io::copy_bidirectional(&mut client_end, &mut server_end)
                            .await;
                });
                lock(&shared.wires).insert(wire, task.abort_handle());
                incoming
                    .send(WireEnd { io: server, wire })
                    .map_err(|_| io::Error::other("the mock server stopped"))?;
                Ok::<_, io::Error>(TokioIo::new(client))
            }
        });
        settings
            .builder()?
            .connect_with_connector(connector)
            .await
            .map(GeyserChannel::Client)
            .map_err(|error| GrpcExitReason::Connect(error_text(&error)))
    }
}

#[derive(Clone, Default)]
pub struct PairDial {
    peers: Arc<Mutex<VecDeque<PairPeer>>>,
    arrived: Arc<Notify>,
}

pub struct PairPeer {
    pub requests: futures::channel::mpsc::Receiver<SubscribeRequest>,
    pub updates: tokio::sync::mpsc::Sender<Result<SubscribeUpdate, Status>>,
}

impl PairDial {
    pub async fn next_peer(&self) -> PairPeer {
        let arrival = async {
            loop {
                let next = lock(&self.peers).pop_front();
                match next {
                    Some(peer) => return peer,
                    None => self.arrived.notified().await,
                }
            }
        };
        timeout(WAIT, arrival)
            .await
            .expect("a dial arrives within the wait")
    }
}

#[async_trait::async_trait]
impl GeyserDial for PairDial {
    async fn dial(
        &self,
        _settings: &GrpcSettings,
    ) -> Result<GeyserChannel, GrpcExitReason> {
        let (requests, written) = futures::channel::mpsc::channel(64);
        let (updates, stream) = tokio::sync::mpsc::channel(64);
        lock(&self.peers).push_back(PairPeer {
            requests: written,
            updates,
        });
        self.arrived.notify_one();
        Ok(GeyserChannel::Opened {
            requests: SubscribeRequestSink::mock(requests),
            updates: GeyserStream::mock(stream),
        })
    }
}

pub fn duplex_pool(mock: &MockGeyser) -> (GrpcConnections, DuplexDial) {
    let dial = mock.dial();
    let shared: Arc<dyn GeyserDial> = Arc::new(dial.clone());
    (GrpcConnections::new(shared), dial)
}

pub async fn subscribed(mock: &MockGeyser) -> MockConnection {
    let mut connection = mock.next_connection().await;
    let opening = connection.next_request().await;
    assert_eq!(ping_id(&opening), Some(1), "{opening:?}");
    connection
}

struct WireEnd {
    io: DuplexStream,
    wire: usize,
}

#[derive(Debug, Clone, Copy)]
struct WireId(usize);

impl Connected for WireEnd {
    type ConnectInfo = WireId;

    fn connect_info(&self) -> Self::ConnectInfo {
        WireId(self.wire)
    }
}

impl AsyncRead for WireEnd {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for WireEnd {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("the mock state is not poisoned")
}

fn error_text(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let message = cause.to_string();
        if !text.ends_with(&message) {
            text = format!("{text}: {message}");
        }
        source = cause.source();
    }
    text
}
