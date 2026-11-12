use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

use super::pool::{AttachId, ConnectionKey, DetachGuard};
use crate::clients::disconnect::DisconnectReason;
use crate::framework::driver::STOP_DRAIN_TIMEOUT;
use crate::framework::model::DomainEvent;
use crate::framework::registry::TaskExit;

pub struct GrpcAttach {
    id: AttachId,
    key: ConnectionKey,
    events: mpsc::Receiver<Vec<DomainEvent>>,
    exit: oneshot::Receiver<TaskExit>,
    guard: Option<DetachGuard>,
}

impl GrpcAttach {
    pub fn id(&self) -> AttachId {
        self.id
    }

    pub fn key(&self) -> &ConnectionKey {
        &self.key
    }

    pub async fn recv(&mut self) -> Option<Vec<DomainEvent>> {
        self.events.recv().await
    }

    pub fn try_recv(&mut self) -> Option<Vec<DomainEvent>> {
        self.events.try_recv().ok()
    }

    pub async fn finish(self) -> TaskExit {
        let outcome = self.exit.await;
        drop(self.guard);
        outcome.unwrap_or_else(|_| {
            TaskExit::Failed(DisconnectReason::TransportError {
                source: "gRPC connection ended without an outcome".into(),
            })
        })
    }

    pub(crate) fn from_parts(
        id: AttachId,
        key: ConnectionKey,
        events: mpsc::Receiver<Vec<DomainEvent>>,
        exit: oneshot::Receiver<TaskExit>,
        guard: Option<DetachGuard>,
    ) -> Self {
        Self {
            id,
            key,
            events,
            exit,
            guard,
        }
    }
}

pub async fn drive_grpc(
    mut attach: GrpcAttach,
    tx: mpsc::Sender<DomainEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> TaskExit {
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let drained = timeout(STOP_DRAIN_TIMEOUT, drain(&mut attach, &tx)).await;
                    return match drained {
                        Ok(()) => TaskExit::Completed,
                        Err(_) => TaskExit::DrainTimedOut,
                    };
                }
            }
            () = tx.closed() => return TaskExit::Completed,
            batch = attach.recv() => match batch {
                Some(batch) => {
                    if !forward(batch, &tx).await {
                        return TaskExit::Completed;
                    }
                }
                None => return attach.finish().await,
            },
        }
    }
}

async fn drain(attach: &mut GrpcAttach, tx: &mpsc::Sender<DomainEvent>) {
    while let Some(batch) = attach.try_recv() {
        if !forward(batch, tx).await {
            return;
        }
    }
}

async fn forward(batch: Vec<DomainEvent>, tx: &mpsc::Sender<DomainEvent>) -> bool {
    for event in batch {
        if tx.send(event).await.is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::config::workers::market_worker_config::GrpcSection;
    use crate::framework::budget::{RateWindow, SourceMetrics};
    use crate::framework::grpc::{
        AttachSpec, GeyserChannel, GeyserDial, GrpcConnections, GrpcExitReason,
        GrpcSettings, rejection,
    };
    use crate::framework::onchain::WatchedAddress;
    use crate::framework::onchain::decoded::fixtures::key;
    use crate::framework::slotbook::SlotTick;
    use aetelier_types::onchain::{Commitment, MatchRole};
    use tokio::time::Instant;

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

    fn connection_key() -> ConnectionKey {
        ConnectionKey {
            endpoint: "http://mock.geyser".to_string(),
            commitment: Commitment::Finalized,
            token_env: None,
            shard: 0,
        }
    }

    fn parts(
        queue: usize,
    ) -> (
        GrpcAttach,
        mpsc::Sender<Vec<DomainEvent>>,
        oneshot::Sender<TaskExit>,
    ) {
        let (events, receiver) = mpsc::channel(queue);
        let (exit, outcome) = oneshot::channel();
        let attach = GrpcAttach::from_parts(
            AttachId(7),
            connection_key(),
            receiver,
            outcome,
            None,
        );
        (attach, events, exit)
    }

    fn tick(slot: u64) -> DomainEvent {
        DomainEvent::SlotTick(SlotTick {
            slot,
            block_height: slot - 50,
            block_time_us: 1_791_321_067_000_000 + slot * 1_000_000,
        })
    }

    fn render(event: &DomainEvent) -> String {
        match event {
            DomainEvent::SlotTick(tick) => format!("tick {}", tick.slot),
            DomainEvent::ConnectionLost { .. } => "lost".to_string(),
            DomainEvent::ConnectionResumed => "resumed".to_string(),
            other => format!("{other:?}"),
        }
    }

    fn forwarded(rx: &mut mpsc::Receiver<DomainEvent>) -> Vec<String> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(render(&event));
        }
        events
    }

    #[tokio::test]
    async fn drive_forwards_each_batch_in_order_then_returns_the_attach_exit() {
        let (attach, events, exit) = parts(8);
        assert_eq!(attach.id(), AttachId(7));
        assert_eq!(attach.key(), &connection_key());
        let (tx, mut rx) = mpsc::channel(64);
        let (_stop, shutdown) = watch::channel(false);
        events
            .send(vec![tick(101), tick(102)])
            .await
            .expect("queued");
        events
            .send(vec![
                DomainEvent::ConnectionLost {
                    reason: "transport error: connection reset".to_string(),
                },
                DomainEvent::ConnectionResumed,
                tick(103),
            ])
            .await
            .expect("queued");
        let reason = "gRPC Unauthenticated: invalid x-token";
        assert!(exit.send(TaskExit::Failed(rejection(reason))).is_ok());
        drop(events);
        match drive_grpc(attach, tx, shutdown).await {
            TaskExit::Failed(DisconnectReason::ProtocolRejection {
                code: 0,
                reason: found,
            }) => {
                assert_eq!(found, reason);
            }
            other => panic!("expected the attach exit, got {other:?}"),
        }
        assert_eq!(
            forwarded(&mut rx),
            vec!["tick 101", "tick 102", "lost", "resumed", "tick 103"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drive_returns_completed_and_detaches_when_the_worker_receiver_is_gone() {
        let (attach, events, _exit) = parts(8);
        events.send(vec![tick(101)]).await.expect("queued");
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        let (_stop, shutdown) = watch::channel(false);
        assert!(matches!(
            drive_grpc(attach, tx, shutdown).await,
            TaskExit::Completed
        ));

        let dial: Arc<dyn GeyserDial> = Arc::new(SilentDial);
        let pool = GrpcConnections::new(dial);
        let section = GrpcSection {
            endpoint: "http://mock.geyser".to_string(),
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
        };
        let pooled = pool
            .attach(AttachSpec {
                venue: "solana",
                section,
                x_token: None,
                watch: WatchedAddress {
                    address: key(1),
                    required_role: MatchRole::AccountKey,
                    include_failed: true,
                    keep_logs: false,
                },
                reconnect: None,
                connect_rate: RateWindow {
                    max: 10,
                    per: Duration::from_secs(60),
                },
                queue_banks: 8,
                metrics: SourceMetrics::default(),
            })
            .expect("attach");
        assert_eq!(pool.connections(), vec![(pooled.key().clone(), 1)]);
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        let (_stop, shutdown) = watch::channel(false);
        assert!(matches!(
            drive_grpc(pooled, tx, shutdown).await,
            TaskExit::Completed
        ));
        assert!(pool.connections().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn drive_drains_queued_batches_on_shutdown_within_the_stop_bound() {
        let (attach, events, _exit) = parts(8);
        for slot in 101..=103 {
            events.send(vec![tick(slot)]).await.expect("queued");
        }
        let (tx, mut rx) = mpsc::channel(64);
        let (stop, shutdown) = watch::channel(false);
        assert!(stop.send(true).is_ok());
        assert!(matches!(
            drive_grpc(attach, tx, shutdown).await,
            TaskExit::Completed
        ));
        assert_eq!(forwarded(&mut rx), vec!["tick 101", "tick 102", "tick 103"]);

        let (attach, events, _exit) = parts(8);
        events.send(vec![tick(104)]).await.expect("queued");
        let (tx, mut rx) = mpsc::channel(64);
        let (stop, shutdown) = watch::channel(false);
        drop(stop);
        assert!(matches!(
            drive_grpc(attach, tx, shutdown).await,
            TaskExit::Completed
        ));
        assert_eq!(forwarded(&mut rx), vec!["tick 104"]);

        let (attach, events, _exit) = parts(8);
        events
            .send(vec![tick(105), tick(106), tick(107)])
            .await
            .expect("queued");
        let (tx, _stuck) = mpsc::channel(1);
        let (stop, shutdown) = watch::channel(false);
        assert!(stop.send(true).is_ok());
        let started = Instant::now();
        assert!(matches!(
            drive_grpc(attach, tx, shutdown).await,
            TaskExit::DrainTimedOut
        ));
        assert_eq!(started.elapsed(), STOP_DRAIN_TIMEOUT);
    }

    #[tokio::test]
    async fn an_attach_whose_connection_died_finishes_failed() {
        let ended = |exit: TaskExit| match exit {
            TaskExit::Failed(DisconnectReason::TransportError { source }) => {
                source.to_string()
            }
            other => panic!("expected a transport failure, got {other:?}"),
        };
        let (attach, events, exit) = parts(8);
        drop(events);
        drop(exit);
        assert_eq!(
            ended(attach.finish().await),
            "gRPC connection ended without an outcome"
        );
        let (attach, events, exit) = parts(8);
        drop(exit);
        drop(events);
        let (tx, _rx) = mpsc::channel(8);
        let (_stop, shutdown) = watch::channel(false);
        assert_eq!(
            ended(drive_grpc(attach, tx, shutdown).await),
            "gRPC connection ended without an outcome"
        );
    }
}
