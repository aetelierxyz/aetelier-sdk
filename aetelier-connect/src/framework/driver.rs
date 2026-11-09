//! The generic Ingest→Sync drive loop shared by every adapter's `spawn`.
//!
//! One connection's lifecycle: the [`WssTransport`] decodes `D::Event` → the
//! venue [`Normalizer`] maps each to `DomainEvent` → the caller's `tx`. Graceful
//! shutdown aborts the socket and best-effort flushes what is buffered; the
//! transport's [`WssExitReason`] is mapped onto a [`TaskExit`] terminal.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinError;

use super::budget::SourceMetrics;
use super::model::{DomainEvent, Normalizer};
use super::protocol::ProtocolHooks;
use super::registry::TaskExit;
use super::transport::WssTransport;
use crate::clients::disconnect::{DisconnectReason, WssExitReason};
use crate::clients::wss::WssDecoder;

/// Default in-process Ingest→Sync buffer depth for one connection.
pub const DEFAULT_RAW_BUFFER: usize = 4096;

/// Bound on the graceful-shutdown drain flush.
///
/// Derivation: the legitimate worst case is CPU-bound (a full
/// [`DEFAULT_RAW_BUFFER`] of already-decoded events through normalize+stamp,
/// well under 50 ms); the only slow path is a stuck-but-alive downstream
/// consumer, which is exactly the pathology this bound exists to cut. 2 s is
/// ~40x the legitimate worst case and leaves the rest of the process kill
/// grace (docker default 10 s) for the final sink flush and teardown.
pub const STOP_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

pub const CLOSE_HANDSHAKE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(1);

/// Drive one venue connection end-to-end. `N::Event` must equal `D::Event`
/// (the normalizer consumes exactly what the decoder yields). `metrics` is the
/// worker's shared counter handle — the transport bumps msgs/decode/drop
/// counters through its clone.
pub async fn drive<H, D, N>(
    hooks: Arc<H>,
    symbols: Vec<String>,
    declared: aetelier_types::config::markets::market_config::DeclaredSet,
    normalizer: N,
    tx: mpsc::Sender<DomainEvent>,
    mut shutdown: watch::Receiver<bool>,
    buffer: usize,
    metrics: SourceMetrics,
) -> TaskExit
where
    H: ProtocolHooks,
    D: WssDecoder,
    N: Normalizer<Event = D::Event>,
{
    // Internal Ingest buffer: transport decodes D::Event → here; this loop
    // normalizes → DomainEvent → the caller's `tx`.
    let (raw_tx, mut raw_rx) = mpsc::channel::<(D::Event, u64)>(buffer);
    let rtt_us = Arc::new(AtomicU64::new(0));
    // The transport mints the epoch when its socket actually connects; this
    // clone is how the loop reads that identity back. Events only reach the
    // loop after the connect, so the value is always the live socket's.
    let epoch_src = metrics.clone();
    let transport = WssTransport::<H, D>::new(hooks, symbols, declared);
    let mut transport_task = tokio::spawn(transport.run(
        raw_tx,
        rtt_us.clone(),
        metrics,
        Some(shutdown.clone()),
    ));
    let mut recv_seq: u64 = 0;

    loop {
        tokio::select! {
            // Graceful shutdown: abort the socket, then flush what is already
            // buffered, bounded by STOP_DRAIN_TIMEOUT — a drain that exceeds
            // the bound (stuck downstream consumer) exits DrainTimedOut and
            // the outstanding artifacts are the caller's `partial` outcome.
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    if tokio::time::timeout(CLOSE_HANDSHAKE_TIMEOUT, &mut transport_task)
                        .await
                        .is_err()
                    {
                        transport_task.abort();
                    }
                    let flush =
                        drain_buffered(
                            &mut raw_rx, &normalizer, &tx, &rtt_us, recv_seq,
                            epoch_src.conn_epoch_us(),
                        );
                    return match tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush).await {
                        Ok(()) => TaskExit::Completed,
                        Err(_) => TaskExit::DrainTimedOut,
                    };
                }
            }
            maybe = raw_rx.recv() => match maybe {
                Some((ev, receipt_us)) => {
                    let rtt = rtt_us.load(Ordering::Relaxed);
                    for mut de in normalizer.normalize(ev) {
                        recv_seq += 1;
                        de.stamp_local(receipt_us, rtt, recv_seq, epoch_src.conn_epoch_us());
                        if tx.send(de).await.is_err() {
                            // Sync end dropped — intentional shutdown.
                            transport_task.abort();
                            return TaskExit::Completed;
                        }
                    }
                }
                // Transport closed its sender: await the exit reason and map it.
                None => {
                    if *shutdown.borrow() {
                        let flush = drain_buffered(
                            &mut raw_rx, &normalizer, &tx, &rtt_us, recv_seq,
                            epoch_src.conn_epoch_us(),
                        );
                        return match tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush).await
                        {
                            Ok(()) => TaskExit::Completed,
                            Err(_) => TaskExit::DrainTimedOut,
                        };
                    }
                    return exit_to_task(transport_task.await);
                }
            },
        }
    }
}

/// Flush every already-buffered event through the normalizer to the caller.
///
/// Non-blocking on the receive side (`try_recv` over what is buffered); the
/// only await is the downstream `tx.send`, which is why the caller bounds
/// this with [`STOP_DRAIN_TIMEOUT`].
async fn drain_buffered<E, N>(
    raw_rx: &mut mpsc::Receiver<(E, u64)>,
    normalizer: &N,
    tx: &mpsc::Sender<DomainEvent>,
    rtt_us: &AtomicU64,
    mut recv_seq: u64,
    conn_epoch_us: u64,
) where
    E: Send + 'static,
    N: Normalizer<Event = E>,
{
    while let Ok((ev, receipt_us)) = raw_rx.try_recv() {
        let rtt = rtt_us.load(Ordering::Relaxed);
        for mut de in normalizer.normalize(ev) {
            recv_seq += 1;
            de.stamp_local(receipt_us, rtt, recv_seq, conn_epoch_us);
            if tx.send(de).await.is_err() {
                return;
            }
        }
    }
}

/// Map a finished transport run onto the terminal outcome.
/// A clean server close / caller-driven shutdown is a natural completion;
/// everything else surfaces for the worker's reconnect policy.
pub fn exit_to_task(joined: Result<WssExitReason, JoinError>) -> TaskExit {
    match joined {
        Ok(reason) => match DisconnectReason::from(reason) {
            DisconnectReason::CleanClose | DisconnectReason::ReceiverDropped => {
                TaskExit::Completed
            }
            other => TaskExit::Failed(other),
        },
        Err(_) => TaskExit::Failed(DisconnectReason::TransportError {
            source: "transport task panicked".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::TradeSide;
    use aetelier_types::orderbooks::f64_to_decimal;
    use aetelier_types::trades::Trade;
    use aetelier_types::trading_pair::TradingPair;

    struct StubNormalizer;

    impl Normalizer for StubNormalizer {
        type Event = u64;

        fn normalize(&self, event: Self::Event) -> Vec<DomainEvent> {
            vec![DomainEvent::Trade {
                trade: Trade {
                    source_trade_ts_us: event,
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: TradingPair::new("BTC", "USDT"),
                    side: TradeSide::Buy,
                    amount: f64_to_decimal(1.0),
                    price: f64_to_decimal(1.0),
                    exchange: "stub".to_string(),
                    id: event.to_string(),
                    origin: Default::default(),
                },
                sequence: Some(event),
            }]
        }
    }

    #[tokio::test]
    async fn drain_flushes_buffered_events_and_completes() {
        let (raw_tx, mut raw_rx) = mpsc::channel::<(u64, u64)>(16);
        let (tx, mut rx) = mpsc::channel::<DomainEvent>(16);
        let rtt = AtomicU64::new(0);
        for i in 0..5u64 {
            raw_tx.try_send((i, i)).unwrap();
        }

        let flush = drain_buffered(&mut raw_rx, &StubNormalizer, &tx, &rtt, 0, 11);
        tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush)
            .await
            .expect("drain of a small buffer finishes well inside the bound");

        let mut got = 0;
        while rx.try_recv().is_ok() {
            got += 1;
        }
        assert_eq!(got, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn stuck_consumer_hits_the_drain_bound() {
        let (raw_tx, mut raw_rx) = mpsc::channel::<(u64, u64)>(16);
        // Downstream capacity 1, pre-filled, receiver alive but never
        // consuming — the send inside the drain parks forever.
        let (tx, _rx) = mpsc::channel::<DomainEvent>(1);
        let rtt = AtomicU64::new(0);
        tx.try_send(StubNormalizer.normalize(99).pop().unwrap())
            .unwrap();
        raw_tx.try_send((1, 1)).unwrap();

        let flush = drain_buffered(&mut raw_rx, &StubNormalizer, &tx, &rtt, 0, 11);
        let out = tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush).await;
        assert!(out.is_err(), "stuck consumer must trip the drain bound");
    }

    const HYPERLIQUID_FRAMES: &str =
        include_str!("../../datasets/hyperliquid/btc_book_trade.jsonl");

    fn hyperliquid_events()
    -> Vec<crate::sources::hyperliquid::events::HyperliquidWssEvent> {
        use crate::sources::hyperliquid::decoder::HyperliquidDecoder;
        HYPERLIQUID_FRAMES
            .lines()
            .filter_map(|line| HyperliquidDecoder::decode(line.trim()).unwrap())
            .collect()
    }

    fn unstamped(ev: &DomainEvent) -> String {
        let mut ev = ev.clone();
        ev.stamp_local(0, 0, 0, 0);
        format!("{ev:?}")
    }

    fn stamps(ev: &DomainEvent) -> (u64, Option<u64>, Option<(u64, u64)>) {
        match ev {
            DomainEvent::Book(d) => (
                d.local_orderbook_ts_us,
                Some(d.source_orderbook_rtt_us),
                None,
            ),
            DomainEvent::Trade { trade, .. } => (
                trade.local_trade_ts_us,
                Some(trade.source_trade_rtt_us),
                None,
            ),
            DomainEvent::FundingRate(fr) => (
                fr.local_funding_ts_us,
                None,
                Some((fr.recv_seq, fr.conn_epoch_us)),
            ),
            DomainEvent::OpenInterest(oi) => (
                oi.local_oi_ts_us,
                None,
                Some((oi.recv_seq, oi.conn_epoch_us)),
            ),
            other => panic!("the hyperliquid normalizer emitted {other:?}"),
        }
    }

    fn expected_hyperliquid_events() -> Vec<(usize, String)> {
        use crate::framework::adapters::hyperliquid::HyperliquidNormalizer;
        let normalizer = HyperliquidNormalizer::default();
        let mut expected = Vec::new();
        for (frame, event) in hyperliquid_events().into_iter().enumerate() {
            for ev in normalizer.normalize(event) {
                expected.push((frame, unstamped(&ev)));
            }
        }
        for kind in ["Book(", "Trade {", "FundingRate(", "OpenInterest("] {
            assert!(
                expected.iter().any(|(_, shape)| shape.starts_with(kind)),
                "the fixture exercises {kind}"
            );
        }
        expected
    }

    #[tokio::test]
    async fn drain_stamps_real_hyperliquid_frames_in_order() {
        use crate::framework::adapters::hyperliquid::HyperliquidNormalizer;
        const RECEIPT_US: u64 = 1_785_932_639_900_000;
        const EPOCH_US: u64 = 1_785_932_600_000_000;
        let expected = expected_hyperliquid_events();
        let frames = hyperliquid_events();
        let (raw_tx, mut raw_rx) = mpsc::channel(frames.len());
        for (frame, event) in frames.into_iter().enumerate() {
            raw_tx.try_send((event, RECEIPT_US + frame as u64)).unwrap();
        }
        let (tx, mut rx) = mpsc::channel::<DomainEvent>(expected.len() + 1);
        let rtt = AtomicU64::new(1_250);
        let normalizer = HyperliquidNormalizer::default();

        let flush = drain_buffered(&mut raw_rx, &normalizer, &tx, &rtt, 40, EPOCH_US);
        tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush)
            .await
            .expect("a drain into an open downstream completes");
        drop(tx);
        assert!(raw_rx.try_recv().is_err(), "every buffered frame drained");

        let mut got = Vec::new();
        while let Some(ev) = rx.recv().await {
            got.push(ev);
        }
        assert_eq!(got.len(), expected.len());
        for (n, (ev, (frame, shape))) in got.iter().zip(&expected).enumerate() {
            assert_eq!(&unstamped(ev), shape, "event {n} keeps its wire order");
            let (local_us, rtt_us, sequenced) = stamps(ev);
            assert_eq!(local_us, RECEIPT_US + *frame as u64, "event {n}");
            if let Some(rtt_us) = rtt_us {
                assert_eq!(rtt_us, 1_250, "event {n}");
            }
            if let Some((recv_seq, conn_epoch_us)) = sequenced {
                assert_eq!(recv_seq, 40 + n as u64 + 1, "event {n}");
                assert_eq!(conn_epoch_us, EPOCH_US, "event {n}");
            }
        }
    }

    #[tokio::test]
    async fn drain_stops_at_a_closed_downstream() {
        use crate::framework::adapters::hyperliquid::HyperliquidNormalizer;
        let expected = expected_hyperliquid_events();
        let frames = hyperliquid_events();
        let total = frames.len();
        let (raw_tx, mut raw_rx) = mpsc::channel(total);
        for (frame, event) in frames.into_iter().enumerate() {
            raw_tx.try_send((event, 1 + frame as u64)).unwrap();
        }
        let (tx, rx) = mpsc::channel::<DomainEvent>(expected.len() + 1);
        drop(rx);
        let rtt = AtomicU64::new(0);
        let normalizer = HyperliquidNormalizer::default();

        let flush = drain_buffered(&mut raw_rx, &normalizer, &tx, &rtt, 0, 11);
        tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush)
            .await
            .expect("a closed downstream ends the drain at once");

        let mut left = 0;
        while raw_rx.try_recv().is_ok() {
            left += 1;
        }
        let consumed = expected[0].0 + 1;
        assert_eq!(
            left,
            total - consumed,
            "the drain stops at the first refused send"
        );
    }

    struct LocalVenue(String);

    impl ProtocolHooks for LocalVenue {
        fn endpoint(&self) -> String {
            self.0.clone()
        }

        fn subscribe_frames(
            &self,
            _symbols: &[String],
            _declared: &aetelier_types::config::markets::market_config::DeclaredSet,
        ) -> Vec<tokio_tungstenite::tungstenite::protocol::Message> {
            vec![tokio_tungstenite::tungstenite::protocol::Message::Text(
                "subscribe".into(),
            )]
        }
    }

    #[tokio::test]
    async fn drive_stamps_real_hyperliquid_frames_end_to_end() {
        use crate::framework::adapters::hyperliquid::HyperliquidNormalizer;
        use crate::sources::hyperliquid::decoder::HyperliquidDecoder;
        use futures_util::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};

        let expected = expected_hyperliquid_events();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _subscribe = ws.next().await;
            for line in HYPERLIQUID_FRAMES.lines() {
                if ws
                    .send(Message::Text(line.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = ws
                .close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "".into(),
                }))
                .await;
            while let Some(Ok(_)) = ws.next().await {}
        });

        let (tx, mut rx) = mpsc::channel::<DomainEvent>(DEFAULT_RAW_BUFFER);
        let collector = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(ev) = rx.recv().await {
                got.push(ev);
            }
            got
        });
        let (sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let before_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            drive::<LocalVenue, HyperliquidDecoder, HyperliquidNormalizer>(
                Arc::new(LocalVenue(format!("ws://{addr}"))),
                vec!["BTC".to_string()],
                aetelier_types::config::markets::market_config::DeclaredSet::all(),
                HyperliquidNormalizer::default(),
                tx,
                sd_rx,
                DEFAULT_RAW_BUFFER,
                metrics.clone(),
            ),
        )
        .await
        .expect("a closed capture ends the drive");
        let after_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        drop(sd_tx);
        assert!(matches!(exit, TaskExit::Completed), "{exit:?}");
        let got = collector.await.unwrap();

        assert_eq!(got.len(), expected.len());
        let conn_epoch_us = metrics.conn_epoch_us();
        assert!(conn_epoch_us >= before_us && conn_epoch_us <= after_us);
        let mut frame_stamp: Option<(usize, (u64, Option<u64>))> = None;
        for (n, (ev, (frame, shape))) in got.iter().zip(&expected).enumerate() {
            assert_eq!(&unstamped(ev), shape, "event {n} keeps its wire order");
            let (local_us, rtt_us, sequenced) = stamps(ev);
            assert!(
                local_us >= before_us && local_us <= after_us,
                "event {n} carries its receipt time"
            );
            if let Some((recv_seq, epoch_us)) = sequenced {
                assert_eq!(recv_seq, n as u64 + 1, "event {n}");
                assert_eq!(epoch_us, conn_epoch_us, "event {n}");
            }
            match frame_stamp {
                Some((last, stamp)) if last == *frame => {
                    assert_eq!(stamp.0, local_us, "one receipt per frame (event {n})");
                    if let (Some(a), Some(b)) = (stamp.1, rtt_us) {
                        assert_eq!(a, b, "one round-trip per frame (event {n})");
                    }
                }
                _ => frame_stamp = Some((*frame, (local_us, rtt_us))),
            }
        }
        let m = metrics.snapshot();
        assert_eq!(m.msgs, hyperliquid_events().len() as u64);
        assert_eq!(m.decode_err, 0);
    }
}

#[cfg(test)]
mod probe_driver {
    use super::*;
    use crate::framework::protocol::{DeclaredSet, ProtocolHooks};
    use tokio_tungstenite::tungstenite::protocol::Message;

    struct QuietHooks(String);
    impl ProtocolHooks for QuietHooks {
        fn endpoint(&self) -> String {
            self.0.clone()
        }
        fn subscribe_frames(&self, _s: &[String], _d: &DeclaredSet) -> Vec<Message> {
            vec![Message::Text("sub".into())]
        }
    }

    struct NoopDec;
    impl crate::clients::wss::WssDecoder for NoopDec {
        type Event = u64;
        fn decode(_t: &str) -> Result<Option<u64>, Box<crate::errors::ExchangeError>> {
            Ok(None)
        }
    }

    struct NoopNorm;
    impl Normalizer for NoopNorm {
        type Event = u64;
        fn normalize(&self, _e: u64) -> Vec<DomainEvent> {
            Vec::new()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_taskexit_is_stable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    use futures_util::StreamExt as _;
                    while ws.next().await.is_some() {}
                });
            }
        });

        let mut failed = 0;
        let mut completed = 0;
        for _ in 0..200 {
            let (sd_tx, sd_rx) = watch::channel(false);
            let (tx, _rx) = mpsc::channel::<DomainEvent>(64);
            let h = tokio::spawn(drive::<QuietHooks, NoopDec, NoopNorm>(
                Arc::new(QuietHooks(format!("ws://{addr}"))),
                vec!["T".into()],
                DeclaredSet::all(),
                NoopNorm,
                tx,
                sd_rx,
                64,
                SourceMetrics::default(),
            ));
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            sd_tx.send(true).unwrap();
            match h.await.unwrap() {
                TaskExit::Completed => completed += 1,
                other => {
                    failed += 1;
                    if failed == 1 {
                        println!("FIRST NON-COMPLETED: {other:?}");
                    }
                }
            }
        }
        println!("completed={completed} non_completed={failed}");
        assert_eq!(failed, 0, "graceful shutdown must always be Completed");
    }
}
