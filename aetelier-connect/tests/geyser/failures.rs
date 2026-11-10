use std::sync::Arc;
use std::time::Duration;

use aetelier_connect::clients::disconnect::DisconnectReason;
use aetelier_connect::config::workers::common::ReconnectSection;
use aetelier_connect::framework::grpc::{
    AttachSpec, GeyserDial, GrpcAttach, GrpcConnections, STALE_AFTER, ping_request,
};
use aetelier_connect::framework::{DomainEvent, RateWindow, SourceMetrics, TaskExit};
use futures::StreamExt;
use tokio::time::{Instant, sleep, timeout};
use yellowstone_grpc_proto::prelude::SubscribeRequest;
use yellowstone_grpc_proto::tonic::metadata::MetadataValue;
use yellowstone_grpc_proto::tonic::{Code, Status};

use crate::chain::{
    Chain, account_include, address, kinds, next_batch, pong, section, server_ping, spec,
    watching,
};
use crate::server::{
    Admission, DuplexDial, MockConnection, MockGeyser, PairDial, PairPeer, duplex_pool,
    subscribed,
};

const MISSING_TRAILER: &str = "protocol error: missing grpc-status trailer, stream was \
                               terminated without a final status (possible truncation \
                               by a proxy or load balancer)";
const HTTP_429: &str = "grpc-status header missing, mapped from HTTP status code 429";

fn attach(pool: &GrpcConnections) -> GrpcAttach {
    pool.attach(spec(section(), watching(1), SourceMetrics::default(), 64))
        .expect("the attach is admitted")
}

fn paired_pool(pair: &PairDial) -> GrpcConnections {
    let dial: Arc<dyn GeyserDial> = Arc::new(pair.clone());
    GrpcConnections::new(dial)
}

async fn within(attach: &mut GrpcAttach, secs: u64) -> Vec<DomainEvent> {
    timeout(Duration::from_secs(secs), attach.recv())
        .await
        .expect("a batch arrives within the wait")
        .expect("the attach is open")
}

fn rejected(exit: TaskExit) -> String {
    match exit {
        TaskExit::Failed(DisconnectReason::ProtocolRejection { code: 0, reason }) => {
            reason
        }
        other => panic!("expected a code 0 rejection, got {other:?}"),
    }
}

fn lost_reason(batch: &[DomainEvent]) -> &str {
    match batch {
        [DomainEvent::ConnectionLost { reason }] => reason,
        other => panic!("expected one loss, got {other:?}"),
    }
}

async fn delivering(
    mock: &MockGeyser,
    chain: &Chain,
    a: &mut GrpcAttach,
) -> MockConnection {
    let connection = subscribed(mock).await;
    connection.send_all([chain.meta(100), chain.meta(101)]);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    assert_eq!(kinds(&next_batch(a).await), vec!["row 102:0", "tick 102"]);
    connection
}

async fn reconnects_at_tip(
    mock: &MockGeyser,
    dial: &DuplexDial,
    a: &mut GrpcAttach,
    failed_at: Instant,
    backoff: Duration,
    first: &SubscribeRequest,
) -> String {
    let lost = next_batch(a).await;
    let reason = lost_reason(&lost).to_string();
    let reconnected = subscribed(mock).await;
    let dials = dial.dials();
    assert_eq!(dials.len(), 2, "{reason}");
    assert_eq!(dials[1] - failed_at, backoff, "{reason}");
    assert_eq!(reconnected.opened_at(), dials[1]);
    assert_eq!(reconnected.first_request(), first, "{reason}");
    assert_eq!(reconnected.first_request().from_slot, None);
    assert_eq!(kinds(&next_batch(a).await), vec!["resumed"]);
    let chain = Chain::recent();
    reconnected.send_all([chain.meta(110), chain.meta(111)]);
    reconnected.send_all(chain.bank(112, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(a).await),
        vec!["gap 102->112 lost 9", "row 112:0", "tick 112"],
        "{reason}"
    );
    reason
}

async fn paired(
    pair: &PairDial,
    chain: &Chain,
    a: &mut GrpcAttach,
) -> (PairPeer, SubscribeRequest) {
    let mut peer = pair.next_peer().await;
    let first = peer.requests.next().await.expect("the opening request");
    assert_eq!(peer.requests.next().await, Some(ping_request(1)));
    let opening = [pong(1), chain.meta(100), chain.meta(101)];
    for update in opening
        .into_iter()
        .chain(chain.bank(102, &[(0, &[address(1)])]))
    {
        peer.updates
            .send(Ok(update))
            .await
            .expect("the session holds the stream");
    }
    assert_eq!(kinds(&next_batch(a).await), vec!["row 102:0", "tick 102"]);
    (peer, first)
}

async fn repaired_after(
    pair: &PairDial,
    a: &mut GrpcAttach,
    failed_at: Instant,
    first: &SubscribeRequest,
) -> (Duration, String) {
    let lost = next_batch(a).await;
    let reason = lost_reason(&lost).to_string();
    let mut again = pair.next_peer().await;
    let waited = Instant::now() - failed_at;
    assert_eq!(
        again.requests.next().await.as_ref(),
        Some(first),
        "{reason}"
    );
    assert_eq!(again.requests.next().await, Some(ping_request(1)));
    (waited, reason)
}

#[tokio::test(start_paused = true)]
async fn retryable_statuses_reconnect_at_tip_after_twice_the_backoff() {
    let statuses = [
        Status::unavailable("upstream connect error"),
        Status::aborted("stream aborted"),
        Status::internal("internal error"),
        Status::unknown(MISSING_TRAILER),
        Status::cancelled("stream cancelled"),
        Status::deadline_exceeded("deadline exceeded"),
    ];
    for status in statuses {
        let code = status.code();
        let mock = MockGeyser::default();
        let (pool, dial) = duplex_pool(&mock);
        let chain = Chain::recent();
        let mut a = attach(&pool);
        let connection = delivering(&mock, &chain, &mut a).await;
        let first = connection.first_request().clone();
        connection.fail(status);
        let failed_at = Instant::now();
        let reason = reconnects_at_tip(
            &mock,
            &dial,
            &mut a,
            failed_at,
            Duration::from_secs(2),
            &first,
        )
        .await;
        assert!(reason.contains(&format!("gRPC {code:?}")), "{reason}");
        assert_eq!(mock.subscribes().len(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn a_connection_reset_reconnects_at_tip_after_twice_the_backoff() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    let connection = delivering(&mock, &chain, &mut a).await;
    let first = connection.first_request().clone();
    connection.reset();
    let failed_at = Instant::now();
    let reason = reconnects_at_tip(
        &mock,
        &dial,
        &mut a,
        failed_at,
        Duration::from_secs(2),
        &first,
    )
    .await;
    assert_eq!(
        reason,
        "transport error: gRPC Unknown: h2 protocol error: error reading a body from \
         connection: stream closed because of a broken pipe"
    );
}

#[tokio::test(start_paused = true)]
async fn a_refused_dial_reconnects_after_twice_the_backoff() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    dial.refuse_next(2);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    for _ in 0..2 {
        let lost = within(&mut a, 10).await;
        assert!(
            lost_reason(&lost).contains("connection refused"),
            "{lost:?}"
        );
    }
    let connection = subscribed(&mock).await;
    let dials = dial.dials();
    assert_eq!(dials.len(), 3);
    assert_eq!(dials[1] - dials[0], Duration::from_secs(2));
    assert_eq!(dials[2] - dials[1], Duration::from_secs(4));
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["resumed"]);
    let first = connection.first_request();
    assert_eq!(account_include(first), vec![address(1).to_base58()]);
    assert_eq!(first.from_slot, None);
    assert_eq!(mock.subscribes(), vec![first.clone()]);
    connection.send_all([chain.meta(100), chain.meta(101)]);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 102:0", "tick 102"]
    );
}

#[tokio::test(start_paused = true)]
async fn data_loss_reconnects_at_tip_after_twice_the_backoff() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    let connection = delivering(&mock, &chain, &mut a).await;
    let first = connection.first_request().clone();
    connection.fail(Status::data_loss("lagged"));
    let failed_at = Instant::now();
    let reason = reconnects_at_tip(
        &mock,
        &dial,
        &mut a,
        failed_at,
        Duration::from_secs(2),
        &first,
    )
    .await;
    assert!(reason.contains("gRPC DataLoss: lagged"), "{reason}");
}

#[tokio::test(start_paused = true)]
async fn resource_exhausted_waits_out_a_ten_second_cooldown() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    let connection = delivering(&mock, &chain, &mut a).await;
    let first = connection.first_request().clone();
    connection.fail(Status::resource_exhausted(
        "too many streams for this token",
    ));
    let failed_at = Instant::now();
    let reason = reconnects_at_tip(
        &mock,
        &dial,
        &mut a,
        failed_at,
        Duration::from_secs(10),
        &first,
    )
    .await;
    assert!(reason.contains("ResourceExhausted"), "{reason}");
}

#[tokio::test(start_paused = true)]
async fn a_cool_down_outlives_the_last_detach_and_holds_the_next_connection() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    let connection = delivering(&mock, &chain, &mut a).await;
    connection.fail(Status::resource_exhausted(
        "too many streams for this token",
    ));
    let failed_at = Instant::now();
    let lost = next_batch(&mut a).await;
    assert!(lost_reason(&lost).contains("ResourceExhausted"), "{lost:?}");
    drop(a);
    assert!(pool.connections().is_empty());
    sleep(Duration::from_secs(4)).await;
    let mut b = attach(&pool);
    let reconnected = subscribed(&mock).await;
    let dials = dial.dials();
    assert_eq!(dials.len(), 2);
    assert_eq!(dials[1] - failed_at, Duration::from_secs(10));
    reconnected.send_all([chain.meta(110), chain.meta(111)]);
    reconnected.send_all(chain.bank(112, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut b).await),
        vec!["row 112:0", "tick 112"]
    );
    drop(b);
    let reattached_at = Instant::now();
    let _c = attach(&pool);
    let _again = subscribed(&mock).await;
    assert_eq!(dial.dials().len(), 3);
    assert_eq!(dial.dials()[2], reattached_at);
}

#[tokio::test(start_paused = true)]
async fn unavailable_from_http_429_waits_out_the_cooldown_and_retry_after_extends_it() {
    let mut retry_after = Status::unavailable(HTTP_429);
    retry_after
        .metadata_mut()
        .insert("retry-after", MetadataValue::from_static("30"));
    let cases = [
        (Status::unavailable(HTTP_429), Duration::from_secs(10)),
        (retry_after, Duration::from_secs(30)),
        (
            Status::unavailable("upstream connect error"),
            Duration::from_secs(2),
        ),
    ];
    for (status, cool_down) in cases {
        let pair = PairDial::default();
        let pool = paired_pool(&pair);
        let chain = Chain::recent();
        let mut a = attach(&pool);
        let (peer, first) = paired(&pair, &chain, &mut a).await;
        peer.updates
            .send(Err(status))
            .await
            .expect("the session holds the stream");
        let failed_at = Instant::now();
        let (waited, reason) = repaired_after(&pair, &mut a, failed_at, &first).await;
        assert_eq!(waited, cool_down, "{reason}");
        assert!(reason.contains("Unavailable"), "{reason}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_stream_ended_with_status_ok_reconnects_after_one_backoff() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    let connection = delivering(&mock, &chain, &mut a).await;
    let first = connection.first_request().clone();
    connection.finish();
    let failed_at = Instant::now();
    let reason = reconnects_at_tip(
        &mock,
        &dial,
        &mut a,
        failed_at,
        Duration::from_secs(1),
        &first,
    )
    .await;
    assert!(
        reason.contains("mock-geyser 0.0.0") && reason.contains("status Ok"),
        "{reason}"
    );
}

#[tokio::test(start_paused = true)]
async fn rejection_statuses_are_terminal_before_and_after_data() {
    let statuses = [
        Status::unauthenticated("invalid x-token"),
        Status::permission_denied("not entitled"),
        Status::invalid_argument("bad filter"),
        Status::unimplemented("subscribe is not served"),
        Status::not_found("no such stream"),
        Status::failed_precondition("node is syncing"),
        Status::already_exists("the stream already exists"),
    ];
    for status in statuses {
        let code = format!("gRPC {:?}", status.code());

        let refusing = MockGeyser::default();
        refusing.admit_next(Admission::Refuse(status.clone()));
        let (pool, dial) = duplex_pool(&refusing);
        let mut a = attach(&pool);
        let ended = timeout(Duration::from_secs(1), a.recv())
            .await
            .expect("the attach ends");
        assert!(ended.is_none(), "{code}: {ended:?}");
        let reason = rejected(a.finish().await);
        assert!(reason.starts_with(&code), "{reason}");
        assert!(reason.contains("yellowstone-grpc-proto@14.0.1"), "{reason}");
        assert!(reason.contains("mock-geyser 0.0.0"), "{reason}");
        sleep(Duration::from_secs(120)).await;
        assert_eq!(dial.dials().len(), 1, "{code}");
        assert_eq!(refusing.subscribes().len(), 1, "{code}");

        let serving = MockGeyser::default();
        serving.admit_next(Admission::Accept);
        let (pool, dial) = duplex_pool(&serving);
        let chain = Chain::recent();
        let mut a = attach(&pool);
        let connection = delivering(&serving, &chain, &mut a).await;
        connection.fail(status);
        let mut after = Vec::new();
        while let Some(batch) = a.recv().await {
            after.extend(kinds(&batch));
        }
        assert!(after.is_empty(), "{code}: {after:?}");
        let reason = rejected(a.finish().await);
        assert!(reason.starts_with(&code), "{reason}");
        assert!(reason.contains("mock-geyser 0.0.0"), "{reason}");
        sleep(Duration::from_secs(120)).await;
        assert_eq!(dial.dials().len(), 1, "{code}");
        assert_eq!(serving.subscribes().len(), 1, "{code}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_ping_write_reconnects_at_tip_after_one_backoff() {
    let pair = PairDial::default();
    let pool = paired_pool(&pair);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    let (peer, first) = paired(&pair, &chain, &mut a).await;
    let PairPeer { requests, updates } = peer;
    drop(requests);
    updates
        .send(Ok(server_ping()))
        .await
        .expect("the session holds the stream");
    let failed_at = Instant::now();
    let (waited, reason) = repaired_after(&pair, &mut a, failed_at, &first).await;
    assert_eq!(waited, Duration::from_secs(1), "{reason}");
    assert!(reason.starts_with("heartbeat write failed"), "{reason}");
}

#[tokio::test(start_paused = true)]
async fn ten_seconds_without_block_meta_reconnect_at_tip() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    let connection = delivering(&mock, &chain, &mut a).await;
    let first = connection.first_request().clone();
    let quiet_from = Instant::now();
    let lost = within(&mut a, 11).await;
    assert_eq!(Instant::now() - quiet_from, STALE_AFTER);
    assert!(
        lost_reason(&lost).starts_with("stale connection"),
        "{lost:?}"
    );
    let reconnected = subscribed(&mock).await;
    let dials = dial.dials();
    assert_eq!(dials[1] - quiet_from, STALE_AFTER + Duration::from_secs(1));
    assert_eq!(reconnected.first_request(), &first);
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["resumed"]);
    reconnected.send_all([chain.meta(110), chain.meta(111)]);
    reconnected.send_all(chain.bank(112, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["gap 102->112 lost 9", "row 112:0", "tick 112"]
    );
}

#[tokio::test(start_paused = true)]
async fn an_unknown_status_code_reconnects_after_twice_the_backoff() {
    let pair = PairDial::default();
    let pool = paired_pool(&pair);
    let chain = Chain::recent();
    let mut a = attach(&pool);
    let (peer, first) = paired(&pair, &chain, &mut a).await;
    peer.updates
        .send(Err(Status::new(Code::Ok, "")))
        .await
        .expect("the session holds the stream");
    let failed_at = Instant::now();
    let (waited, reason) = repaired_after(&pair, &mut a, failed_at, &first).await;
    assert_eq!(waited, Duration::from_secs(2), "{reason}");
    assert!(reason.starts_with("transport error: gRPC Ok"), "{reason}");
}

#[tokio::test(start_paused = true)]
async fn reconnect_attempts_are_paced_to_ten_per_minute() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    dial.refuse_next(20);
    let paced = AttachSpec {
        reconnect: Some(ReconnectSection {
            initial_delay_ms: Some(1),
            max_delay_ms: Some(1),
            max_attempts: None,
            jitter_factor: Some(0.0),
        }),
        connect_rate: RateWindow {
            max: 10,
            per: Duration::from_secs(60),
        },
        ..spec(section(), watching(1), SourceMetrics::default(), 64)
    };
    let _a = pool.attach(paced).expect("the attach is admitted");
    sleep(Duration::from_secs(59)).await;
    assert_eq!(dial.dials().len(), 10);
    sleep(Duration::from_secs(2)).await;
    let dials = dial.dials();
    assert!(dials.len() >= 11, "{}", dials.len());
    assert!(dials[9] - dials[0] < Duration::from_secs(1));
    assert_eq!(dials[10] - dials[0], Duration::from_secs(60));
    for window in dials.windows(11) {
        assert!(window[10] - window[0] >= Duration::from_secs(60));
    }
}

#[tokio::test(start_paused = true)]
async fn a_connection_opened_during_a_held_wait_keeps_the_earlier_connect_attempts() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    dial.refuse_next(4);
    let paced = |initial_delay_ms| AttachSpec {
        reconnect: Some(ReconnectSection {
            initial_delay_ms: Some(initial_delay_ms),
            max_delay_ms: Some(30_000),
            max_attempts: None,
            jitter_factor: Some(0.0),
        }),
        connect_rate: RateWindow {
            max: 2,
            per: Duration::from_secs(60),
        },
        ..spec(section(), watching(1), SourceMetrics::default(), 64)
    };
    let mut a = pool.attach(paced(2_500)).expect("the attach is admitted");
    for _ in 0..2 {
        lost_reason(&within(&mut a, 10).await);
    }
    drop(a);
    let mut b = pool.attach(paced(1)).expect("the attach is admitted");
    let _connection = subscribed(&mock).await;
    let dials = dial.dials();
    let offsets: Vec<Duration> = dials.iter().map(|at| *at - dials[0]).collect();
    assert_eq!(
        offsets,
        [0, 5, 60, 65, 120].map(Duration::from_secs).to_vec()
    );
    for kind in ["lost", "lost", "resumed"] {
        assert_eq!(kinds(&within(&mut b, 1).await), vec![kind]);
    }
}

#[tokio::test(start_paused = true)]
async fn max_attempts_opens_the_circuit_and_the_attach_never_gives_up() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    dial.refuse_next(3);
    let chain = Chain::recent();
    let bounded = AttachSpec {
        reconnect: Some(ReconnectSection {
            initial_delay_ms: Some(1_000),
            max_delay_ms: Some(2_000),
            max_attempts: Some(3),
            jitter_factor: Some(0.0),
        }),
        ..spec(section(), watching(1), SourceMetrics::default(), 64)
    };
    let mut a = pool.attach(bounded).expect("the attach is admitted");
    for _ in 0..3 {
        let lost = within(&mut a, 10).await;
        lost_reason(&lost);
    }
    let connection = subscribed(&mock).await;
    let dials = dial.dials();
    let offsets: Vec<Duration> = dials.iter().map(|at| *at - dials[0]).collect();
    assert_eq!(offsets, [0, 2, 6, 14].map(Duration::from_secs).to_vec());
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["resumed"]);
    connection.send_all([chain.meta(100), chain.meta(101)]);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 102:0", "tick 102"]
    );
}
