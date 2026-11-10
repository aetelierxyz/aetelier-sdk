use std::time::Duration;

use aetelier_connect::clients::disconnect::DisconnectReason;
use aetelier_connect::config::workers::market_worker_config::GrpcSection;
use aetelier_connect::framework::grpc::{
    BLOCKS_META_FILTER, GrpcAttach, GrpcConnections, TRANSACTIONS_FILTER, ping_request,
};
use aetelier_connect::framework::{DomainEvent, SourceMetrics, TaskExit};
use tokio::time::{Instant, sleep, sleep_until};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeUpdate, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::Status;

use crate::chain::{
    Chain, account_include, address, kinds, next_batch, ping_id, pong, section,
    server_ping, spec, watching,
};
use crate::server::{MockConnection, MockGeyser, duplex_pool, subscribed};

fn join(
    pool: &GrpcConnections,
    section: GrpcSection,
    seed: u8,
    queue_banks: usize,
) -> (GrpcAttach, SourceMetrics) {
    let metrics = SourceMetrics::default();
    let attach = pool
        .attach(spec(section, watching(seed), metrics.clone(), queue_banks))
        .expect("the attach is admitted");
    (attach, metrics)
}

fn base58(seeds: &[u8]) -> Vec<String> {
    seeds
        .iter()
        .map(|&seed| address(seed).to_base58())
        .collect()
}

fn lead_in(connection: &MockConnection, chain: &Chain, first_slot: u64) {
    connection.send_all([chain.meta(first_slot), chain.meta(first_slot + 1)]);
}

async fn settle(connection: &mut MockConnection) {
    connection.send(server_ping());
    while ping_id(&connection.next_request().await).is_none() {}
}

fn rejected(exit: TaskExit) -> String {
    match exit {
        TaskExit::Failed(DisconnectReason::ProtocolRejection { code: 0, reason }) => {
            reason
        }
        other => panic!("expected a code 0 rejection, got {other:?}"),
    }
}

fn rows(batch: &[DomainEvent]) -> Vec<&aetelier_types::onchain::Transaction> {
    batch
        .iter()
        .filter_map(|event| match event {
            DomainEvent::Transaction(row) => Some(row.as_ref()),
            _ => None,
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn the_first_subscribe_is_one_full_request_without_from_slot_or_ping() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let (_a, _) = join(&pool, section(), 2, 8);
    let (_b, _) = join(&pool, section(), 1, 8);
    let mut connection = mock.next_connection().await;
    let first = connection.first_request().clone();
    assert_eq!(mock.subscribes(), vec![first.clone()]);
    assert_eq!(account_include(&first), base58(&[1, 2]));
    let transactions = &first.transactions[TRANSACTIONS_FILTER];
    assert_eq!(
        (transactions.vote, transactions.failed),
        (Some(false), None)
    );
    assert_eq!(first.transactions.len(), 1);
    assert_eq!(
        first.blocks_meta.keys().collect::<Vec<_>>(),
        vec![BLOCKS_META_FILTER]
    );
    assert_eq!(first.commitment, Some(CommitmentLevel::Finalized as i32));
    assert_eq!((first.from_slot, first.ping), (None, None));
    assert!(first.slots.is_empty() && first.accounts.is_empty());
    assert_eq!(connection.next_request().await, ping_request(1));
    assert_eq!(mock.tokens(), vec![Some("t".to_string())]);
}

#[tokio::test(start_paused = true)]
async fn startup_attaches_join_one_opening_request() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let (_b, _) = join(&pool, section(), 2, 8);
    let (_c, _) = join(&pool, section(), 3, 8);
    let mut connection = subscribed(&mock).await;
    assert_eq!(
        account_include(connection.first_request()),
        base58(&[1, 2, 3])
    );
    let started = Instant::now();
    lead_in(&connection, &chain, 100);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 102:0", "tick 102"]
    );
    for (slot, at) in [(103, 5), (104, 10)] {
        sleep_until(started + Duration::from_secs(at)).await;
        connection.send(chain.meta(slot));
    }
    let next = connection.next_request().await;
    assert_eq!(next, ping_request(2));
    assert_eq!(started.elapsed(), Duration::from_secs(15));
    assert_eq!(mock.subscribes().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn coverage_starts_two_slots_after_the_highest_slot_at_the_pong() {
    let mock = MockGeyser::default();
    mock.auto_pong(false);
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let connection = subscribed(&mock).await;
    connection.send_all([chain.meta(100), chain.meta(101), pong(1)]);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    connection.send_all(chain.bank(103, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 103:0", "tick 103"]
    );
    connection.send_all(chain.bank(104, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 104:0", "tick 104"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_disconnect_reconnects_at_tip_with_the_same_request_and_one_exact_slot_gap() {
    let mock = MockGeyser::default();
    mock.auto_pong(false);
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let (mut b, _) = join(&pool, section(), 2, 8);
    let connection = subscribed(&mock).await;
    connection.send(pong(1));
    lead_in(&connection, &chain, 100);
    for slot in 102..=105 {
        connection.send_all(chain.bank(slot, &[(0, &[address(1), address(2)])]));
    }
    for slot in 102..=105 {
        let expected = vec![format!("row {slot}:0"), format!("tick {slot}")];
        assert_eq!(kinds(&next_batch(&mut a).await), expected);
        assert_eq!(kinds(&next_batch(&mut b).await), expected);
    }
    connection.reset();
    let reset_at = Instant::now();
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["lost"]);
    assert_eq!(kinds(&next_batch(&mut b).await), vec!["lost"]);
    let reconnected = subscribed(&mock).await;
    let dials = dial.dials();
    assert_eq!(dials.len(), 2);
    assert_eq!(dials[1] - reset_at, Duration::from_secs(2));
    assert_eq!(reconnected.first_request(), connection.first_request());
    assert_eq!(reconnected.first_request().from_slot, None);
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["resumed"]);
    assert_eq!(kinds(&next_batch(&mut b).await), vec!["resumed"]);
    reconnected.send_all([chain.meta(139), chain.meta(140), pong(1), chain.meta(141)]);
    reconnected.send_all(chain.bank(142, &[(0, &[address(1), address(2)])]));
    let first = next_batch(&mut a).await;
    let second = next_batch(&mut b).await;
    let expected = vec!["gap 105->142 lost 36", "row 142:0", "tick 142"];
    assert_eq!(kinds(&first), expected);
    assert_eq!(kinds(&second), expected);
    assert!(matches!(
        (&first[0], &second[0]),
        (DomainEvent::SlotGap(left), DomainEvent::SlotGap(right)) if left == right
    ));
    assert_eq!(mock.subscribes().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn held_banks_are_flushed_before_the_loss_is_reported() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let mut connection = subscribed(&mock).await;
    lead_in(&connection, &chain, 100);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 102:0", "tick 102"]
    );
    connection.send_all(chain.bank(104, &[(0, &[address(1)])]));
    settle(&mut connection).await;
    assert!(a.try_recv().is_none());
    connection.reset();
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["gap 102->104 lost 1", "row 104:0", "tick 104"]
    );
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["lost"]);
}

#[tokio::test(start_paused = true)]
async fn a_live_attach_switching_mid_slot_is_covered_from_two_slots_after_its_pong() {
    let mock = MockGeyser::default();
    mock.auto_pong(false);
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let (mut b, _) = join(&pool, section(), 2, 8);
    let mut connection = subscribed(&mock).await;
    connection.send(pong(1));
    lead_in(&connection, &chain, 100);
    connection.send_all(chain.bank(102, &[(0, &[address(1), address(2)])]));
    for attach in [&mut a, &mut b] {
        assert_eq!(
            kinds(&next_batch(attach).await),
            vec!["row 102:0", "tick 102"]
        );
    }
    let (mut c, _) = join(&pool, section(), 3, 8);
    let update = connection.next_request().await;
    assert_eq!(account_include(&update), base58(&[1, 2, 3]));
    assert_eq!((update.ping, update.from_slot), (None, None));
    assert_eq!(update.commitment, Some(CommitmentLevel::Finalized as i32));
    let acknowledgement = connection.next_request().await;
    let ack = ping_id(&acknowledgement).expect("a ping-only request");
    assert_eq!(acknowledgement, ping_request(ack));
    connection.send(chain.tx(103, 0, &[address(1), address(3)], 1));
    connection.send(pong(ack));
    connection.send_all([
        chain.tx(103, 1, &[address(3), address(2)], 1),
        chain.meta(103),
    ]);
    for slot in 104..=105 {
        connection.send_all(chain.bank(
            slot,
            &[(0, &[address(1)]), (1, &[address(2)]), (2, &[address(3)])],
        ));
    }
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 103:0", "tick 103"]
    );
    assert_eq!(
        kinds(&next_batch(&mut b).await),
        vec!["row 103:1", "tick 103"]
    );
    for slot in 104..=105 {
        assert_eq!(
            kinds(&next_batch(&mut a).await),
            vec![format!("row {slot}:0"), format!("tick {slot}")]
        );
        assert_eq!(
            kinds(&next_batch(&mut b).await),
            vec![format!("row {slot}:1"), format!("tick {slot}")]
        );
    }
    assert_eq!(
        kinds(&next_batch(&mut c).await),
        vec!["row 105:2", "tick 105"]
    );
    assert!(c.try_recv().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_rejected_update_fails_only_the_attaching_worker_and_the_rest_reconnect_with_the_last_good_filter()
 {
    let mock = MockGeyser::default();
    mock.auto_pong(false);
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let (mut b, _) = join(&pool, section(), 2, 8);
    let mut connection = subscribed(&mock).await;
    connection.send(pong(1));
    lead_in(&connection, &chain, 100);
    connection.send_all(chain.bank(102, &[(0, &[address(1), address(2)])]));
    for attach in [&mut a, &mut b] {
        assert_eq!(
            kinds(&next_batch(attach).await),
            vec!["row 102:0", "tick 102"]
        );
    }
    let (d, _) = join(&pool, section(), 4, 8);
    let update = connection.next_request().await;
    assert_eq!(account_include(&update), base58(&[1, 2, 4]));
    let acknowledgement = connection.next_request().await;
    assert!(ping_id(&acknowledgement).is_some());
    connection.fail(Status::invalid_argument(
        "account_include names a closed account",
    ));
    let failed_at = Instant::now();
    let reason = rejected(d.finish().await);
    assert!(reason.contains("InvalidArgument"), "{reason}");
    assert!(reason.starts_with("filter update rejected: "), "{reason}");
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["lost"]);
    assert_eq!(kinds(&next_batch(&mut b).await), vec!["lost"]);
    let reconnected = subscribed(&mock).await;
    assert_eq!(dial.dials()[1] - failed_at, Duration::from_secs(1));
    assert_eq!(
        account_include(reconnected.first_request()),
        base58(&[1, 2])
    );
    assert_eq!(reconnected.first_request(), connection.first_request());
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["resumed"]);
    assert_eq!(kinds(&next_batch(&mut b).await), vec!["resumed"]);
    reconnected.send(pong(1));
    lead_in(&reconnected, &chain, 120);
    reconnected.send_all(chain.bank(122, &[(0, &[address(1), address(2)])]));
    let expected = vec!["gap 102->122 lost 19", "row 122:0", "tick 122"];
    assert_eq!(kinds(&next_batch(&mut a).await), expected);
    assert_eq!(kinds(&next_batch(&mut b).await), expected);
}

#[tokio::test(start_paused = true)]
async fn client_pings_get_pongs_that_stamp_the_row_round_trip() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let (mut b, _) = join(&pool, section(), 2, 8);
    let mut connection = subscribed(&mock).await;
    let started = Instant::now();
    lead_in(&connection, &chain, 100);
    let mut seqs = Vec::new();
    for (slot, at) in [(102, 0), (103, 5), (104, 10), (105, 16)] {
        sleep_until(started + Duration::from_secs(at)).await;
        connection.send_all(chain.bank(slot, &[(0, &[address(1), address(2)])]));
        let first = next_batch(&mut a).await;
        let second = next_batch(&mut b).await;
        let (left, right) = (rows(&first), rows(&second));
        assert_eq!((left.len(), right.len()), (1, 1));
        assert_eq!(left[0].recv_seq, right[0].recv_seq);
        assert_eq!(left[0].conn_epoch_us, right[0].conn_epoch_us);
        assert!(left[0].source_tx_rtt_us >= 1, "{:?}", left[0]);
        assert_eq!(left[0].source_tx_rtt_us, right[0].source_tx_rtt_us);
        seqs.push(left[0].recv_seq);
    }
    assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]), "{seqs:?}");
    assert_eq!(connection.next_request().await, ping_request(2));
}

#[tokio::test(start_paused = true)]
async fn a_missing_pong_never_reconnects_and_coverage_opens_after_the_grace() {
    let mock = MockGeyser::default();
    mock.auto_pong(false);
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let connection = subscribed(&mock).await;
    let started = Instant::now();
    let mut first = None;
    let mut highest_before_grace = 0;
    for index in 0..=60u64 {
        let at = 1 + 2 * index;
        sleep_until(started + Duration::from_secs(at)).await;
        let slot = 100 + index;
        connection.send(chain.meta(slot));
        if at < 30 {
            highest_before_grace = slot;
        }
        sleep(Duration::from_millis(10)).await;
        if first.is_none()
            && let Some(batch) = a.try_recv()
        {
            first = Some((started.elapsed(), kinds(&batch)));
        }
    }
    let (arrived, batch) = first.expect("coverage opened");
    assert_eq!(batch, vec![format!("tick {}", highest_before_grace + 2)]);
    assert!(arrived > Duration::from_secs(30), "{arrived:?}");
    assert!(arrived < Duration::from_secs(36), "{arrived:?}");
    assert_eq!(mock.subscribes().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn server_pings_are_answered_with_a_ping_only_request() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let (_a, _) = join(&pool, section(), 1, 8);
    let mut connection = subscribed(&mock).await;
    connection.send(server_ping());
    let reply = connection.next_request().await;
    assert_eq!(reply, ping_request(2));
    assert!(reply.transactions.is_empty() && reply.blocks_meta.is_empty());
    assert_eq!((reply.commitment, reply.from_slot), (None, None));
    connection.send(server_ping());
    assert_eq!(connection.next_request().await, ping_request(3));
}

#[tokio::test(start_paused = true)]
async fn unauthenticated_after_data_is_terminal_and_never_retried() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let connection = subscribed(&mock).await;
    lead_in(&connection, &chain, 100);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 102:0", "tick 102"]
    );
    connection.fail(Status::unauthenticated("invalid x-token"));
    let mut after = Vec::new();
    while let Some(batch) = a.recv().await {
        after.extend(kinds(&batch));
    }
    assert!(after.is_empty(), "{after:?}");
    let reason = rejected(a.finish().await);
    for part in [
        "Unauthenticated",
        "invalid x-token",
        "yellowstone-grpc-proto@14.0.1",
        "mock-geyser 0.0.0",
        "x_token_env",
    ] {
        assert!(reason.contains(part), "{reason}");
    }
    sleep(Duration::from_secs(120)).await;
    assert_eq!(mock.subscribes().len(), 1);
    assert_eq!(dial.dials().len(), 1);
    assert!(pool.connections().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_oversized_message_is_terminal() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let small = GrpcSection {
        max_decoding_message_size_mib: 1,
        ..section()
    };
    let (mut a, _) = join(&pool, small, 1, 8);
    let connection = subscribed(&mock).await;
    connection.send(Chain::oversized(2 << 20));
    assert!(a.recv().await.is_none());
    let reason = rejected(a.finish().await);
    assert!(reason.contains("OutOfRange"), "{reason}");
    assert!(
        reason.contains("decoded message length too large"),
        "{reason}"
    );
    assert!(reason.contains("max_decoding_message_size_mib"), "{reason}");
    sleep(Duration::from_secs(120)).await;
    assert_eq!(dial.dials().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_slow_attach_overflows_alone_and_resumes_with_an_address_scoped_gap() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, fast) = join(&pool, section(), 1, 64);
    let (mut b, slow) = join(&pool, section(), 2, 4);
    let connection = subscribed(&mock).await;
    lead_in(&connection, &chain, 100);
    for slot in 102..=109 {
        connection.send_all(chain.bank(slot, &[(0, &[address(1), address(2)])]));
    }
    for slot in 102..=109 {
        assert_eq!(
            kinds(&next_batch(&mut a).await),
            vec![format!("row {slot}:0"), format!("tick {slot}")]
        );
    }
    for slot in 102..=105 {
        assert_eq!(
            kinds(&next_batch(&mut b).await),
            vec![format!("row {slot}:0"), format!("tick {slot}")]
        );
    }
    assert!(b.try_recv().is_none());
    connection.send_all(chain.bank(110, &[(0, &[address(1), address(2)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 110:0", "tick 110"]
    );
    let resumed = next_batch(&mut b).await;
    assert_eq!(
        kinds(&resumed),
        vec![
            "lost",
            "resumed",
            "gap 105->110 lost 4",
            "row 110:0",
            "tick 110"
        ]
    );
    assert!(matches!(
        &resumed[0],
        DomainEvent::ConnectionLost { reason } if reason == "fan-out queue full"
    ));
    assert_eq!(slow.snapshot().fanout_overflow, 1);
    assert_eq!(fast.snapshot().fanout_overflow, 0);
    assert_eq!(mock.subscribes().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_child_bank_sealed_before_its_parent_reaches_every_attach_in_height_order() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let (mut b, _) = join(&pool, section(), 2, 8);
    let mut connection = subscribed(&mock).await;
    lead_in(&connection, &chain, 100);
    connection.send_all(chain.bank(102, &[(0, &[address(1), address(2)])]));
    for attach in [&mut a, &mut b] {
        assert_eq!(
            kinds(&next_batch(attach).await),
            vec!["row 102:0", "tick 102"]
        );
    }
    connection.send_all(chain.bank(104, &[(0, &[address(1), address(2)])]));
    settle(&mut connection).await;
    assert!(a.try_recv().is_none() && b.try_recv().is_none());
    connection.send_all(chain.bank(103, &[(1, &[address(1), address(2)])]));
    for attach in [&mut a, &mut b] {
        assert_eq!(
            kinds(&next_batch(attach).await),
            vec!["row 103:1", "tick 103"]
        );
        assert_eq!(
            kinds(&next_batch(attach).await),
            vec!["row 104:0", "tick 104"]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_bank_unsealed_beyond_max_pending_slots_is_abandoned_and_shows_as_one_gap() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let short = GrpcSection {
        max_pending_slots: 4,
        ..section()
    };
    let (mut a, metrics) = join(&pool, short, 1, 16);
    let mut connection = subscribed(&mock).await;
    lead_in(&connection, &chain, 100);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 102:0", "tick 102"]
    );
    connection.send(chain.tx(103, 0, &[address(1)], 1));
    for slot in 104..=107 {
        connection.send(chain.meta(slot));
    }
    settle(&mut connection).await;
    assert!(a.try_recv().is_none());
    assert_eq!(metrics.snapshot().abandoned_banks, 0);
    connection.send(chain.meta(108));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["gap 102->104 lost 1", "tick 104"]
    );
    for slot in 105..=108 {
        assert_eq!(
            kinds(&next_batch(&mut a).await),
            vec![format!("tick {slot}")]
        );
    }
    assert_eq!(metrics.snapshot().abandoned_banks, 1);
    connection.send(chain.meta(103));
    connection.send_all(chain.bank(109, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 109:0", "tick 109"]
    );
}

#[tokio::test(start_paused = true)]
async fn attaches_beyond_the_shard_size_open_a_second_connection_and_stay_on_it() {
    let mock = MockGeyser::default();
    let (pool, dial) = duplex_pool(&mock);
    let pair = GrpcSection {
        max_addresses_per_connection: 2,
        ..section()
    };
    let (a, _) = join(&pool, pair.clone(), 1, 8);
    let (b, _) = join(&pool, pair.clone(), 2, 8);
    let (c, _) = join(&pool, pair.clone(), 3, 8);
    assert_eq!((a.key().shard, b.key().shard, c.key().shard), (0, 0, 1));
    let mut connections = vec![subscribed(&mock).await, subscribed(&mock).await];
    connections.sort_by_key(|connection| {
        std::cmp::Reverse(account_include(connection.first_request()).len())
    });
    let Ok([mut first, mut second]) = <[MockConnection; 2]>::try_from(connections) else {
        panic!("two connections");
    };
    assert_eq!(account_include(first.first_request()), base58(&[1, 2]));
    assert_eq!(account_include(second.first_request()), base58(&[3]));
    assert_eq!(
        pool.connections()
            .iter()
            .map(|(key, attaches)| (key.shard, *attaches))
            .collect::<Vec<_>>(),
        vec![(0, 2), (1, 1)]
    );
    drop(b);
    let (d, _) = join(&pool, pair.clone(), 4, 8);
    let (again, _) = join(&pool, pair.clone(), 3, 8);
    assert_eq!((d.key().shard, again.key().shard), (0, 1));
    loop {
        let request = first.next_request().await;
        if account_include(&request) == base58(&[1, 4]) {
            break;
        }
    }
    assert_eq!(account_include(&second.next_request().await), base58(&[3]));
    assert_eq!(dial.dials().len(), 2);
    assert_eq!(mock.subscribes().len(), 2);
    drop(c);
    assert_eq!(
        pool.connections()
            .iter()
            .map(|(key, attaches)| (key.shard, *attaches))
            .collect::<Vec<_>>(),
        vec![(0, 2), (1, 1)]
    );
}

#[tokio::test(start_paused = true)]
async fn the_last_detach_closes_the_connection() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let (a, first_metrics) = join(&pool, section(), 1, 8);
    let mut connection = subscribed(&mock).await;
    let epoch = first_metrics.conn_epoch_us();
    assert!(epoch > 0);
    drop(a);
    connection.closed().await;
    assert!(pool.connections().is_empty());
    let (_b, second_metrics) = join(&pool, section(), 1, 8);
    let reopened = subscribed(&mock).await;
    assert!(second_metrics.conn_epoch_us() > epoch);
    assert_eq!(reopened.first_request(), connection.first_request());
    assert_eq!(mock.subscribes().len(), 2);
    assert_eq!(pool.connections().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn an_attach_queued_behind_the_last_detach_joins_the_open_subscription() {
    let mock = MockGeyser::default();
    mock.auto_pong(false);
    let (pool, dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, _) = join(&pool, section(), 1, 8);
    let mut connection = subscribed(&mock).await;
    connection.send(pong(1));
    lead_in(&connection, &chain, 100);
    connection.send_all(chain.bank(102, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 102:0", "tick 102"]
    );
    let (refused, _) = join(&pool, section(), 2, 8);
    assert_eq!(
        account_include(&connection.next_request().await),
        base58(&[1, 2])
    );
    assert!(ping_id(&connection.next_request().await).is_some());
    connection.fail(Status::invalid_argument(
        "account_include names a closed account",
    ));
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["lost"]);
    let mut reconnected = subscribed(&mock).await;
    assert_eq!(account_include(reconnected.first_request()), base58(&[1]));
    assert_eq!(kinds(&next_batch(&mut a).await), vec!["resumed"]);
    reconnected.send(pong(1));
    lead_in(&reconnected, &chain, 120);
    reconnected.send_all(chain.bank(122, &[(0, &[address(1)])]));
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["gap 102->122 lost 19", "row 122:0", "tick 122"]
    );
    drop(a);
    let (mut late, _) = join(&pool, section(), 3, 8);
    let update = reconnected.next_request().await;
    assert_eq!(account_include(&update), base58(&[3]));
    assert_eq!((update.ping, update.from_slot), (None, None));
    let ack = ping_id(&reconnected.next_request().await).expect("a ping-only request");
    reconnected.send(pong(ack));
    reconnected.send_all(chain.bank(123, &[(0, &[address(3)])]));
    reconnected.send_all(chain.bank(124, &[(0, &[address(3)])]));
    assert_eq!(
        kinds(&next_batch(&mut late).await),
        vec!["row 124:0", "tick 124"]
    );
    assert_eq!(dial.dials().len(), 2);
    assert_eq!(mock.subscribes().len(), 2);
    assert_eq!(pool.connections()[0].1, 2);
    let reason = rejected(refused.finish().await);
    assert!(reason.starts_with("filter update rejected: "), "{reason}");
    assert_eq!(pool.connections()[0].1, 1);
}

#[tokio::test(start_paused = true)]
async fn every_attach_of_one_physical_connection_carries_its_conn_epoch_and_recv_seq() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, first_metrics) = join(&pool, section(), 1, 8);
    let (mut b, second_metrics) = join(&pool, section(), 2, 8);
    let connection = subscribed(&mock).await;
    let epoch = first_metrics.conn_epoch_us();
    assert_eq!(second_metrics.conn_epoch_us(), epoch);
    lead_in(&connection, &chain, 100);
    connection
        .send_all(chain.bank(102, &[(0, &[address(1), address(2)]), (1, &[address(2)])]));
    let (left, right) = (next_batch(&mut a).await, next_batch(&mut b).await);
    let (left, right) = (rows(&left), rows(&right));
    assert_eq!((left.len(), right.len()), (1, 2));
    assert_eq!(left[0].conn_epoch_us, epoch);
    assert_eq!(
        (left[0].recv_seq, left[0].conn_epoch_us),
        (right[0].recv_seq, right[0].conn_epoch_us)
    );
    assert!(right[1].recv_seq > right[0].recv_seq);
    let before = right[1].recv_seq;
    connection.reset();
    for attach in [&mut a, &mut b] {
        assert_eq!(kinds(&next_batch(attach).await), vec!["lost"]);
    }
    let reconnected = subscribed(&mock).await;
    for attach in [&mut a, &mut b] {
        assert_eq!(kinds(&next_batch(attach).await), vec!["resumed"]);
    }
    let renewed = first_metrics.conn_epoch_us();
    assert!(renewed > epoch);
    assert_eq!(second_metrics.conn_epoch_us(), renewed);
    lead_in(&reconnected, &chain, 120);
    reconnected.send_all(chain.bank(122, &[(0, &[address(1), address(2)])]));
    let (left, right) = (next_batch(&mut a).await, next_batch(&mut b).await);
    assert_eq!(
        kinds(&left),
        vec!["gap 102->122 lost 19", "row 122:0", "tick 122"]
    );
    let (left, right) = (rows(&left), rows(&right));
    assert_eq!((left.len(), right.len()), (1, 1));
    assert_eq!(left[0].conn_epoch_us, renewed);
    assert_eq!(right[0].conn_epoch_us, renewed);
    assert_eq!(left[0].recv_seq, right[0].recv_seq);
    assert!(left[0].recv_seq < before, "{} {before}", left[0].recv_seq);
}

#[tokio::test(start_paused = true)]
async fn connection_counters_are_reported_on_the_oldest_attach_only() {
    let mock = MockGeyser::default();
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, oldest) = join(&pool, section(), 1, 16);
    let (mut b, younger) = join(&pool, section(), 2, 16);
    let connection = subscribed(&mock).await;
    lead_in(&connection, &chain, 100);
    connection.send_all([
        chain.tx(102, 0, &[address(1)], 1),
        chain.tx(102, 1, &[address(9)], 1),
        chain.vote(102, 2, &[address(2)]),
        SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot::default())),
            created_at: None,
        },
        chain.meta(102),
        chain.partial_meta(103),
    ]);
    for slot in 104..=108 {
        connection.send(chain.meta(slot));
    }
    assert_eq!(
        kinds(&next_batch(&mut a).await),
        vec!["row 102:0", "tick 102"]
    );
    assert_eq!(kinds(&next_batch(&mut b).await), vec!["tick 102"]);
    for attach in [&mut a, &mut b] {
        assert_eq!(
            kinds(&next_batch(attach).await),
            vec!["gap 102->104 lost 1", "tick 104"]
        );
        for slot in 105..=108 {
            assert_eq!(
                kinds(&next_batch(attach).await),
                vec![format!("tick {slot}")]
            );
        }
    }
    let counted = oldest.snapshot();
    assert_eq!(
        (
            counted.filtered_out,
            counted.votes_excluded,
            counted.partial_banks_discarded,
            counted.dropped_frames,
            counted.msgs,
        ),
        (1, 1, 1, 1, 12)
    );
    assert_eq!((counted.slot_gaps, counted.blocks_lost), (0, 0));
    assert_eq!(younger.snapshot(), Default::default());
    drop(a);
    connection.send(chain.tx(109, 0, &[address(9)], 1));
    connection.send_all(chain.bank(109, &[(1, &[address(2)])]));
    assert_eq!(
        kinds(&next_batch(&mut b).await),
        vec!["row 109:1", "tick 109"]
    );
    let moved = younger.snapshot();
    assert_eq!((moved.filtered_out, moved.msgs), (1, 3));
    assert_eq!(oldest.snapshot(), counted);
}

#[tokio::test(start_paused = true)]
async fn behind_tip_slots_follows_get_slot_at_the_commitment() {
    let mock = MockGeyser::default();
    mock.set_tip(CommitmentLevel::Finalized, 150);
    mock.set_tip(CommitmentLevel::Confirmed, 400);
    let (pool, _dial) = duplex_pool(&mock);
    let chain = Chain::recent();
    let (mut a, metrics) = join(&pool, section(), 1, 8);
    let connection = subscribed(&mock).await;
    let started = Instant::now();
    lead_in(&connection, &chain, 100);
    for (slot, at) in [(102, 0), (103, 5), (104, 10)] {
        sleep_until(started + Duration::from_secs(at)).await;
        connection.send_all(chain.bank(slot, &[(0, &[address(1)])]));
        assert_eq!(
            kinds(&next_batch(&mut a).await),
            vec![format!("row {slot}:0"), format!("tick {slot}")]
        );
    }
    assert_eq!(metrics.snapshot().behind_tip_slots, 0);
    sleep_until(started + Duration::from_secs(16)).await;
    assert_eq!(metrics.snapshot().behind_tip_slots, 150 - 104);
    mock.set_tip(CommitmentLevel::Finalized, 160);
    for (slot, at) in [(105, 18), (106, 26)] {
        sleep_until(started + Duration::from_secs(at)).await;
        connection.send_all(chain.bank(slot, &[(0, &[address(1)])]));
        assert_eq!(
            kinds(&next_batch(&mut a).await),
            vec![format!("row {slot}:0"), format!("tick {slot}")]
        );
    }
    assert_eq!(metrics.snapshot().behind_tip_slots, 150 - 104);
    sleep_until(started + Duration::from_secs(31)).await;
    assert_eq!(metrics.snapshot().behind_tip_slots, 160 - 106);
    assert_eq!(mock.subscribes().len(), 1);
}
