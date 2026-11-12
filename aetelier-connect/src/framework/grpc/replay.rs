use aetelier_types::config::markets::market_config::DataTypesSection;
use aetelier_types::onchain::{Chain, ChainAddress, Commitment};
use yellowstone_grpc_proto::prelude::{SubscribeUpdate, subscribe_update::UpdateOneof};
use yellowstone_grpc_proto::prost::Message as _;

use super::assemble::{SlotAssembler, bank_events};
use super::decode::decode_update;
use crate::errors::ExchangeError;
use crate::framework::budget::SourceMetrics;
use crate::framework::model::DomainEvent;
use crate::framework::onchain::{
    DecodedTransaction, ReceiptStamp, RowContext, WatchedAddress,
};
use crate::framework::registry::ReplayFrame;
use crate::framework::slotbook::{CoverageStart, ReleasedBank, SlotBookConfig};

pub fn replay_frames(
    symbols: &[String],
    datatypes: &DataTypesSection,
    commitment: Commitment,
    frames: &[ReplayFrame],
    metrics: &SourceMetrics,
) -> Result<Vec<DomainEvent>, Box<ExchangeError>> {
    let watches = symbols
        .iter()
        .map(|symbol| {
            ChainAddress::parse(Chain::Solana, symbol)
                .map(|address| WatchedAddress::new(address, &datatypes.transactions))
                .map_err(|_| {
                    Box::new(ExchangeError::Unsupported(format!(
                        "cannot decode symbol {symbol} for solana"
                    )))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let watched: Vec<&WatchedAddress> = watches.iter().collect();
    let counters = [metrics];
    let context = RowContext {
        exchange: "solana",
        commitment,
    };
    let mut assembler = SlotAssembler::new(SlotBookConfig::default());
    let mut segment = Segment::default();
    let mut events = Vec::new();
    assembler.open(CoverageStart::AfterHighestSlot);
    for frame in frames {
        match frame {
            ReplayFrame::Update { t_us, bytes } => {
                let update = SubscribeUpdate::decode(bytes.as_slice())
                    .map_err(|error| Box::new(ExchangeError::IoError(error.into())))?;
                if matches!(
                    update.update_oneof,
                    Some(UpdateOneof::Ping(_) | UpdateOneof::Pong(_))
                ) {
                    continue;
                }
                let receipt = segment.receipt(*t_us);
                let released =
                    assembler.apply(decode_update(update, receipt), &watched, &counters);
                release(&mut events, &released, &watched, context);
            }
            ReplayFrame::Reconnect { t_us } => {
                release(&mut events, &assembler.close(&counters), &watched, context);
                events.push(DomainEvent::ConnectionLost {
                    reason: format!("reconnect marker at {t_us}"),
                });
                assembler.open(CoverageStart::AfterHighestSlot);
                events.push(DomainEvent::ConnectionResumed);
                segment = Segment::default();
            }
        }
    }
    release(&mut events, &assembler.close(&counters), &watched, context);
    Ok(events)
}

#[derive(Default)]
struct Segment {
    conn_epoch_us: Option<u64>,
    recv_seq: u64,
}

impl Segment {
    fn receipt(&mut self, t_us: u64) -> ReceiptStamp {
        self.recv_seq += 1;
        ReceiptStamp {
            local_tx_ts_us: t_us,
            source_tx_rtt_us: 0,
            recv_seq: self.recv_seq,
            conn_epoch_us: *self.conn_epoch_us.get_or_insert(t_us),
        }
    }
}

fn release(
    events: &mut Vec<DomainEvent>,
    banks: &[ReleasedBank<DecodedTransaction>],
    watched: &[&WatchedAddress],
    context: RowContext<'_>,
) {
    for bank in banks {
        events.extend(bank_events(bank, bank.gap, watched, context));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::adapters::solana::SOLANA;
    use crate::framework::grpc::{BLOCKS_META_FILTER, TRANSACTIONS_FILTER};
    use crate::framework::onchain::decoded::fixtures::key;
    use crate::framework::registry::ExchangeAdapter;
    use crate::framework::slotbook::{SlotGap, SlotPosition};
    use aetelier_types::onchain::{MatchRole, Transaction};
    use yellowstone_grpc_proto::prelude::{
        BlockHeight, Message as WireMessage, MessageHeader as WireHeader,
        SubscribeUpdateBlockMeta, SubscribeUpdatePing, SubscribeUpdatePong,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        Transaction as WireTransaction, TransactionError as WireError,
        TransactionStatusMeta as WireMeta, UnixTimestamp,
    };

    const BASE_SLOT: u64 = 100;
    const HEIGHT_OFFSET: u64 = 1_000;
    const T0_SECS: i64 = 1_791_321_000;
    const T_US: u64 = 1_791_321_067_000_000;
    const FRAME_US: u64 = 1_000;
    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

    fn height(slot: u64) -> u64 {
        slot + HEIGHT_OFFSET
    }

    fn block_time_secs(slot: u64) -> i64 {
        T0_SECS + slot as i64 - BASE_SLOT as i64
    }

    fn position(slot: u64) -> SlotPosition {
        SlotPosition {
            slot,
            block_height: height(slot),
            block_time_us: block_time_secs(slot) as u64 * 1_000_000,
        }
    }

    fn info(
        slot: u64,
        index: u64,
        keys: &[ChainAddress],
    ) -> SubscribeUpdateTransactionInfo {
        let mut signature = vec![7u8; 64];
        signature[..8].copy_from_slice(&slot.to_le_bytes());
        signature[8..16].copy_from_slice(&index.to_le_bytes());
        SubscribeUpdateTransactionInfo {
            signature: signature.clone(),
            is_vote: false,
            transaction: Some(WireTransaction {
                signatures: vec![signature],
                message: Some(WireMessage {
                    header: Some(WireHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    }),
                    account_keys: keys.iter().map(|key| key.pubkey().to_vec()).collect(),
                    recent_blockhash: vec![9u8; 32],
                    instructions: Vec::new(),
                    versioned: false,
                    address_table_lookups: Vec::new(),
                    config: None,
                }),
            }),
            meta: Some(WireMeta {
                fee: 5_000,
                log_messages_none: true,
                ..Default::default()
            }),
            index,
        }
    }

    fn transaction(slot: u64, info: SubscribeUpdateTransactionInfo) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![TRANSACTIONS_FILTER.to_string()],
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                transaction: Some(info),
                slot,
                bank_id: 0,
            })),
            created_at: None,
        }
    }

    fn tx(slot: u64, index: u64, keys: &[ChainAddress]) -> SubscribeUpdate {
        transaction(slot, info(slot, index, keys))
    }

    fn meta(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![BLOCKS_META_FILTER.to_string()],
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                blockhash: format!("blockhash{slot}"),
                block_time: Some(UnixTimestamp {
                    timestamp: block_time_secs(slot),
                }),
                block_height: Some(BlockHeight {
                    block_height: height(slot),
                }),
                parent_slot: slot - 1,
                ..Default::default()
            })),
            created_at: None,
        }
    }

    fn control(update_oneof: Option<UpdateOneof>) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: Vec::new(),
            update_oneof,
            created_at: None,
        }
    }

    fn ping() -> SubscribeUpdate {
        control(Some(UpdateOneof::Ping(SubscribeUpdatePing {})))
    }

    fn pong(id: i32) -> SubscribeUpdate {
        control(Some(UpdateOneof::Pong(SubscribeUpdatePong { id })))
    }

    fn frame(t_us: u64, update: SubscribeUpdate) -> ReplayFrame {
        ReplayFrame::Update {
            t_us,
            bytes: update.encode_to_vec(),
        }
    }

    #[derive(Default)]
    struct Capture {
        frames: Vec<ReplayFrame>,
    }

    impl Capture {
        fn next_t_us(&self) -> u64 {
            T_US + FRAME_US * self.frames.len() as u64
        }

        fn send(mut self, updates: impl IntoIterator<Item = SubscribeUpdate>) -> Self {
            for update in updates {
                let t_us = self.next_t_us();
                self.frames.push(frame(t_us, update));
            }
            self
        }

        fn reconnect(mut self) -> Self {
            let t_us = self.next_t_us();
            self.frames.push(ReplayFrame::Reconnect { t_us });
            self
        }
    }

    fn datatypes() -> DataTypesSection {
        let mut datatypes = DataTypesSection::default();
        datatypes.transactions.enabled = true;
        datatypes
    }

    fn symbols(seeds: &[u8]) -> Vec<String> {
        seeds.iter().map(|&seed| key(seed).to_base58()).collect()
    }

    fn replay(seeds: &[u8], frames: &[ReplayFrame]) -> (Vec<DomainEvent>, SourceMetrics) {
        let metrics = SourceMetrics::default();
        let events = replay_frames(
            &symbols(seeds),
            &datatypes(),
            Commitment::Finalized,
            frames,
            &metrics,
        )
        .expect("the frames replay");
        (events, metrics)
    }

    fn render(events: &[DomainEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| match event {
                DomainEvent::ConnectionLost { .. } => "lost".to_string(),
                DomainEvent::ConnectionResumed => "resumed".to_string(),
                DomainEvent::SlotGap(gap) => format!(
                    "gap {}->{} lost {}",
                    gap.prev_slot,
                    gap.next_slot,
                    gap.blocks_lost()
                ),
                DomainEvent::Transaction(row) => {
                    format!("row {}:{}", row.slot, row.tx_index)
                }
                DomainEvent::SlotTick(tick) => format!("tick {}", tick.slot),
                other => format!("{other:?}"),
            })
            .collect()
    }

    fn rows(events: &[DomainEvent]) -> Vec<&Transaction> {
        events
            .iter()
            .filter_map(|event| match event {
                DomainEvent::Transaction(row) => Some(row.as_ref()),
                _ => None,
            })
            .collect()
    }

    fn refusal(symbols: &[String], frames: &[ReplayFrame]) -> ExchangeError {
        let metrics = SourceMetrics::default();
        let refused = replay_frames(
            symbols,
            &datatypes(),
            Commitment::Finalized,
            frames,
            &metrics,
        )
        .expect_err("the replay is refused");
        *refused
    }

    #[test]
    fn replay_releases_banks_as_rows_and_ticks_from_two_slots_after_the_first_frame() {
        let watched = [key(1)];
        let capture = Capture::default().send([
            tx(100, 0, &watched),
            meta(100),
            meta(101),
            tx(102, 0, &watched),
            tx(102, 1, &watched),
            meta(102),
            tx(103, 0, &watched),
            meta(103),
        ]);
        let (events, _) = replay(&[1], &capture.frames);
        assert_eq!(
            render(&events),
            [
                "row 102:0",
                "row 102:1",
                "tick 102",
                "row 103:0",
                "tick 103"
            ]
        );
        for row in rows(&events) {
            assert_eq!(row.check_invariants(), Ok(()));
            assert_eq!(row.exchange, "solana");
            assert_eq!(row.commitment, Commitment::Finalized);
            assert_eq!(row.address, key(1));
            assert_eq!(row.source_tx_ts_us, position(row.slot).block_time_us);
        }
        assert!(matches!(
            events[2],
            DomainEvent::SlotTick(tick) if tick.block_height == height(102)
        ));
        let held = Capture::default().send([
            meta(100),
            meta(101),
            tx(103, 0, &watched),
            meta(103),
        ]);
        let (events, _) = replay(&[1], &held.frames);
        assert_eq!(render(&events), ["row 103:0", "tick 103"]);
        let (events, _) = replay(&[1], &[]);
        assert!(events.is_empty());
    }

    #[test]
    fn a_reconnect_marker_flushes_held_banks_then_reports_loss_resume_and_one_exact_gap()
    {
        let watched = [key(1)];
        let capture = Capture::default()
            .send([meta(100), meta(101), tx(103, 0, &watched), meta(103)])
            .reconnect()
            .send([meta(110), meta(111), tx(112, 0, &watched), meta(112)]);
        let (events, metrics) = replay(&[1], &capture.frames);
        assert_eq!(
            render(&events),
            [
                "row 103:0",
                "tick 103",
                "lost",
                "resumed",
                "gap 103->112 lost 8",
                "row 112:0",
                "tick 112"
            ]
        );
        let marker_us = T_US + 4 * FRAME_US;
        assert!(matches!(
            &events[2],
            DomainEvent::ConnectionLost { reason }
                if *reason == format!("reconnect marker at {marker_us}")
        ));
        let gaps: Vec<SlotGap> = events
            .iter()
            .filter_map(|event| match event {
                DomainEvent::SlotGap(gap) => Some(*gap),
                _ => None,
            })
            .collect();
        assert_eq!(gaps, [SlotGap::between(position(103), position(112))]);
        let counted = metrics.snapshot();
        assert_eq!((counted.slot_gaps, counted.blocks_lost), (0, 0));
        assert_eq!(counted.abandoned_banks, 0);
    }

    #[test]
    fn replay_stamps_rows_with_frame_receipts_and_segment_epochs() {
        let watched = [key(1)];
        let at = |n: u64| T_US + n * FRAME_US;
        let frames = [
            frame(at(0), pong(1)),
            frame(at(1), meta(100)),
            frame(at(2), meta(101)),
            frame(at(3), tx(102, 0, &watched)),
            frame(at(4), ping()),
            frame(at(5), tx(102, 1, &watched)),
            frame(at(6), meta(102)),
            ReplayFrame::Reconnect { t_us: at(7) },
            frame(at(8), ping()),
            frame(at(9), meta(110)),
            frame(at(10), meta(111)),
            frame(at(11), tx(112, 0, &watched)),
            frame(at(12), meta(112)),
        ];
        let (events, _) = replay(&[1], &frames);
        let stamps: Vec<(u64, u64, u64, u64, u64)> = rows(&events)
            .into_iter()
            .map(|row| {
                (
                    row.slot,
                    row.local_tx_ts_us,
                    row.source_tx_rtt_us,
                    row.recv_seq,
                    row.conn_epoch_us,
                )
            })
            .collect();
        assert_eq!(
            stamps,
            [
                (102, at(3), 0, 3, at(1)),
                (102, at(5), 0, 4, at(1)),
                (112, at(11), 0, 3, at(9)),
            ]
        );
    }

    #[test]
    fn replay_counts_unmatched_votes_undecodable_errors_and_malformed_updates() {
        let watched = key(1);
        let mut vote = info(102, 2, &[watched]);
        vote.is_vote = true;
        let mut failed = info(102, 3, &[watched]);
        if let Some(meta) = failed.meta.as_mut() {
            meta.err = Some(WireError {
                err: vec![200, 0, 0, 0],
            });
        }
        let mut malformed = info(103, 0, &[watched]);
        malformed.meta = None;
        let capture = Capture::default().send([
            meta(100),
            meta(101),
            tx(102, 0, &[watched]),
            tx(102, 1, &[key(9)]),
            transaction(102, vote),
            transaction(102, failed),
            control(None),
            control(Some(UpdateOneof::Slot(SubscribeUpdateSlot::default()))),
            meta(102),
            transaction(103, malformed),
            meta(103),
            tx(104, 0, &[watched]),
            meta(104),
        ]);
        let (events, metrics) = replay(&[1], &capture.frames);
        assert_eq!(
            render(&events),
            [
                "row 102:0",
                "row 102:3",
                "tick 102",
                "gap 102->104 lost 1",
                "row 104:0",
                "tick 104"
            ]
        );
        let rows = rows(&events);
        assert!(!rows[1].success);
        let failure = rows[1].failure.as_ref().expect("a failed row");
        assert_eq!(failure.error, "0xc8000000");
        assert_eq!(rows[1].check_invariants(), Ok(()));
        let counted = metrics.snapshot();
        assert_eq!(
            (
                counted.msgs,
                counted.filtered_out,
                counted.votes_excluded,
                counted.tx_error_decode_failures,
                counted.decode_err,
                counted.dropped_frames,
                counted.partial_banks_discarded,
            ),
            (10, 1, 1, 1, 1, 2, 1)
        );
        assert_eq!((counted.slot_gaps, counted.blocks_lost), (0, 0));
    }

    #[test]
    fn replay_refuses_undecodable_frame_bytes_and_unparseable_symbols() {
        let watched = [key(1)];
        let mut frames = Capture::default()
            .send([meta(100), meta(101), tx(102, 0, &watched), meta(102)])
            .frames;
        frames.insert(
            2,
            ReplayFrame::Update {
                t_us: T_US,
                bytes: vec![0x0a, 0x05, 0x01],
            },
        );
        match refusal(&symbols(&[1]), &frames) {
            ExchangeError::IoError(error) => {
                assert_eq!(error.kind(), std::io::ErrorKind::InvalidData)
            }
            other => panic!("expected an undecodable frame, got {other:?}"),
        }
        let folded = JUPITER_V6.to_lowercase();
        for symbol in ["SOLUSDC", folded.as_str(), "", "JUP6"] {
            let listed = [key(2).to_base58(), symbol.to_string()];
            match refusal(&listed, &[]) {
                ExchangeError::Unsupported(reason) => {
                    assert_eq!(
                        reason,
                        format!("cannot decode symbol {symbol} for solana")
                    )
                }
                other => panic!("expected an unparseable symbol, got {other:?}"),
            }
        }
        let metrics = SourceMetrics::default();
        let refused = replay_frames(
            &["SOLUSDC".to_string()],
            &datatypes(),
            Commitment::Finalized,
            &frames,
            &metrics,
        );
        assert!(refused.is_err());
        assert_eq!(metrics.snapshot(), SourceMetrics::default().snapshot());
    }

    #[test]
    fn two_replays_of_the_same_frames_are_identical() {
        let watched = [key(1), key(2)];
        let capture = Capture::default()
            .send([
                pong(1),
                meta(100),
                meta(101),
                tx(102, 0, &watched),
                tx(102, 1, &[key(9)]),
                meta(102),
                tx(104, 0, &watched[..1]),
                meta(104),
            ])
            .reconnect()
            .send([
                meta(120),
                meta(121),
                tx(122, 0, &watched[1..]),
                ping(),
                meta(122),
            ]);
        let (first, first_metrics) = replay(&[1, 2], &capture.frames);
        let second_metrics = SourceMetrics::default();
        let second = SOLANA
            .replay_stream(
                &symbols(&[1, 2]),
                &datatypes(),
                Commitment::Finalized,
                &capture.frames,
                &second_metrics,
            )
            .expect("the frames replay");
        assert!(!first.is_empty());
        assert_eq!(format!("{first:?}"), format!("{second:?}"));
        assert_eq!(first_metrics.snapshot(), second_metrics.snapshot());
    }

    #[test]
    fn replay_keeps_one_row_per_watched_address() {
        let (a, b) = (key(1), key(2));
        let mut reloaded = info(102, 0, &[a, b]);
        if let Some(meta) = reloaded.meta.as_mut() {
            meta.loaded_readonly_addresses = vec![a.pubkey().to_vec()];
        }
        let capture = Capture::default().send([
            meta(100),
            meta(101),
            transaction(102, reloaded),
            tx(102, 1, &[b]),
            tx(102, 2, &[key(9), a]),
            tx(102, 3, &[key(9)]),
            meta(102),
        ]);
        let (events, metrics) = replay(&[1, 2], &capture.frames);
        let found: Vec<(u64, ChainAddress, MatchRole)> = rows(&events)
            .into_iter()
            .map(|row| (row.tx_index, row.address, row.match_role))
            .collect();
        assert_eq!(
            found,
            [
                (0, a, MatchRole::FeePayer),
                (0, b, MatchRole::AccountKey),
                (1, b, MatchRole::FeePayer),
                (2, a, MatchRole::AccountKey),
            ]
        );
        let shared = rows(&events);
        assert_eq!(shared[0].signature, shared[1].signature);
        assert_ne!(shared[1].signature, shared[2].signature);
        assert_eq!(metrics.snapshot().filtered_out, 1);
    }
}
