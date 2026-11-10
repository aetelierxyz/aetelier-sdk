use super::decode::{GrpcUpdate, TransactionUpdate};
use crate::framework::budget::SourceMetrics;
use crate::framework::model::DomainEvent;
use crate::framework::onchain::{
    DecodedTransaction, RowContext, WatchedAddress, transaction_rows,
};
use crate::framework::slotbook::{
    CoverageStart, ReleasedBank, SlotBookConfig, SlotBookCounters, SlotGap,
    SourcedSlotbook,
};

pub(crate) struct SlotAssembler {
    book: SourcedSlotbook<DecodedTransaction>,
    mirrored: SlotBookCounters,
}

impl SlotAssembler {
    pub(crate) fn new(config: SlotBookConfig) -> Self {
        Self {
            book: SourcedSlotbook::new(config),
            mirrored: SlotBookCounters::default(),
        }
    }

    pub(crate) fn book(&self) -> &SourcedSlotbook<DecodedTransaction> {
        &self.book
    }

    pub(crate) fn open(&mut self, start: CoverageStart) {
        self.book.open_coverage(start);
    }

    pub(crate) fn close(
        &mut self,
        counters: &[&SourceMetrics],
    ) -> Vec<ReleasedBank<DecodedTransaction>> {
        let released = self.book.close_coverage();
        self.mirror(counters);
        released
    }

    pub(crate) fn apply(
        &mut self,
        update: GrpcUpdate,
        watches: &[&WatchedAddress],
        counters: &[&SourceMetrics],
    ) -> Vec<ReleasedBank<DecodedTransaction>> {
        let released = match update {
            GrpcUpdate::Transaction(TransactionUpdate {
                slot,
                bank_id,
                decoded: Ok(tx),
            }) => {
                let undecodable = tx
                    .error
                    .as_ref()
                    .is_some_and(|error| error.decoded.is_none());
                for metrics in counters {
                    metrics.bump_msgs();
                    if undecodable {
                        metrics.bump_tx_error_decode_failures();
                    }
                }
                if watches.iter().any(|watch| watch.matches(&tx).is_some()) {
                    self.book.on_transaction(slot, bank_id, tx.tx_index, tx);
                } else {
                    for metrics in counters {
                        if tx.is_vote {
                            metrics.bump_votes_excluded();
                        } else {
                            metrics.bump_filtered_out();
                        }
                    }
                }
                Vec::new()
            }
            GrpcUpdate::Transaction(TransactionUpdate {
                slot,
                bank_id,
                decoded: Err(_),
            }) => {
                for metrics in counters {
                    metrics.bump_decode_err();
                }
                self.book.on_undecodable(slot, bank_id);
                Vec::new()
            }
            GrpcUpdate::BlockMeta(meta) => {
                for metrics in counters {
                    metrics.bump_msgs();
                }
                self.book.on_block_meta(meta).released
            }
            GrpcUpdate::Ping | GrpcUpdate::Pong { .. } => Vec::new(),
            GrpcUpdate::Unrequested(_) => {
                for metrics in counters {
                    metrics.add_dropped_frames(1);
                }
                Vec::new()
            }
        };
        self.mirror(counters);
        released
    }

    fn mirror(&mut self, counters: &[&SourceMetrics]) {
        let current = self.book.counters();
        let late_txs = current.late_txs.saturating_sub(self.mirrored.late_txs);
        let partial_banks = current
            .partial_banks_discarded
            .saturating_sub(self.mirrored.partial_banks_discarded);
        let abandoned_banks = current
            .abandoned_banks
            .saturating_sub(self.mirrored.abandoned_banks);
        for metrics in counters {
            if late_txs > 0 {
                metrics.add_late_txs(late_txs);
            }
            if partial_banks > 0 {
                metrics.add_partial_banks_discarded(partial_banks);
            }
            if abandoned_banks > 0 {
                metrics.add_abandoned_banks(abandoned_banks);
            }
        }
        self.mirrored = current;
    }
}

pub(crate) fn bank_events(
    bank: &ReleasedBank<DecodedTransaction>,
    gap: Option<SlotGap>,
    watches: &[&WatchedAddress],
    context: RowContext<'_>,
) -> Vec<DomainEvent> {
    let block = bank.block();
    let mut events: Vec<DomainEvent> =
        gap.map(DomainEvent::SlotGap).into_iter().collect();
    for tx in &bank.txs {
        events.extend(
            transaction_rows(block, tx, watches.iter().copied(), context)
                .into_iter()
                .map(|row| DomainEvent::Transaction(Box::new(row))),
        );
    }
    events.push(DomainEvent::SlotTick(bank.tick()));
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::grpc::MalformedTransaction;
    use crate::framework::onchain::ExecutionError;
    use crate::framework::onchain::decoded::fixtures::{decoded, key};
    use crate::framework::slotbook::{BlockMeta, SlotPosition};
    use aetelier_types::onchain::{Commitment, MatchRole};

    const SLOT: u64 = 380_000_123;
    const T0: i64 = 1_791_321_067;

    fn watching(seed: u8) -> WatchedAddress {
        WatchedAddress {
            address: key(seed),
            required_role: MatchRole::AccountKey,
            include_failed: true,
            keep_logs: false,
        }
    }

    fn height(slot: u64) -> u64 {
        slot - 22_000_000
    }

    fn block_time_secs(slot: u64) -> i64 {
        T0 + slot as i64 - SLOT as i64
    }

    fn block_time_us(slot: u64) -> u64 {
        block_time_secs(slot) as u64 * 1_000_000
    }

    fn position(slot: u64) -> SlotPosition {
        SlotPosition {
            slot,
            block_height: height(slot),
            block_time_us: block_time_us(slot),
        }
    }

    fn transaction(slot: u64, tx: DecodedTransaction) -> GrpcUpdate {
        GrpcUpdate::Transaction(TransactionUpdate {
            slot,
            bank_id: 0,
            decoded: Ok(tx),
        })
    }

    fn block(slot: u64) -> BlockMeta {
        BlockMeta {
            slot,
            bank_id: 0,
            parent_slot: slot - 1,
            block_height: Some(height(slot)),
            block_time_secs: Some(block_time_secs(slot)),
            blockhash: format!("hash{slot}"),
        }
    }

    fn meta(slot: u64) -> GrpcUpdate {
        GrpcUpdate::BlockMeta(block(slot))
    }

    fn covered() -> SlotAssembler {
        let mut assembler = SlotAssembler::new(SlotBookConfig::default());
        assembler.open(CoverageStart::AtSlot(SLOT));
        assembler
    }

    fn context() -> RowContext<'static> {
        RowContext {
            exchange: "solana",
            commitment: Commitment::Finalized,
        }
    }

    fn render(events: &[DomainEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| match event {
                DomainEvent::SlotGap(gap) => {
                    format!("gap {}->{}", gap.prev_slot, gap.next_slot)
                }
                DomainEvent::Transaction(row) => {
                    format!("row {}:{} {}", row.slot, row.tx_index, row.address)
                }
                DomainEvent::SlotTick(tick) => format!("tick {}", tick.slot),
                other => format!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn matched_transactions_buffer_and_unmatched_ones_are_counted_never_buffered() {
        let mut assembler = covered();
        let reporter = SourceMetrics::default();
        let totals = SourceMetrics::default();
        let counters = [&reporter, &totals];
        let watch = watching(3);
        let watches = [&watch];
        let mut vote = decoded(1);
        vote.is_vote = true;
        let mut elsewhere = decoded(2);
        elsewhere.static_keys = vec![key(7), key(8)];
        let mut undecodable = decoded(4);
        undecodable.error = Some(ExecutionError {
            raw: vec![200, 0, 0, 0],
            decoded: None,
        });
        for update in [
            transaction(SLOT, decoded(0)),
            transaction(SLOT, vote),
            transaction(SLOT, elsewhere),
            transaction(SLOT, undecodable),
        ] {
            assert!(assembler.apply(update, &watches, &counters).is_empty());
        }
        assert_eq!(assembler.book().pending_banks(), 1);
        let released = assembler.apply(meta(SLOT), &watches, &counters);
        assert_eq!(released.len(), 1);
        let buffered: Vec<u64> = released[0].txs.iter().map(|tx| tx.tx_index).collect();
        assert_eq!(buffered, vec![0, 4]);
        for metrics in counters {
            let counted = metrics.snapshot();
            assert_eq!(
                (
                    counted.msgs,
                    counted.filtered_out,
                    counted.votes_excluded,
                    counted.tx_error_decode_failures,
                    counted.decode_err,
                ),
                (5, 1, 1, 1, 0)
            );
        }
    }

    #[test]
    fn malformed_transactions_poison_their_bank_into_one_exact_gap() {
        let mut assembler = covered();
        let reporter = SourceMetrics::default();
        let totals = SourceMetrics::default();
        let counters = [&reporter, &totals];
        let watch = watching(3);
        let watches = [&watch];
        assert_eq!(assembler.apply(meta(SLOT), &watches, &counters).len(), 1);
        assembler.apply(transaction(SLOT + 1, decoded(0)), &watches, &counters);
        let malformed = GrpcUpdate::Transaction(TransactionUpdate {
            slot: SLOT + 1,
            bank_id: 0,
            decoded: Err(MalformedTransaction::Missing("transaction.meta")),
        });
        assert!(assembler.apply(malformed, &watches, &counters).is_empty());
        assert!(
            assembler
                .apply(meta(SLOT + 1), &watches, &counters)
                .is_empty()
        );
        let mut released = Vec::new();
        for slot in SLOT + 2..=SLOT + 6 {
            released.extend(assembler.apply(meta(slot), &watches, &counters));
        }
        let slots: Vec<u64> = released.iter().map(|bank| bank.slot).collect();
        assert_eq!(slots, (SLOT + 2..=SLOT + 6).collect::<Vec<_>>());
        let gaps: Vec<SlotGap> = released.iter().filter_map(|bank| bank.gap).collect();
        assert_eq!(
            gaps,
            vec![SlotGap::between(position(SLOT), position(SLOT + 2))]
        );
        assert_eq!(gaps[0].blocks_lost(), 1);
        assert_eq!(assembler.book().counters().slot_gaps, 1);
        for metrics in counters {
            let counted = metrics.snapshot();
            assert_eq!(
                (
                    counted.decode_err,
                    counted.partial_banks_discarded,
                    counted.msgs
                ),
                (1, 1, 8)
            );
            assert_eq!((counted.slot_gaps, counted.blocks_lost), (0, 0));
        }
    }

    #[test]
    fn block_metas_release_banks_and_mirror_book_counters_once() {
        let mut assembler = covered();
        let reporter = SourceMetrics::default();
        let totals = SourceMetrics::default();
        let counters = [&reporter, &totals];
        let watch = watching(3);
        let watches = [&watch];
        assembler.apply(transaction(SLOT, decoded(0)), &watches, &counters);
        let released = assembler.apply(meta(SLOT), &watches, &counters);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].txs.len(), 1);
        assembler.apply(transaction(SLOT, decoded(1)), &watches, &counters);
        let timeless = BlockMeta {
            block_time_secs: None,
            ..block(SLOT + 1)
        };
        assert!(
            assembler
                .apply(GrpcUpdate::BlockMeta(timeless), &watches, &counters)
                .is_empty()
        );
        assembler.apply(transaction(SLOT + 3, decoded(2)), &watches, &counters);
        for update in [
            GrpcUpdate::Ping,
            GrpcUpdate::Pong { id: 4 },
            transaction(SLOT + 3, decoded(5)),
        ] {
            assembler.apply(update, &watches, &counters);
        }
        assert!(assembler.close(&counters).is_empty());
        assert!(assembler.close(&counters).is_empty());
        let book = assembler.book().counters();
        assert_eq!(
            (
                book.late_txs,
                book.partial_banks_discarded,
                book.abandoned_banks
            ),
            (1, 1, 1)
        );
        for metrics in counters {
            let counted = metrics.snapshot();
            assert_eq!(
                (
                    counted.late_txs,
                    counted.partial_banks_discarded,
                    counted.abandoned_banks,
                    counted.msgs,
                ),
                (1, 1, 1, 6)
            );
        }
    }

    #[test]
    fn bank_events_order_gap_rows_then_tick() {
        let bank = ReleasedBank {
            slot: SLOT,
            bank_id: 0,
            parent_slot: SLOT - 1,
            block_height: height(SLOT),
            block_time_us: block_time_us(SLOT),
            blockhash: format!("hash{SLOT}"),
            gap: None,
            txs: vec![decoded(2), decoded(5)],
        };
        let gap = SlotGap::between(position(SLOT - 3), position(SLOT));
        let (account, payer) = (watching(3), watching(1));
        let events = bank_events(&bank, Some(gap), &[&account, &payer], context());
        assert_eq!(
            render(&events),
            vec![
                format!("gap {}->{SLOT}", SLOT - 3),
                format!("row {SLOT}:2 {}", key(3)),
                format!("row {SLOT}:2 {}", key(1)),
                format!("row {SLOT}:5 {}", key(3)),
                format!("row {SLOT}:5 {}", key(1)),
                format!("tick {SLOT}"),
            ]
        );
        assert!(matches!(events[0], DomainEvent::SlotGap(found) if found == gap));
        assert!(matches!(events[5], DomainEvent::SlotTick(tick) if tick == bank.tick()));
        for event in &events[1..5] {
            let DomainEvent::Transaction(row) = event else {
                panic!("expected a row, got {event:?}");
            };
            assert_eq!(row.check_invariants(), Ok(()));
            assert_eq!(row.conn_epoch_us, bank.txs[0].receipt.conn_epoch_us);
        }
        let quiet = watching(9);
        assert_eq!(
            render(&bank_events(&bank, None, &[&quiet], context())),
            vec![format!("tick {SLOT}")]
        );
    }

    #[test]
    fn unrequested_updates_count_as_dropped_frames() {
        let mut assembler = covered();
        let reporter = SourceMetrics::default();
        let totals = SourceMetrics::default();
        let counters = [&reporter, &totals];
        let watch = watching(3);
        for update in [
            GrpcUpdate::Unrequested("slot"),
            GrpcUpdate::Ping,
            GrpcUpdate::Unrequested("empty"),
            GrpcUpdate::Pong { id: 2 },
        ] {
            assert!(assembler.apply(update, &[&watch], &counters).is_empty());
        }
        for metrics in counters {
            let counted = metrics.snapshot();
            assert_eq!((counted.dropped_frames, counted.msgs), (2, 0));
        }
        assert_eq!(assembler.book().highest_slot(), 0);
        assert_eq!(assembler.book().pending_banks(), 0);
    }
}
