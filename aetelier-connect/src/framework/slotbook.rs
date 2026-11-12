use std::collections::BTreeMap;

pub const DEFAULT_MAX_PENDING_SLOTS: u64 = 32;
pub const DEFAULT_REORDER_WINDOW_SLOTS: u64 = 4;
pub const COVERAGE_MARGIN_SLOTS: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotBookConfig {
    pub max_pending_slots: u64,
    pub reorder_window_slots: u64,
}

impl Default for SlotBookConfig {
    fn default() -> Self {
        Self {
            max_pending_slots: DEFAULT_MAX_PENDING_SLOTS,
            reorder_window_slots: DEFAULT_REORDER_WINDOW_SLOTS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMeta {
    pub slot: u64,
    pub bank_id: u64,
    pub parent_slot: u64,
    pub block_height: Option<u64>,
    pub block_time_secs: Option<i64>,
    pub blockhash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SlotPosition {
    pub slot: u64,
    pub block_height: u64,
    pub block_time_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotTick {
    pub slot: u64,
    pub block_height: u64,
    pub block_time_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotGap {
    pub prev_slot: u64,
    pub prev_height: u64,
    pub prev_block_time_us: u64,
    pub next_slot: u64,
    pub next_height: u64,
    pub next_block_time_us: u64,
}

impl SlotGap {
    pub fn between(prev: SlotPosition, next: SlotPosition) -> Self {
        Self {
            prev_slot: prev.slot,
            prev_height: prev.block_height,
            prev_block_time_us: prev.block_time_us,
            next_slot: next.slot,
            next_height: next.block_height,
            next_block_time_us: next.block_time_us,
        }
    }

    pub fn blocks_lost(&self) -> u64 {
        self.next_height
            .saturating_sub(self.prev_height)
            .saturating_sub(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockContext<'a> {
    pub slot: u64,
    pub block_height: u64,
    pub block_time_us: u64,
    pub blockhash: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotApply {
    Finalized,
    GappedFinalized { blocks_lost: u64 },
    Held,
    Duplicate,
    Partial,
    Abandoned,
    Uncovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxApply {
    Buffered,
    Poisoned,
    Duplicate,
    Late,
    Uncovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageStart {
    AtSlot(u64),
    AfterHighestSlot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotBookState {
    Uncovered,
    Covered,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SlotBookCounters {
    pub slot_gaps: u64,
    pub blocks_lost: u64,
    pub late_txs: u64,
    pub partial_banks_discarded: u64,
    pub abandoned_banks: u64,
    pub duplicate_banks: u64,
    pub duplicate_txs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasedBank<T> {
    pub slot: u64,
    pub bank_id: u64,
    pub parent_slot: u64,
    pub block_height: u64,
    pub block_time_us: u64,
    pub blockhash: String,
    pub gap: Option<SlotGap>,
    pub txs: Vec<T>,
}

impl<T> ReleasedBank<T> {
    pub fn position(&self) -> SlotPosition {
        SlotPosition {
            slot: self.slot,
            block_height: self.block_height,
            block_time_us: self.block_time_us,
        }
    }

    pub fn tick(&self) -> SlotTick {
        SlotTick {
            slot: self.slot,
            block_height: self.block_height,
            block_time_us: self.block_time_us,
        }
    }

    pub fn block(&self) -> BlockContext<'_> {
        BlockContext {
            slot: self.slot,
            block_height: self.block_height,
            block_time_us: self.block_time_us,
            blockhash: &self.blockhash,
        }
    }

    pub fn apply(&self) -> SlotApply {
        match self.gap {
            Some(gap) => SlotApply::GappedFinalized {
                blocks_lost: gap.blocks_lost(),
            },
            None => SlotApply::Finalized,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSeal<T> {
    pub apply: SlotApply,
    pub released: Vec<ReleasedBank<T>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosedBank {
    Sealed,
    Abandoned,
}

struct PendingBank<T> {
    txs: BTreeMap<u64, T>,
    poisoned: bool,
}

impl<T> Default for PendingBank<T> {
    fn default() -> Self {
        Self {
            txs: BTreeMap::new(),
            poisoned: false,
        }
    }
}

struct SealedBank<T> {
    slot: u64,
    bank_id: u64,
    parent_slot: u64,
    block_time_us: u64,
    blockhash: String,
    txs: BTreeMap<u64, T>,
}

pub struct SourcedSlotbook<T> {
    config: SlotBookConfig,
    state: SlotBookState,
    boundary_slot: Option<u64>,
    highest_slot: u64,
    pending: BTreeMap<(u64, u64), PendingBank<T>>,
    sealed: BTreeMap<u64, SealedBank<T>>,
    closed: BTreeMap<(u64, u64), ClosedBank>,
    last_released: Option<SlotPosition>,
    counters: SlotBookCounters,
}

impl<T> SourcedSlotbook<T> {
    pub fn new(config: SlotBookConfig) -> Self {
        Self {
            config,
            state: SlotBookState::Uncovered,
            boundary_slot: None,
            highest_slot: 0,
            pending: BTreeMap::new(),
            sealed: BTreeMap::new(),
            closed: BTreeMap::new(),
            last_released: None,
            counters: SlotBookCounters::default(),
        }
    }

    pub fn state(&self) -> SlotBookState {
        self.state
    }

    pub fn counters(&self) -> SlotBookCounters {
        self.counters
    }

    pub fn highest_slot(&self) -> u64 {
        self.highest_slot
    }

    pub fn boundary_slot(&self) -> Option<u64> {
        self.boundary_slot
    }

    pub fn last_released(&self) -> Option<SlotPosition> {
        self.last_released
    }

    pub fn pending_banks(&self) -> usize {
        self.pending.len()
    }

    pub fn held_banks(&self) -> usize {
        self.sealed.len()
    }

    pub fn open_coverage(&mut self, start: CoverageStart) {
        debug_assert_eq!(self.state, SlotBookState::Uncovered);
        self.state = SlotBookState::Covered;
        self.boundary_slot = match start {
            CoverageStart::AtSlot(slot) => Some(slot),
            CoverageStart::AfterHighestSlot if self.highest_slot > 0 => {
                Some(self.highest_slot.saturating_add(COVERAGE_MARGIN_SLOTS))
            }
            CoverageStart::AfterHighestSlot => None,
        };
    }

    pub fn close_coverage(&mut self) -> Vec<ReleasedBank<T>> {
        self.counters.abandoned_banks += self.pending.len() as u64;
        self.pending.clear();
        let mut released = Vec::with_capacity(self.sealed.len());
        while let Some((block_height, bank)) = self.sealed.pop_first() {
            released.push(self.release(block_height, bank));
        }
        self.closed.clear();
        self.state = SlotBookState::Uncovered;
        self.boundary_slot = None;
        self.highest_slot = 0;
        released
    }

    pub fn on_transaction(
        &mut self,
        slot: u64,
        bank_id: u64,
        tx_index: u64,
        tx: T,
    ) -> TxApply {
        let Some(bank) = self.pending_bank(slot, bank_id) else {
            return self.rejected_input(slot, bank_id);
        };
        if bank.txs.contains_key(&tx_index) {
            self.counters.duplicate_txs += 1;
            return TxApply::Duplicate;
        }
        bank.txs.insert(tx_index, tx);
        TxApply::Buffered
    }

    pub fn on_undecodable(&mut self, slot: u64, bank_id: u64) -> TxApply {
        let Some(bank) = self.pending_bank(slot, bank_id) else {
            return self.rejected_input(slot, bank_id);
        };
        bank.poisoned = true;
        TxApply::Poisoned
    }

    pub fn on_block_meta(&mut self, meta: BlockMeta) -> SlotSeal<T> {
        let key = (meta.slot, meta.bank_id);
        let sealed = self.seal(meta);
        let released = self.release_ready();
        let apply = match sealed {
            SlotApply::Held => released
                .iter()
                .find(|bank| (bank.slot, bank.bank_id) == key)
                .map_or(SlotApply::Held, ReleasedBank::apply),
            other => other,
        };
        SlotSeal { apply, released }
    }

    fn pending_bank(&mut self, slot: u64, bank_id: u64) -> Option<&mut PendingBank<T>> {
        self.advance(slot);
        let accepted = self.covers(slot)
            && !self.closed.contains_key(&(slot, bank_id))
            && !self.last_released.is_some_and(|last| slot <= last.slot)
            && slot >= self.pending_floor();
        accepted.then(|| self.pending.entry((slot, bank_id)).or_default())
    }

    fn rejected_input(&mut self, slot: u64, bank_id: u64) -> TxApply {
        if !self.covers(slot) {
            return TxApply::Uncovered;
        }
        if !self.closed.contains_key(&(slot, bank_id))
            && self.last_released.is_some_and(|last| slot <= last.slot)
        {
            self.counters.duplicate_txs += 1;
            return TxApply::Duplicate;
        }
        self.counters.late_txs += 1;
        TxApply::Late
    }

    fn covers(&self, slot: u64) -> bool {
        self.state == SlotBookState::Covered
            && self.boundary_slot.is_some_and(|boundary| slot >= boundary)
    }

    fn pending_floor(&self) -> u64 {
        self.highest_slot
            .saturating_sub(self.config.max_pending_slots)
    }

    fn advance(&mut self, slot: u64) {
        if self.state == SlotBookState::Covered && self.boundary_slot.is_none() {
            self.boundary_slot = Some(slot.saturating_add(COVERAGE_MARGIN_SLOTS));
        }
        if slot <= self.highest_slot {
            return;
        }
        self.highest_slot = slot;
        let floor = self.pending_floor();
        while let Some(entry) = self.pending.first_entry() {
            if entry.key().0 >= floor {
                break;
            }
            let key = *entry.key();
            entry.remove();
            self.counters.abandoned_banks += 1;
            self.closed.insert(key, ClosedBank::Abandoned);
        }
        let keep_from = floor.saturating_sub(self.config.max_pending_slots);
        self.closed = self.closed.split_off(&(keep_from, 0));
    }

    fn seal(&mut self, meta: BlockMeta) -> SlotApply {
        self.advance(meta.slot);
        let key = (meta.slot, meta.bank_id);
        let bank = self.pending.remove(&key).unwrap_or_default();
        if !self.covers(meta.slot) {
            return SlotApply::Uncovered;
        }
        match self.closed.get(&key) {
            Some(ClosedBank::Sealed) => {
                self.counters.duplicate_banks += 1;
                return SlotApply::Duplicate;
            }
            Some(ClosedBank::Abandoned) => return SlotApply::Abandoned,
            None => {}
        }
        if meta.slot < self.pending_floor() {
            self.counters.abandoned_banks += 1;
            self.closed.insert(key, ClosedBank::Abandoned);
            return SlotApply::Abandoned;
        }
        self.closed.insert(key, ClosedBank::Sealed);
        let block_height = meta.block_height.filter(|height| *height > 0);
        let block_time_us = meta
            .block_time_secs
            .and_then(|secs| u64::try_from(secs).ok())
            .filter(|secs| *secs > 0)
            .and_then(|secs| secs.checked_mul(1_000_000));
        let (Some(block_height), Some(block_time_us)) = (block_height, block_time_us)
        else {
            self.counters.partial_banks_discarded += 1;
            return SlotApply::Partial;
        };
        if meta.blockhash.is_empty() {
            self.counters.partial_banks_discarded += 1;
            return SlotApply::Partial;
        }
        let stale = self.last_released.is_some_and(|last| {
            block_height <= last.block_height || meta.slot <= last.slot
        });
        if stale || self.sealed.contains_key(&block_height) {
            self.counters.duplicate_banks += 1;
            return SlotApply::Duplicate;
        }
        if bank.poisoned {
            self.counters.partial_banks_discarded += 1;
            return SlotApply::Partial;
        }
        self.sealed.insert(
            block_height,
            SealedBank {
                slot: meta.slot,
                bank_id: meta.bank_id,
                parent_slot: meta.parent_slot,
                block_time_us,
                blockhash: meta.blockhash,
                txs: bank.txs,
            },
        );
        SlotApply::Held
    }

    fn release_ready(&mut self) -> Vec<ReleasedBank<T>> {
        let mut released = Vec::new();
        while let Some(entry) = self.sealed.first_entry() {
            let block_height = *entry.key();
            let bank = entry.get();
            let contiguous = self
                .last_released
                .is_some_and(|last| block_height == last.block_height + 1);
            let parent_uncovered = self
                .boundary_slot
                .is_some_and(|boundary| bank.parent_slot < boundary);
            let window_passed = self.highest_slot
                >= bank.slot.saturating_add(self.config.reorder_window_slots);
            if !(contiguous || parent_uncovered || window_passed) {
                break;
            }
            let bank = entry.remove();
            released.push(self.release(block_height, bank));
        }
        released
    }

    fn release(&mut self, block_height: u64, bank: SealedBank<T>) -> ReleasedBank<T> {
        let next = SlotPosition {
            slot: bank.slot,
            block_height,
            block_time_us: bank.block_time_us,
        };
        let gap = self
            .last_released
            .filter(|last| block_height > last.block_height + 1)
            .map(|last| SlotGap::between(last, next));
        if let Some(gap) = gap {
            self.counters.slot_gaps += 1;
            self.counters.blocks_lost += gap.blocks_lost();
        }
        self.last_released = Some(next);
        ReleasedBank {
            slot: bank.slot,
            bank_id: bank.bank_id,
            parent_slot: bank.parent_slot,
            block_height,
            block_time_us: bank.block_time_us,
            blockhash: bank.blockhash,
            gap,
            txs: bank.txs.into_values().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: i64 = 1_791_321_067;

    fn book(reorder_window_slots: u64) -> SourcedSlotbook<&'static str> {
        SourcedSlotbook::new(SlotBookConfig {
            max_pending_slots: 32,
            reorder_window_slots,
        })
    }

    fn covered(boundary_slot: u64) -> SourcedSlotbook<&'static str> {
        let mut book = book(4);
        book.open_coverage(CoverageStart::AtSlot(boundary_slot));
        book
    }

    fn meta(slot: u64, parent_slot: u64, block_height: u64) -> BlockMeta {
        BlockMeta {
            slot,
            bank_id: 0,
            parent_slot,
            block_height: Some(block_height),
            block_time_secs: Some(T0 + slot as i64),
            blockhash: format!("hash{slot}"),
        }
    }

    fn block_time_us(slot: u64) -> u64 {
        (T0 as u64 + slot) * 1_000_000
    }

    fn slots(banks: &[ReleasedBank<&'static str>]) -> Vec<u64> {
        banks.iter().map(|bank| bank.slot).collect()
    }

    #[test]
    fn a_sealed_bank_releases_rows_in_tx_index_order_with_its_block_context() {
        let mut book = covered(100);
        assert_eq!(book.on_transaction(100, 0, 5, "e"), TxApply::Buffered);
        assert_eq!(book.on_transaction(100, 0, 1, "a"), TxApply::Buffered);
        assert_eq!(book.on_transaction(100, 0, 3, "c"), TxApply::Buffered);
        let seal = book.on_block_meta(meta(100, 99, 50));
        assert_eq!(seal.apply, SlotApply::Finalized);
        let bank = &seal.released[0];
        assert_eq!(bank.txs, vec!["a", "c", "e"]);
        assert_eq!(
            bank.tick(),
            SlotTick {
                slot: 100,
                block_height: 50,
                block_time_us: block_time_us(100),
            }
        );
        assert_eq!(bank.block().blockhash, "hash100");
        assert_eq!(book.pending_banks(), 0);
        assert_eq!(book.counters(), SlotBookCounters::default());
    }

    #[test]
    fn a_child_sealed_before_its_parent_is_held_then_released_in_height_order() {
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        book.on_transaction(102, 0, 0, "child");
        let child = book.on_block_meta(meta(102, 101, 52));
        assert_eq!(child.apply, SlotApply::Held);
        assert!(child.released.is_empty());
        assert_eq!(book.held_banks(), 1);
        book.on_transaction(101, 0, 0, "parent");
        let parent = book.on_block_meta(meta(101, 100, 51));
        assert_eq!(parent.apply, SlotApply::Finalized);
        assert_eq!(slots(&parent.released), vec![101, 102]);
        assert!(parent.released.iter().all(|bank| bank.gap.is_none()));
        let heights: Vec<u64> = parent
            .released
            .iter()
            .map(|bank| bank.tick().block_height)
            .collect();
        assert_eq!(heights, vec![51, 52]);
        assert_eq!(book.counters().slot_gaps, 0);
    }

    #[test]
    fn a_parent_later_than_the_reorder_window_is_a_duplicate_after_a_counted_gap() {
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        assert_eq!(
            book.on_block_meta(meta(102, 101, 52)).apply,
            SlotApply::Held
        );
        let expired = book.on_block_meta(meta(106, 102, 53));
        assert_eq!(slots(&expired.released), vec![102, 106]);
        assert_eq!(
            expired.released[0].gap,
            Some(SlotGap {
                prev_slot: 100,
                prev_height: 50,
                prev_block_time_us: block_time_us(100),
                next_slot: 102,
                next_height: 52,
                next_block_time_us: block_time_us(102),
            })
        );
        assert_eq!(
            expired.released[0].apply(),
            SlotApply::GappedFinalized { blocks_lost: 1 }
        );
        assert_eq!(book.on_transaction(101, 0, 0, "late"), TxApply::Duplicate);
        assert_eq!(
            book.on_block_meta(meta(101, 100, 51)).apply,
            SlotApply::Duplicate
        );
        let counters = book.counters();
        assert_eq!((counters.slot_gaps, counters.blocks_lost), (1, 1));
        assert_eq!((counters.duplicate_banks, counters.duplicate_txs), (1, 1));
    }

    #[test]
    fn a_partial_bank_is_discarded_counted_and_never_ticks() {
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        let no_time = BlockMeta {
            block_time_secs: None,
            ..meta(101, 100, 51)
        };
        book.on_transaction(101, 0, 0, "lost");
        assert_eq!(book.on_block_meta(no_time).apply, SlotApply::Partial);
        for partial in [
            BlockMeta {
                block_height: None,
                ..meta(102, 101, 52)
            },
            BlockMeta {
                block_time_secs: Some(0),
                ..meta(103, 102, 53)
            },
            BlockMeta {
                blockhash: String::new(),
                ..meta(104, 103, 54)
            },
        ] {
            assert_eq!(book.on_block_meta(partial).apply, SlotApply::Partial);
        }
        assert_eq!(book.on_transaction(101, 0, 1, "after"), TxApply::Late);
        let next = book.on_block_meta(meta(105, 104, 55));
        assert_eq!(next.apply, SlotApply::Held);
        let released = book.on_block_meta(meta(109, 105, 56)).released;
        assert_eq!(slots(&released), vec![105, 109]);
        assert_eq!(released[0].gap.map(|gap| gap.blocks_lost()), Some(4));
        let counters = book.counters();
        assert_eq!(counters.partial_banks_discarded, 4);
        assert_eq!(counters.late_txs, 1);
        assert_eq!(counters.blocks_lost, 4);
    }

    #[test]
    fn a_repeated_block_meta_or_tx_index_is_a_duplicate() {
        let mut book = covered(100);
        assert_eq!(book.on_transaction(100, 0, 0, "first"), TxApply::Buffered);
        assert_eq!(book.on_transaction(100, 0, 0, "again"), TxApply::Duplicate);
        let seal = book.on_block_meta(meta(100, 99, 50));
        assert_eq!(seal.released[0].txs, vec!["first"]);
        assert_eq!(
            book.on_block_meta(meta(100, 99, 50)).apply,
            SlotApply::Duplicate
        );
        assert_eq!(book.on_transaction(100, 0, 1, "late"), TxApply::Late);
        let counters = book.counters();
        assert_eq!(
            (
                counters.duplicate_txs,
                counters.duplicate_banks,
                counters.late_txs
            ),
            (1, 1, 1)
        );
    }

    #[test]
    fn a_reconnect_at_tip_yields_exactly_one_gap_bounded_by_released_banks() {
        let mut book = covered(100);
        for (slot, height) in [(100, 50), (101, 51), (103, 52)] {
            book.on_block_meta(meta(slot, slot - 1, height));
        }
        book.on_transaction(104, 0, 0, "in flight");
        assert!(book.close_coverage().is_empty());
        assert_eq!(book.state(), SlotBookState::Uncovered);
        assert_eq!(book.counters().abandoned_banks, 1);
        assert_eq!(book.on_transaction(140, 0, 0, "early"), TxApply::Uncovered);
        assert_eq!(
            book.on_block_meta(meta(140, 139, 85)).apply,
            SlotApply::Uncovered
        );
        book.open_coverage(CoverageStart::AfterHighestSlot);
        assert_eq!(book.boundary_slot(), Some(142));
        assert_eq!(
            book.on_block_meta(meta(141, 140, 86)).apply,
            SlotApply::Uncovered
        );
        book.on_transaction(142, 0, 0, "covered");
        let seal = book.on_block_meta(meta(142, 141, 87));
        assert_eq!(seal.apply, SlotApply::GappedFinalized { blocks_lost: 34 });
        assert_eq!(
            seal.released[0].gap,
            Some(SlotGap {
                prev_slot: 103,
                prev_height: 52,
                prev_block_time_us: block_time_us(103),
                next_slot: 142,
                next_height: 87,
                next_block_time_us: block_time_us(142),
            })
        );
        assert_eq!(seal.released[0].txs, vec!["covered"]);
        let follow = book.on_block_meta(meta(143, 142, 88));
        assert_eq!(follow.apply, SlotApply::Finalized);
        let counters = book.counters();
        assert_eq!((counters.slot_gaps, counters.blocks_lost), (1, 34));
    }

    #[test]
    fn banks_redelivered_after_a_reconnect_at_tip_yield_no_rows() {
        let mut book = covered(100);
        for (slot, height) in [(100, 50), (101, 51), (102, 52)] {
            book.on_block_meta(meta(slot, slot - 1, height));
        }
        book.close_coverage();
        book.on_transaction(99, 0, 0, "behind");
        book.open_coverage(CoverageStart::AfterHighestSlot);
        assert_eq!(book.on_transaction(101, 0, 0, "again"), TxApply::Duplicate);
        assert_eq!(
            book.on_block_meta(meta(101, 100, 51)).apply,
            SlotApply::Duplicate
        );
        assert_eq!(
            book.on_block_meta(meta(102, 101, 52)).apply,
            SlotApply::Duplicate
        );
        book.on_transaction(103, 0, 0, "new");
        let seal = book.on_block_meta(meta(103, 102, 53));
        assert_eq!(seal.apply, SlotApply::Finalized);
        assert_eq!(seal.released[0].txs, vec!["new"]);
        let counters = book.counters();
        assert_eq!((counters.duplicate_banks, counters.duplicate_txs), (2, 1));
        assert_eq!(counters.slot_gaps, 0);
    }

    #[test]
    fn closing_coverage_flushes_held_banks_and_abandons_unsealed_ones() {
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        book.on_transaction(102, 0, 0, "held");
        book.on_block_meta(meta(102, 101, 52));
        book.on_transaction(103, 0, 0, "unsealed");
        let flushed = book.close_coverage();
        assert_eq!(slots(&flushed), vec![102]);
        assert_eq!(flushed[0].txs, vec!["held"]);
        assert_eq!(
            flushed[0].apply(),
            SlotApply::GappedFinalized { blocks_lost: 1 }
        );
        assert_eq!(book.counters().abandoned_banks, 1);
        assert_eq!(book.pending_banks(), 0);
        assert_eq!(book.held_banks(), 0);
        assert_eq!(book.highest_slot(), 0);
    }

    #[test]
    fn unsealed_banks_behind_max_pending_slots_are_abandoned_and_show_as_a_gap() {
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        book.on_transaction(101, 0, 0, "stuck");
        book.on_transaction(134, 0, 0, "ahead");
        assert_eq!(book.counters().abandoned_banks, 1);
        assert_eq!(book.on_transaction(101, 0, 1, "too late"), TxApply::Late);
        assert_eq!(
            book.on_block_meta(meta(101, 100, 51)).apply,
            SlotApply::Abandoned
        );
        assert_eq!(
            book.on_block_meta(meta(134, 101, 52)).apply,
            SlotApply::Held
        );
        let released = book.on_block_meta(meta(138, 134, 53)).released;
        assert_eq!(slots(&released), vec![134, 138]);
        assert_eq!(
            released[0].apply(),
            SlotApply::GappedFinalized { blocks_lost: 1 }
        );
        assert_eq!(released[0].txs, vec!["ahead"]);
        assert_eq!(book.counters().abandoned_banks, 1);
    }

    #[test]
    fn an_undecodable_update_discards_its_bank_as_partial_and_shows_as_a_gap() {
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        book.on_transaction(101, 0, 0, "decoded");
        assert_eq!(book.on_undecodable(101, 0), TxApply::Poisoned);
        assert_eq!(
            book.on_block_meta(meta(101, 100, 51)).apply,
            SlotApply::Partial
        );
        assert_eq!(book.on_undecodable(101, 0), TxApply::Late);
        assert_eq!(book.on_undecodable(99, 0), TxApply::Uncovered);
        book.on_block_meta(meta(102, 101, 52));
        let released = book.on_block_meta(meta(106, 102, 53)).released;
        assert_eq!(slots(&released), vec![102, 106]);
        assert_eq!(released[0].gap.map(|gap| gap.blocks_lost()), Some(1));
        assert_eq!(book.counters().partial_banks_discarded, 1);
    }

    #[test]
    fn a_quiet_bank_releases_and_ticks_with_no_rows() {
        let mut book = covered(100);
        let seal = book.on_block_meta(meta(100, 99, 50));
        assert_eq!(seal.apply, SlotApply::Finalized);
        assert!(seal.released[0].txs.is_empty());
        assert_eq!(seal.released[0].tick().slot, 100);
    }

    #[test]
    fn the_first_bank_waits_for_the_window_unless_its_parent_is_uncovered() {
        let mut book = covered(100);
        assert_eq!(
            book.on_block_meta(meta(101, 100, 51)).apply,
            SlotApply::Held
        );
        assert!(book.on_block_meta(meta(102, 101, 52)).released.is_empty());
        let late_parent = book.on_block_meta(meta(100, 99, 50));
        assert_eq!(late_parent.apply, SlotApply::Finalized);
        assert_eq!(slots(&late_parent.released), vec![100, 101, 102]);
        let mut book = covered(100);
        assert_eq!(
            book.on_block_meta(meta(101, 100, 51)).apply,
            SlotApply::Held
        );
        let expired = book.on_block_meta(meta(105, 101, 52));
        assert_eq!(slots(&expired.released), vec![101, 105]);
        assert!(expired.released.iter().all(|bank| bank.gap.is_none()));
    }

    #[test]
    fn gap_bounds_count_lost_heights_not_skipped_slots() {
        let gap = SlotGap::between(
            SlotPosition {
                slot: 100,
                block_height: 50,
                block_time_us: block_time_us(100),
            },
            SlotPosition {
                slot: 110,
                block_height: 52,
                block_time_us: block_time_us(110),
            },
        );
        assert_eq!(gap.blocks_lost(), 1);
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        let skipped = book.on_block_meta(meta(110, 100, 51));
        assert_eq!(skipped.apply, SlotApply::Finalized);
        assert_eq!(book.counters().blocks_lost, 0);
    }

    #[test]
    fn gap_bounds_carry_the_block_times_of_both_released_banks() {
        let mut book = covered(100);
        let first = book.on_block_meta(meta(100, 99, 50)).released.remove(0);
        book.on_block_meta(meta(102, 101, 52));
        let expired = book.on_block_meta(meta(106, 102, 53)).released;
        let gap = expired[0].gap.unwrap();
        assert_eq!(
            gap,
            SlotGap::between(first.position(), expired[0].position())
        );
        assert_eq!(
            (gap.prev_block_time_us, gap.next_block_time_us),
            (block_time_us(100), block_time_us(102))
        );
        assert_eq!(book.last_released(), Some(expired[1].position()));
        book.close_coverage();
        book.on_transaction(140, 0, 0, "tip");
        book.open_coverage(CoverageStart::AfterHighestSlot);
        let resumed = book.on_block_meta(meta(142, 141, 87)).released;
        assert_eq!(
            resumed[0].gap,
            Some(SlotGap {
                prev_slot: 106,
                prev_height: 53,
                prev_block_time_us: block_time_us(106),
                next_slot: 142,
                next_height: 87,
                next_block_time_us: block_time_us(142),
            })
        );
    }

    #[test]
    fn banks_of_one_slot_assemble_separately_and_one_height_releases_once() {
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        book.on_transaction(101, 1, 0, "bank one");
        book.on_transaction(101, 2, 0, "bank two");
        let two = book.on_block_meta(BlockMeta {
            bank_id: 2,
            ..meta(101, 100, 51)
        });
        assert_eq!(two.released[0].txs, vec!["bank two"]);
        assert_eq!(two.released[0].bank_id, 2);
        let one = book.on_block_meta(BlockMeta {
            bank_id: 1,
            ..meta(101, 100, 51)
        });
        assert_eq!(one.apply, SlotApply::Duplicate);
        assert_eq!(book.pending_banks(), 0);
    }

    #[test]
    fn a_zero_reorder_window_releases_every_sealed_bank_at_once() {
        let mut book = book(0);
        book.open_coverage(CoverageStart::AtSlot(100));
        book.on_block_meta(meta(100, 99, 50));
        let seal = book.on_block_meta(meta(102, 101, 52));
        assert_eq!(seal.apply, SlotApply::GappedFinalized { blocks_lost: 1 });
    }

    #[test]
    fn coverage_opened_before_any_update_starts_two_slots_after_the_first_one() {
        let mut book = covered(100);
        book.on_block_meta(meta(100, 99, 50));
        book.close_coverage();
        book.open_coverage(CoverageStart::AfterHighestSlot);
        assert_eq!(book.boundary_slot(), None);
        assert_eq!(book.on_transaction(150, 0, 0, "tail"), TxApply::Uncovered);
        assert_eq!(book.boundary_slot(), Some(152));
        assert_eq!(
            book.on_block_meta(meta(150, 149, 100)).apply,
            SlotApply::Uncovered
        );
        assert_eq!(
            book.on_block_meta(meta(151, 150, 101)).apply,
            SlotApply::Uncovered
        );
        let seal = book.on_block_meta(meta(152, 151, 102));
        assert_eq!(seal.apply, SlotApply::GappedFinalized { blocks_lost: 51 });
    }

    #[test]
    fn inputs_before_coverage_opens_are_uncovered_but_raise_the_highest_slot() {
        let mut book = book(4);
        assert_eq!(book.on_transaction(120, 0, 0, "early"), TxApply::Uncovered);
        assert_eq!(
            book.on_block_meta(meta(121, 120, 70)).apply,
            SlotApply::Uncovered
        );
        assert_eq!(book.highest_slot(), 121);
        assert_eq!(book.last_released(), None);
        assert_eq!(book.counters(), SlotBookCounters::default());
    }
}
