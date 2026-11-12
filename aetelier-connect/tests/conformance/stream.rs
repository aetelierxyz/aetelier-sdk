use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use aetelier_connect::config::workers::market_worker_config::GrpcSection;
use aetelier_connect::framework::budget::SourceMetricsSnapshot;
use aetelier_connect::framework::grpc::{
    GrpcUpdate, StreamFilter, TRANSACTIONS_FILTER, TransactionUpdate, commitment_level,
    decode_update, ping_request,
};
use aetelier_connect::framework::model::DomainEvent;
use aetelier_connect::framework::onchain::{ReceiptStamp, WatchedAddress};
use aetelier_connect::framework::registry::{ReplayFrame, registry};
use aetelier_connect::framework::runtime::{ReconstructedEvent, RuntimeOutcome};
use aetelier_connect::framework::slotbook::{
    COVERAGE_MARGIN_SLOTS, CoverageStart, DEFAULT_REORDER_WINDOW_SLOTS, SlotApply,
    SlotBookConfig, SlotGap, SlotTick, SourcedSlotbook, TxApply,
};
use aetelier_connect::synchronizers::MarketSynchronizer;
use aetelier_connect::workers::gap_ledger::{GapCause, GapIncident, GapLedger};
use aetelier_io::onchain::{
    check_transactions, read_transactions_parquet, write_transactions_parquet,
};
use aetelier_io::snapshots::decompose_snapshots;
use aetelier_types::config::markets::market_config::{
    DeclaredDatatype, DeclaredSet, TransactionsConfig,
};
use aetelier_types::onchain::{
    ChainAddress, MatchRole, Transaction, TransactionVersion, encode_base58,
};
use aetelier_types::snapshots::MarketSnapshot;
use yellowstone_grpc_proto::prelude::{
    Message as WireMessage, SubscribeUpdate, SubscribeUpdateTransactionInfo,
    TransactionStatusMeta, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::prost::Message as _;

use super::harness::{
    Class, ExpectGap, ExpectInstrument, Outcome, StreamFixture, VenueConformance,
    stream_frame, stream_frames,
};
use super::kinds::{replay_stream_frames, run_stream_runtime, stream_fixtures};

const LAMPORTS_PER_SIGNATURE: u64 = 5_000;
const MICRO_LAMPORTS_PER_LAMPORT: u128 = 1_000_000;
const GRID_PERIOD_US: u64 = 1_000_000;
const PROVIDER_ENDPOINT: &str = "https://solana-yellowstone-grpc.publicnode.com:443";
const COMPUTE_BUDGET_PROGRAM: &str = "ComputeBudget111111111111111111111111111111";
const SET_COMPUTE_UNIT_LIMIT: u8 = 2;
const SET_COMPUTE_UNIT_PRICE: u8 = 3;
const PRECOMPILE_PROGRAMS: [&str; 3] = [
    "Ed25519SigVerify111111111111111111111111111",
    "KeccakSecp256k11111111111111111111111111111",
    "Secp256r1SigVerify1111111111111111111111111",
];
const INSTRUCTION_ERROR_TAG: [u8; 4] = [8, 0, 0, 0];

type Checked = Result<(), Outcome>;

fn fail<T>(reason: String) -> Result<T, Outcome> {
    Err(Outcome::Fail(reason))
}

fn ensure(condition: bool, reason: impl FnOnce() -> String) -> Checked {
    if condition { Ok(()) } else { fail(reason()) }
}

fn outcome_of(
    desc: &VenueConformance,
    check: impl FnOnce(&VenueConformance, &'static [StreamFixture]) -> Checked,
) -> Outcome {
    match stream_fixtures(desc).and_then(|fixtures| check(desc, fixtures)) {
        Ok(()) => Outcome::Pass,
        Err(outcome) => outcome,
    }
}

fn located(file: &str, outcome: Outcome) -> Outcome {
    match outcome {
        Outcome::Fail(reason) => Outcome::Fail(format!("{file}: {reason}")),
        other => other,
    }
}

struct WireTx {
    segment: usize,
    slot: u64,
    bank_id: u64,
    signature: String,
    info: SubscribeUpdateTransactionInfo,
}

struct WireMeta {
    segment: usize,
    slot: u64,
    bank_id: u64,
    parent_slot: u64,
    block_height: Option<u64>,
    block_time_us: Option<u64>,
    blockhash: String,
}

enum Wire {
    Tx(Box<WireTx>),
    Meta(WireMeta),
    Reconnect,
    Other,
}

struct Bank {
    segment: usize,
    slot: u64,
    parent_slot: u64,
    block_height: u64,
    block_time_us: u64,
    blockhash: String,
    meta: usize,
    txs: Vec<usize>,
    covered: bool,
}

struct Capture {
    frames: Vec<ReplayFrame>,
    wire: Vec<Wire>,
    banks: Vec<Bank>,
}

impl Capture {
    fn load(file: &'static str) -> Result<Arc<Capture>, Outcome> {
        static CAPTURES: OnceLock<Mutex<HashMap<&'static str, Arc<Capture>>>> =
            OnceLock::new();
        let cache = CAPTURES.get_or_init(Default::default);
        let cached = cache
            .lock()
            .map_err(|e| Outcome::Fail(format!("capture cache poisoned: {e}")))?
            .get(file)
            .cloned();
        if let Some(capture) = cached {
            return Ok(capture);
        }
        let capture = Arc::new(Capture::decode(file).map_err(Outcome::Fail)?);
        cache
            .lock()
            .map_err(|e| Outcome::Fail(format!("capture cache poisoned: {e}")))?
            .insert(file, capture.clone());
        Ok(capture)
    }

    fn decode(file: &str) -> Result<Capture, String> {
        let frames = stream_frames(file)?;
        let mut wire = Vec::with_capacity(frames.len());
        let mut first_slots: Vec<Option<u64>> = vec![None];
        for (i, frame) in frames.iter().enumerate() {
            let segment = first_slots.len() - 1;
            let item = match frame {
                ReplayFrame::Reconnect { .. } => {
                    first_slots.push(None);
                    Wire::Reconnect
                }
                ReplayFrame::Update { bytes, .. } => {
                    let update =
                        SubscribeUpdate::decode(bytes.as_slice()).map_err(|e| {
                            format!("{file} frame {i} is not a SubscribeUpdate: {e}")
                        })?;
                    match update.update_oneof {
                        Some(UpdateOneof::Transaction(update)) => {
                            let Some(info) = update.transaction else {
                                return Err(format!(
                                    "{file} frame {i} carries no transaction"
                                ));
                            };
                            first_slots[segment].get_or_insert(update.slot);
                            Wire::Tx(Box::new(WireTx {
                                segment,
                                slot: update.slot,
                                bank_id: update.bank_id,
                                signature: encode_base58(&info.signature),
                                info,
                            }))
                        }
                        Some(UpdateOneof::BlockMeta(meta)) => {
                            first_slots[segment].get_or_insert(meta.slot);
                            Wire::Meta(WireMeta {
                                segment,
                                slot: meta.slot,
                                bank_id: meta.bank_id,
                                parent_slot: meta.parent_slot,
                                block_height: meta.block_height.map(|h| h.block_height),
                                block_time_us: meta
                                    .block_time
                                    .and_then(|t| u64::try_from(t.timestamp).ok())
                                    .filter(|secs| *secs > 0)
                                    .map(|secs| secs * 1_000_000),
                                blockhash: meta.blockhash,
                            })
                        }
                        _ => Wire::Other,
                    }
                }
            };
            wire.push(item);
        }
        let boundaries: Vec<Option<u64>> = first_slots
            .iter()
            .map(|slot| slot.map(|slot| slot + COVERAGE_MARGIN_SLOTS))
            .collect();
        let mut banks = Vec::new();
        for (i, item) in wire.iter().enumerate() {
            let Wire::Meta(meta) = item else { continue };
            let (Some(block_height), Some(block_time_us)) =
                (meta.block_height, meta.block_time_us)
            else {
                continue;
            };
            let mut txs: Vec<usize> = wire[..i]
                .iter()
                .enumerate()
                .filter_map(|(j, other)| match other {
                    Wire::Tx(tx)
                        if (tx.segment, tx.slot, tx.bank_id)
                            == (meta.segment, meta.slot, meta.bank_id) =>
                    {
                        Some(j)
                    }
                    _ => None,
                })
                .collect();
            txs.sort_by_key(|&j| match &wire[j] {
                Wire::Tx(tx) => tx.info.index,
                _ => u64::MAX,
            });
            banks.push(Bank {
                segment: meta.segment,
                slot: meta.slot,
                parent_slot: meta.parent_slot,
                block_height,
                block_time_us,
                blockhash: meta.blockhash.clone(),
                meta: i,
                txs,
                covered: boundaries[meta.segment].is_some_and(|b| meta.slot >= b),
            });
        }
        Ok(Capture {
            frames,
            wire,
            banks,
        })
    }

    fn txs(&self) -> impl Iterator<Item = &WireTx> {
        self.wire.iter().filter_map(|item| match item {
            Wire::Tx(tx) => Some(tx.as_ref()),
            _ => None,
        })
    }

    fn bank_txs<'a>(&'a self, bank: &'a Bank) -> impl Iterator<Item = &'a WireTx> {
        bank.txs.iter().filter_map(|&j| match &self.wire[j] {
            Wire::Tx(tx) => Some(tx.as_ref()),
            _ => None,
        })
    }

    fn metas(&self) -> usize {
        self.wire
            .iter()
            .filter(|item| matches!(item, Wire::Meta(_)))
            .count()
    }

    fn markers(&self) -> Vec<usize> {
        self.wire
            .iter()
            .enumerate()
            .filter(|(_, item)| matches!(item, Wire::Reconnect))
            .map(|(i, _)| i)
            .collect()
    }

    fn by_signature(&self, signature: &str) -> Result<&WireTx, Outcome> {
        self.txs()
            .find(|tx| tx.signature == signature)
            .ok_or_else(|| {
                Outcome::Fail(format!(
                    "row {signature} has no transaction update in the capture"
                ))
            })
    }

    fn kept_in<'a>(
        &'a self,
        bank: &'a Bank,
        address: &ChainAddress,
        config: &TransactionsConfig,
    ) -> Vec<&'a WireTx> {
        self.bank_txs(bank)
            .filter(|tx| keeps(tx, address, config))
            .collect()
    }

    fn bank_frames(&self, bank: &Bank) -> Vec<usize> {
        let mut frames = bank.txs.clone();
        frames.push(bank.meta);
        frames.sort_unstable();
        frames
    }

    fn identity(&self) -> Vec<usize> {
        (0..self.frames.len()).collect()
    }

    fn arranged(&self, order: &[usize]) -> Vec<ReplayFrame> {
        order.iter().map(|&i| self.frames[i].clone()).collect()
    }
}

fn message(info: &SubscribeUpdateTransactionInfo) -> Option<&WireMessage> {
    info.transaction.as_ref()?.message.as_ref()
}

fn status(info: &SubscribeUpdateTransactionInfo) -> Option<&TransactionStatusMeta> {
    info.meta.as_ref()
}

fn failed(info: &SubscribeUpdateTransactionInfo) -> bool {
    status(info).is_some_and(|meta| meta.err.is_some())
}

fn signer_count(message: &WireMessage) -> usize {
    message
        .header
        .as_ref()
        .map_or(0, |header| header.num_required_signatures as usize)
        .min(message.account_keys.len())
}

fn loaded(info: &SubscribeUpdateTransactionInfo) -> impl Iterator<Item = &Vec<u8>> {
    status(info).into_iter().flat_map(|meta| {
        meta.loaded_writable_addresses
            .iter()
            .chain(&meta.loaded_readonly_addresses)
    })
}

fn all_keys(info: &SubscribeUpdateTransactionInfo) -> Vec<&Vec<u8>> {
    message(info)
        .into_iter()
        .flat_map(|message| message.account_keys.iter())
        .chain(loaded(info))
        .collect()
}

fn account_keys(info: &SubscribeUpdateTransactionInfo) -> Vec<String> {
    all_keys(info)
        .into_iter()
        .map(|key| encode_base58(key))
        .collect()
}

fn signers(info: &SubscribeUpdateTransactionInfo) -> Vec<String> {
    message(info).map_or_else(Vec::new, |message| {
        message.account_keys[..signer_count(message)]
            .iter()
            .map(|key| encode_base58(key))
            .collect()
    })
}

fn role(
    info: &SubscribeUpdateTransactionInfo,
    address: &ChainAddress,
) -> Option<MatchRole> {
    let key = address.pubkey().as_slice();
    let static_role = message(info).and_then(|message| {
        let signers = signer_count(message);
        message
            .account_keys
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.as_slice() == key)
            .map(|(index, _)| match index {
                0 if signers > 0 => MatchRole::FeePayer,
                index if index < signers => MatchRole::Signer,
                _ => MatchRole::AccountKey,
            })
            .max()
    });
    static_role.or_else(|| {
        loaded(info)
            .any(|candidate| candidate.as_slice() == key)
            .then_some(MatchRole::AccountKey)
    })
}

fn invokes(info: &SubscribeUpdateTransactionInfo, address: &ChainAddress) -> bool {
    let keys = all_keys(info);
    message(info).is_some_and(|message| {
        message.instructions.iter().any(|instruction| {
            keys.get(instruction.program_id_index as usize)
                .is_some_and(|key| key.as_slice() == address.pubkey().as_slice())
        })
    })
}

fn keeps(tx: &WireTx, address: &ChainAddress, config: &TransactionsConfig) -> bool {
    !tx.info.is_vote
        && (config.include_failed || !failed(&tx.info))
        && role(&tx.info, address).is_some_and(|role| role.satisfies(config.match_role))
}

fn wire_version(message: &WireMessage) -> TransactionVersion {
    match (message.config.is_some(), message.versioned) {
        (true, _) => TransactionVersion::V1,
        (false, true) => TransactionVersion::V0,
        (false, false) => TransactionVersion::Legacy,
    }
}

fn program_of<'a>(keys: &[&'a Vec<u8>], program_id_index: u32) -> Option<&'a Vec<u8>> {
    keys.get(program_id_index as usize).copied()
}

fn requested_compute(
    info: &SubscribeUpdateTransactionInfo,
) -> (Option<u32>, Option<u64>, Option<u64>) {
    let Some(message) = message(info) else {
        return (None, None, None);
    };
    if let Some(config) = &message.config {
        return (
            Some(config.compute_unit_limit.unwrap_or(0)),
            None,
            Some(config.priority_fee.unwrap_or(0)),
        );
    }
    let keys = all_keys(info);
    let (mut limit, mut price) = (None, None);
    for instruction in &message.instructions {
        let budget = program_of(&keys, instruction.program_id_index)
            .is_some_and(|key| encode_base58(key) == COMPUTE_BUDGET_PROGRAM);
        if !budget {
            continue;
        }
        match instruction.data.split_first() {
            Some((&SET_COMPUTE_UNIT_LIMIT, payload)) if limit.is_none() => {
                limit = payload
                    .first_chunk::<4>()
                    .map(|bytes| u32::from_le_bytes(*bytes));
            }
            Some((&SET_COMPUTE_UNIT_PRICE, payload)) if price.is_none() => {
                price = payload
                    .first_chunk::<8>()
                    .map(|bytes| u64::from_le_bytes(*bytes));
            }
            _ => {}
        }
    }
    (limit, price, None)
}

fn fee_signatures(info: &SubscribeUpdateTransactionInfo) -> u64 {
    let signatures = info
        .transaction
        .as_ref()
        .map_or(0, |transaction| transaction.signatures.len() as u64);
    let keys = all_keys(info);
    let precompiled: u64 = message(info)
        .map(|message| {
            message
                .instructions
                .iter()
                .filter(|instruction| {
                    program_of(&keys, instruction.program_id_index).is_some_and(|key| {
                        PRECOMPILE_PROGRAMS.contains(&encode_base58(key).as_str())
                    })
                })
                .map(|instruction| {
                    u64::from(instruction.data.first().copied().unwrap_or(0))
                })
                .sum()
        })
        .unwrap_or(0);
    signatures + precompiled
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn failed_program_line(logs: &[String]) -> Option<(String, String)> {
    logs.iter().find_map(|line| {
        let (program, message) =
            line.strip_prefix("Program ")?.split_once(" failed: ")?;
        ChainAddress::solana(program).ok()?;
        Some((program.to_string(), message.to_string()))
    })
}

fn watched(
    desc: &VenueConformance,
    fixture: &StreamFixture,
) -> Result<ChainAddress, Outcome> {
    let ExpectInstrument::Address(chain) = desc.expect_instrument else {
        return fail(format!(
            "stream venue {} expects no address instrument",
            desc.venue
        ));
    };
    ChainAddress::parse(chain, fixture.watched).map_err(|e| {
        Outcome::Fail(format!(
            "{}: watched address {} does not parse: {e}",
            fixture.file, fixture.watched
        ))
    })
}

fn watching(
    match_role: MatchRole,
    include_failed: bool,
    logs: bool,
) -> TransactionsConfig {
    TransactionsConfig {
        enabled: true,
        match_role,
        include_failed,
        logs,
    }
}

fn replay(
    desc: &VenueConformance,
    fixture: &StreamFixture,
    frames: &[ReplayFrame],
    config: TransactionsConfig,
) -> Result<(Vec<DomainEvent>, SourceMetricsSnapshot), Outcome> {
    replay_stream_frames(
        desc,
        &[fixture.watched.to_string()],
        fixture.commitment,
        frames,
        config,
    )
    .map_err(|outcome| located(fixture.file, outcome))
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

fn ticks(events: &[DomainEvent]) -> Vec<SlotTick> {
    events
        .iter()
        .filter_map(|event| match event {
            DomainEvent::SlotTick(tick) => Some(*tick),
            _ => None,
        })
        .collect()
}

fn gaps(events: &[DomainEvent]) -> Vec<SlotGap> {
    events
        .iter()
        .filter_map(|event| match event {
            DomainEvent::SlotGap(gap) => Some(*gap),
            _ => None,
        })
        .collect()
}

fn render_row(row: &Transaction) -> String {
    format!(
        "row {} {}:{} {} {} {}",
        row.address,
        row.slot,
        row.tx_index,
        row.signature,
        row.block_height,
        row.blockhash
    )
}

fn render(events: &[DomainEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            DomainEvent::Transaction(row) => Some(render_row(row)),
            DomainEvent::SlotTick(tick) => Some(format!(
                "tick {} {} {}",
                tick.slot, tick.block_height, tick.block_time_us
            )),
            DomainEvent::SlotGap(gap) => Some(format!(
                "gap {}/{} {}/{}",
                gap.prev_slot, gap.prev_height, gap.next_slot, gap.next_height
            )),
            DomainEvent::ConnectionLost { .. } => Some("lost".to_string()),
            DomainEvent::ConnectionResumed => Some("resumed".to_string()),
            _ => None,
        })
        .collect()
}

fn heights_rise(file: &str, events: &[DomainEvent]) -> Checked {
    let ticks = ticks(events);
    match ticks.windows(2).find(|pair| {
        pair[1].block_height <= pair[0].block_height || pair[1].slot <= pair[0].slot
    }) {
        Some(pair) => fail(format!(
            "{file}: SlotTick {}/{} after {}/{}: ticks must rise in height order",
            pair[1].slot, pair[1].block_height, pair[0].slot, pair[0].block_height
        )),
        None => Ok(()),
    }
}

fn unique_rows(file: &str, rows: &[&Transaction]) -> Checked {
    let mut seen = HashSet::new();
    match rows.iter().find(|row| !seen.insert(row.row_key())) {
        Some(row) => fail(format!(
            "{file}: ({}, {}) emitted twice",
            row.address, row.signature
        )),
        None => Ok(()),
    }
}

fn expect_gap(gap: &SlotGap) -> ExpectGap {
    ExpectGap {
        prev_slot: gap.prev_slot,
        prev_height: gap.prev_height,
        next_slot: gap.next_slot,
        next_height: gap.next_height,
    }
}

#[derive(Debug, PartialEq)]
enum Verdict {
    Tx(TxApply),
    Seal(SlotApply, Vec<(u64, SlotApply)>),
}

fn slotbook_verdicts(
    capture: &Capture,
    order: &[usize],
    watch: &WatchedAddress,
) -> Result<Vec<Option<Verdict>>, Outcome> {
    let mut book: SourcedSlotbook<()> = SourcedSlotbook::new(SlotBookConfig::default());
    book.open_coverage(CoverageStart::AfterHighestSlot);
    let mut verdicts = Vec::with_capacity(order.len());
    for &index in order {
        let verdict = match &capture.frames[index] {
            ReplayFrame::Reconnect { .. } => {
                book.close_coverage();
                book.open_coverage(CoverageStart::AfterHighestSlot);
                None
            }
            ReplayFrame::Update { bytes, .. } => {
                let update = SubscribeUpdate::decode(bytes.as_slice()).map_err(|e| {
                    Outcome::Fail(format!("frame {index} is not a SubscribeUpdate: {e}"))
                })?;
                match decode_update(update, ReceiptStamp::default()) {
                    GrpcUpdate::Transaction(TransactionUpdate {
                        slot,
                        bank_id,
                        decoded: Ok(tx),
                    }) => watch.matches(&tx).map(|_| {
                        Verdict::Tx(book.on_transaction(slot, bank_id, tx.tx_index, ()))
                    }),
                    GrpcUpdate::BlockMeta(meta) => {
                        let seal = book.on_block_meta(meta);
                        Some(Verdict::Seal(
                            seal.apply,
                            seal.released
                                .iter()
                                .map(|bank| (bank.slot, bank.apply()))
                                .collect(),
                        ))
                    }
                    _ => None,
                }
            }
        };
        verdicts.push(verdict);
    }
    Ok(verdicts)
}

fn verdict_at<'a>(
    order: &[usize],
    verdicts: &'a [Option<Verdict>],
    frame: usize,
    last: bool,
) -> Option<&'a Verdict> {
    let position = if last {
        order.iter().rposition(|&i| i == frame)
    } else {
        order.iter().position(|&i| i == frame)
    }?;
    verdicts.get(position)?.as_ref()
}

fn moved(order: &[usize], block: &[usize], anchor: usize, after: bool) -> Vec<usize> {
    let mut out = Vec::with_capacity(order.len());
    for &index in order {
        if block.contains(&index) {
            continue;
        }
        if index == anchor && !after {
            out.extend_from_slice(block);
        }
        out.push(index);
        if index == anchor && after {
            out.extend_from_slice(block);
        }
    }
    out
}

fn decoded_row(file: &str, row: &Transaction, tx: &WireTx) -> Result<bool, Outcome> {
    let at = || format!("{file}: {}", row.signature);
    let (Some(message), Some(meta)) = (message(&tx.info), status(&tx.info)) else {
        return fail(format!("{}: the update carries no message or status", at()));
    };
    let version = wire_version(message);
    ensure(row.version == version, || {
        format!(
            "{}: version {:?}, the message is {version:?}",
            at(),
            row.version
        )
    })?;
    let requested = requested_compute(&tx.info);
    let found = (
        row.compute_unit_limit,
        row.compute_unit_price_micro_lamports,
        row.priority_fee_lamports,
    );
    ensure(found == requested, || {
        format!(
            "{}: compute (limit, price, priority) {found:?}, the message requests {requested:?}",
            at()
        )
    })?;
    ensure(row.fee_lamports == meta.fee, || {
        format!(
            "{}: fee {} lamports, the status says {}",
            at(),
            row.fee_lamports,
            meta.fee
        )
    })?;
    ensure(
        row.compute_units_consumed == meta.compute_units_consumed
            && row.cost_units == meta.cost_units,
        || format!("{}: consumed or cost units differ from the status", at()),
    )?;
    let priority = match requested {
        (_, _, Some(fee)) => Some(fee),
        (_, None, None) => Some(0),
        (Some(limit), Some(price), None) => Some(
            (u128::from(price) * u128::from(limit)).div_ceil(MICRO_LAMPORTS_PER_LAMPORT)
                as u64,
        ),
        (None, Some(_), None) => None,
    };
    if let Some(priority) = priority {
        let signatures = fee_signatures(&tx.info);
        let expected = LAMPORTS_PER_SIGNATURE * signatures + priority;
        ensure(meta.fee == expected, || {
            format!(
                "{}: fee {} lamports, {signatures} signatures and a {priority} lamport priority fee make {expected}",
                at(),
                meta.fee
            )
        })?;
    }
    ensure(row.success == meta.err.is_none(), || {
        format!("{}: success {} against the status error", at(), row.success)
    })?;
    match (&meta.err, &row.failure) {
        (None, None) => {}
        (Some(err), Some(failure)) => {
            ensure(!failure.error.is_empty(), || {
                format!("{}: empty failure_error", at())
            })?;
            let keys = all_keys(&tx.info);
            if err.err.len() > INSTRUCTION_ERROR_TAG.len()
                && err.err[..INSTRUCTION_ERROR_TAG.len()] == INSTRUCTION_ERROR_TAG
            {
                let index = u32::from(err.err[INSTRUCTION_ERROR_TAG.len()]);
                let instruction = message.instructions.get(index as usize);
                let program = instruction
                    .and_then(|instruction| {
                        program_of(&keys, instruction.program_id_index)
                    })
                    .map(|key| encode_base58(key));
                ensure(
                    failure
                        .error
                        .starts_with(&format!("InstructionError({index}, ")),
                    || {
                        format!(
                            "{}: failure_error {} for instruction {index}",
                            at(),
                            failure.error
                        )
                    },
                )?;
                ensure(
                    failure.instruction_index == Some(index)
                        && failure.program_id == program
                        && failure.instruction_data
                            == instruction.map(|instruction| hex(&instruction.data))
                        && failure.instruction_error.is_some(),
                    || format!("{}: failure instruction columns {failure:?}", at()),
                )?;
            } else {
                ensure(
                    failure.instruction_index.is_none()
                        && failure.program_id.is_none()
                        && failure.instruction_data.is_none()
                        && failure.instruction_error.is_none(),
                    || {
                        format!(
                            "{}: a transaction-level error with instruction columns",
                            at()
                        )
                    },
                )?;
            }
            let logged = (!meta.log_messages_none)
                .then(|| failed_program_line(&meta.log_messages))
                .flatten();
            let found = failure
                .log_program_id
                .clone()
                .zip(failure.log_message.clone());
            ensure(found == logged, || {
                format!(
                    "{}: failure log columns {found:?}, the logs say {logged:?}",
                    at()
                )
            })?;
        }
        _ => return fail(format!("{}: failure columns disagree with success", at())),
    }
    ensure(row.log_messages.is_none(), || {
        format!("{}: log_messages stored with logs = false", at())
    })?;
    Ok(priority.is_some())
}

pub fn tx_decode_surface(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        let mut versions = HashSet::new();
        let mut classes = Vec::new();
        let (mut failures, mut lookups, mut fees) = (0usize, 0usize, 0usize);
        for fixture in fixtures {
            let file = fixture.file;
            let capture = Capture::load(file)?;
            let (events, metrics) = replay(
                desc,
                fixture,
                &capture.frames,
                TransactionsConfig::default(),
            )?;
            ensure(
                metrics.decode_err == 0 && metrics.dropped_frames == 0,
                || {
                    format!(
                        "{file}: {} malformed and {} unrequested updates",
                        metrics.decode_err, metrics.dropped_frames
                    )
                },
            )?;
            let decoded = (capture.txs().count() + capture.metas()) as u64;
            ensure(metrics.msgs == decoded, || {
                format!(
                    "{file}: {} updates decoded, the capture holds {decoded} transactions and block metas",
                    metrics.msgs
                )
            })?;
            let replayed = rows(&events);
            ensure(replayed.len() == fixture.rows, || {
                format!(
                    "{file}: {} rows, the descriptor expects {}",
                    replayed.len(),
                    fixture.rows
                )
            })?;
            let raw = replayed
                .iter()
                .filter(|row| {
                    row.failure
                        .as_ref()
                        .is_some_and(|failure| failure.error.starts_with("0x"))
                })
                .count() as u64;
            ensure(raw <= metrics.tx_error_decode_failures, || {
                format!(
                    "{file}: {raw} raw failure errors, {} counted",
                    metrics.tx_error_decode_failures
                )
            })?;
            for row in &replayed {
                ensure(
                    row.exchange == desc.venue && row.commitment == fixture.commitment,
                    || {
                        format!(
                            "{file}: {} row of {} at {}",
                            row.signature, row.exchange, row.commitment
                        )
                    },
                )?;
                let tx = capture
                    .by_signature(&row.signature)
                    .map_err(|o| located(file, o))?;
                if decoded_row(file, row, tx)? {
                    fees += 1;
                }
                versions.insert(row.version);
                failures += usize::from(!row.success);
                lookups += usize::from(loaded(&tx.info).next().is_some());
                if !classes.contains(&Class::Transaction) {
                    classes.push(Class::Transaction);
                }
            }
            let (logged, _) = replay(
                desc,
                fixture,
                &capture.frames,
                watching(MatchRole::AccountKey, true, true),
            )?;
            for row in rows(&logged) {
                let tx = capture
                    .by_signature(&row.signature)
                    .map_err(|o| located(file, o))?;
                let wire = status(&tx.info)
                    .filter(|meta| !meta.log_messages_none)
                    .map(|meta| meta.log_messages.clone());
                ensure(row.log_messages == wire, || {
                    format!(
                        "{file}: {} stores other logs than the update carries",
                        row.signature
                    )
                })?;
            }
        }
        for want in desc.expect_classes {
            ensure(classes.contains(want), || {
                format!("documented class {want:?} never appears in the stream fixtures")
            })?;
        }
        for version in [
            TransactionVersion::Legacy,
            TransactionVersion::V0,
            TransactionVersion::V1,
        ] {
            ensure(versions.contains(&version), || {
                format!("no replayed row is a {version:?} transaction")
            })?;
        }
        ensure(failures > 0 && lookups > 0 && fees > 0, || {
            format!(
                "the stream fixtures exercise {failures} failed, {lookups} address-table and {fees} fee-checked rows"
            )
        })
    })
}

pub fn slot_assembly(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        for fixture in fixtures {
            let file = fixture.file;
            let capture = Capture::load(file)?;
            let address = watched(desc, fixture)?;
            let config = TransactionsConfig::default();
            let (events, metrics) =
                replay(desc, fixture, &capture.frames, config.clone())?;
            ensure(
                metrics.partial_banks_discarded == 0
                    && metrics.abandoned_banks == 0
                    && metrics.late_txs == 0,
                || {
                    format!(
                        "{file}: {} partial, {} abandoned banks and {} late transactions",
                        metrics.partial_banks_discarded,
                        metrics.abandoned_banks,
                        metrics.late_txs
                    )
                },
            )?;
            let mut banks = capture.banks.iter().filter(|bank| bank.covered);
            let mut pending: Vec<&Transaction> = Vec::new();
            let mut ticked = 0usize;
            for event in &events {
                match event {
                    DomainEvent::Transaction(row) => pending.push(row),
                    DomainEvent::SlotTick(tick) => {
                        let Some(bank) = banks.next() else {
                            return fail(format!(
                                "{file}: SlotTick {} past the covered banks",
                                tick.slot
                            ));
                        };
                        ensure(
                            (tick.slot, tick.block_height, tick.block_time_us)
                                == (bank.slot, bank.block_height, bank.block_time_us),
                            || {
                                format!(
                                    "{file}: SlotTick {}/{} where bank {}/{} seals next",
                                    tick.slot,
                                    tick.block_height,
                                    bank.slot,
                                    bank.block_height
                                )
                            },
                        )?;
                        let want: Vec<(&str, u64)> = capture
                            .kept_in(bank, &address, &config)
                            .iter()
                            .map(|tx| (tx.signature.as_str(), tx.info.index))
                            .collect();
                        let found: Vec<(&str, u64)> = pending
                            .iter()
                            .map(|row| (row.signature.as_str(), row.tx_index))
                            .collect();
                        ensure(found == want, || {
                            format!(
                                "{file}: bank {} released rows {found:?}, its updates hold {want:?}",
                                bank.slot
                            )
                        })?;
                        for row in pending.drain(..) {
                            ensure(
                                row.slot == bank.slot
                                    && row.block_height == bank.block_height
                                    && row.blockhash == bank.blockhash
                                    && row.source_tx_ts_us == bank.block_time_us,
                                || {
                                    format!(
                                        "{file}: {} carries block context {}/{} {}, its bank is {}/{} {}",
                                        row.signature,
                                        row.slot,
                                        row.block_height,
                                        row.blockhash,
                                        bank.slot,
                                        bank.block_height,
                                        bank.blockhash
                                    )
                                },
                            )?;
                        }
                        ticked += 1;
                    }
                    _ => ensure(pending.is_empty(), || {
                        format!("{file}: rows released outside their bank")
                    })?,
                }
            }
            ensure(pending.is_empty() && banks.next().is_none(), || {
                format!("{file}: covered banks left without their SlotTick")
            })?;
            ensure(ticked == fixture.ticks, || {
                format!(
                    "{file}: {ticked} SlotTicks, the descriptor expects {}",
                    fixture.ticks
                )
            })?;
            let positions: Vec<(u64, u64)> =
                rows(&events).iter().map(|row| row.position()).collect();
            ensure(positions.windows(2).all(|pair| pair[0] < pair[1]), || {
                format!("{file}: rows out of (slot, tx_index) order")
            })?;
        }
        dropped_block_meta(desc, fixtures)
    })
}

fn dropped_block_meta(
    desc: &VenueConformance,
    fixtures: &'static [StreamFixture],
) -> Checked {
    let config = TransactionsConfig::default();
    for fixture in fixtures.iter().filter(|fixture| fixture.gaps.is_empty()) {
        let capture = Capture::load(fixture.file)?;
        let address = watched(desc, fixture)?;
        let covered: Vec<&Bank> =
            capture.banks.iter().filter(|bank| bank.covered).collect();
        let Some(k) = (1..covered.len().saturating_sub(1)).find(|&k| {
            covered[k - 1].segment == covered[k].segment
                && covered[k + 1].segment == covered[k].segment
                && !capture.kept_in(covered[k], &address, &config).is_empty()
        }) else {
            continue;
        };
        let (prev, lost, next) = (covered[k - 1], covered[k], covered[k + 1]);
        let file = fixture.file;
        let order: Vec<usize> = capture
            .identity()
            .into_iter()
            .filter(|&i| i != lost.meta)
            .collect();
        let (baseline, _) = replay(desc, fixture, &capture.frames, config.clone())?;
        let (events, metrics) = replay(desc, fixture, &capture.arranged(&order), config)?;
        let want: Vec<String> = rows(&baseline)
            .into_iter()
            .filter(|row| row.slot != lost.slot)
            .map(render_row)
            .collect();
        let found: Vec<String> = rows(&events).into_iter().map(render_row).collect();
        ensure(found == want, || {
            format!(
                "{file}: without the blocks_meta of slot {} its rows must vanish and no other row change",
                lost.slot
            )
        })?;
        let want_ticks: Vec<u64> = ticks(&baseline)
            .iter()
            .map(|tick| tick.slot)
            .filter(|&slot| slot != lost.slot)
            .collect();
        let found_ticks: Vec<u64> = ticks(&events).iter().map(|tick| tick.slot).collect();
        ensure(found_ticks == want_ticks, || {
            format!("{file}: a bank without its blocks_meta must not tick")
        })?;
        let want_gap = ExpectGap {
            prev_slot: prev.slot,
            prev_height: prev.block_height,
            next_slot: next.slot,
            next_height: next.block_height,
        };
        let found_gaps: Vec<ExpectGap> = gaps(&events).iter().map(expect_gap).collect();
        ensure(found_gaps == [want_gap], || {
            format!("{file}: gaps {found_gaps:?}, the unsealed bank leaves {want_gap:?}")
        })?;
        return ensure(metrics.abandoned_banks == 1, || {
            format!(
                "{file}: {} abandoned banks, the unsealed bank of slot {} is one",
                metrics.abandoned_banks, lost.slot
            )
        });
    }
    fail("no capture holds a bank with rows between two covered banks".into())
}

fn reorder_candidate(
    desc: &VenueConformance,
    fixtures: &'static [StreamFixture],
) -> Result<(&'static StreamFixture, Arc<Capture>, [usize; 4]), Outcome> {
    let config = TransactionsConfig::default();
    for fixture in fixtures.iter().filter(|fixture| fixture.gaps.is_empty()) {
        let capture = Capture::load(fixture.file)?;
        let address = watched(desc, fixture)?;
        let banks = &capture.banks;
        for c in 2..banks.len() {
            let (g, p) = (c - 2, c - 1);
            let chained = [g, p, c]
                .iter()
                .all(|&i| banks[i].covered && banks[i].segment == banks[c].segment)
                && banks[c].parent_slot == banks[p].slot
                && banks[p].parent_slot == banks[g].slot;
            if !chained
                || capture.kept_in(&banks[p], &address, &config).is_empty()
                || capture.kept_in(&banks[c], &address, &config).is_empty()
            {
                continue;
            }
            let Some(j) = (c + 1..banks.len()).find(|&j| {
                banks[j].segment == banks[c].segment
                    && banks[j].slot >= banks[c].slot + DEFAULT_REORDER_WINDOW_SLOTS
            }) else {
                continue;
            };
            return Ok((fixture, capture.clone(), [g, p, c, j]));
        }
    }
    fail("no capture holds a parent and a child bank with rows and a reorder window after them".into())
}

pub fn slot_reorder(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        let (fixture, capture, [g, p, c, j]) = reorder_candidate(desc, fixtures)?;
        let file = fixture.file;
        let config = TransactionsConfig::default();
        let watch = WatchedAddress::new(watched(desc, fixture)?, &config);
        let (grand, parent, child) =
            (&capture.banks[g], &capture.banks[p], &capture.banks[c]);
        let (baseline, _) = replay(desc, fixture, &capture.frames, config.clone())?;
        heights_rise(file, &baseline)?;

        let early = moved(
            &capture.identity(),
            &capture.bank_frames(child),
            capture.bank_frames(parent)[0],
            false,
        );
        let (events, _) =
            replay(desc, fixture, &capture.arranged(&early), config.clone())?;
        ensure(render(&events) == render(&baseline), || {
            format!(
                "{file}: with bank {} sent before its parent {} the release must equal the captured order",
                child.slot, parent.slot
            )
        })?;
        heights_rise(file, &events)?;
        let verdicts = slotbook_verdicts(&capture, &early, &watch)?;
        let held = verdict_at(&early, &verdicts, child.meta, false);
        ensure(
            held == Some(&Verdict::Seal(SlotApply::Held, Vec::new())),
            || {
                format!(
                    "{file}: the early child {} sealed {held:?}, not Held",
                    child.slot
                )
            },
        )?;
        let accepted = verdict_at(&early, &verdicts, parent.meta, false);
        let want = Verdict::Seal(
            SlotApply::Finalized,
            vec![
                (parent.slot, SlotApply::Finalized),
                (child.slot, SlotApply::Finalized),
            ],
        );
        ensure(accepted == Some(&want), || {
            format!(
                "{file}: the late parent {} sealed {accepted:?}, it must release itself then its child",
                parent.slot
            )
        })?;

        let expiry = &capture.banks[j];
        let late = moved(
            &capture.identity(),
            &capture.bank_frames(parent),
            expiry.meta,
            true,
        );
        let (events, _) =
            replay(desc, fixture, &capture.arranged(&late), config.clone())?;
        heights_rise(file, &events)?;
        let want_rows: Vec<String> = rows(&baseline)
            .into_iter()
            .filter(|row| row.slot != parent.slot)
            .map(render_row)
            .collect();
        let found_rows: Vec<String> = rows(&events).into_iter().map(render_row).collect();
        ensure(found_rows == want_rows, || {
            format!(
                "{file}: a parent {} past the reorder window must yield no row and leave the rest unchanged",
                parent.slot
            )
        })?;
        let want_ticks: Vec<u64> = ticks(&baseline)
            .iter()
            .map(|tick| tick.slot)
            .filter(|&slot| slot != parent.slot)
            .collect();
        let found_ticks: Vec<u64> = ticks(&events).iter().map(|tick| tick.slot).collect();
        ensure(found_ticks == want_ticks, || {
            format!("{file}: ticks {found_ticks:?}, expected {want_ticks:?}")
        })?;
        let want_gap = ExpectGap {
            prev_slot: grand.slot,
            prev_height: grand.block_height,
            next_slot: child.slot,
            next_height: child.block_height,
        };
        let found_gaps: Vec<ExpectGap> = gaps(&events).iter().map(expect_gap).collect();
        ensure(found_gaps == [want_gap], || {
            format!("{file}: gaps {found_gaps:?}, the expired window leaves {want_gap:?}")
        })?;
        let verdicts = slotbook_verdicts(&capture, &late, &watch)?;
        let released = match verdict_at(&late, &verdicts, expiry.meta, false) {
            Some(Verdict::Seal(_, released)) => released.first().copied(),
            _ => None,
        };
        let lost = child.block_height - grand.block_height - 1;
        ensure(
            released
                == Some((child.slot, SlotApply::GappedFinalized { blocks_lost: lost })),
            || {
                format!(
                    "{file}: at the window's end the child {} released as {released:?}",
                    child.slot
                )
            },
        )?;
        let stale = verdict_at(&late, &verdicts, parent.meta, false);
        ensure(
            stale == Some(&Verdict::Seal(SlotApply::Duplicate, Vec::new())),
            || {
                format!(
                    "{file}: the expired parent {} sealed {stale:?}",
                    parent.slot
                )
            },
        )?;
        for &frame in &parent.txs {
            if let Some(found) = verdict_at(&late, &verdicts, frame, false) {
                ensure(found == &Verdict::Tx(TxApply::Duplicate), || {
                    format!(
                        "{file}: a transaction of the expired parent applied {found:?}"
                    )
                })?;
            }
        }
        Ok(())
    })
}

pub fn tx_dedup(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        let mut overlaps = 0usize;
        for fixture in fixtures {
            let file = fixture.file;
            let capture = Capture::load(file)?;
            let address = watched(desc, fixture)?;
            let config = TransactionsConfig::default();
            let watch = WatchedAddress::new(address, &config);
            let (baseline, _) = replay(desc, fixture, &capture.frames, config.clone())?;
            unique_rows(file, &rows(&baseline))?;

            let doubled: Vec<usize> = capture
                .identity()
                .into_iter()
                .flat_map(|i| match capture.frames[i] {
                    ReplayFrame::Update { .. } => vec![i, i],
                    ReplayFrame::Reconnect { .. } => vec![i],
                })
                .collect();
            let (events, _) =
                replay(desc, fixture, &capture.arranged(&doubled), config.clone())?;
            ensure(render(&events) == render(&baseline), || {
                format!(
                    "{file}: every update delivered twice must replay like the capture"
                )
            })?;
            let verdicts = slotbook_verdicts(&capture, &doubled, &watch)?;
            for bank in capture.banks.iter().filter(|bank| bank.covered) {
                let again = verdict_at(&doubled, &verdicts, bank.meta, true);
                ensure(
                    matches!(again, Some(Verdict::Seal(SlotApply::Duplicate, _))),
                    || {
                        format!(
                            "{file}: the repeated blocks_meta of slot {} applied {again:?}",
                            bank.slot
                        )
                    },
                )?;
                for &frame in &bank.txs {
                    if let Some(found) = verdict_at(&doubled, &verdicts, frame, true) {
                        ensure(found == &Verdict::Tx(TxApply::Duplicate), || {
                            format!(
                                "{file}: a repeated transaction of slot {} applied {found:?}",
                                bank.slot
                            )
                        })?;
                    }
                }
            }

            for marker in capture.markers() {
                overlaps += 1;
                let segment = capture
                    .banks
                    .iter()
                    .filter(|bank| bank.meta < marker)
                    .map(|bank| bank.segment)
                    .max()
                    .unwrap_or(0);
                let before: Vec<&Bank> = capture
                    .banks
                    .iter()
                    .filter(|bank| bank.segment == segment && bank.meta < marker)
                    .collect();
                let Some(last) = before.iter().rev().find(|bank| bank.covered) else {
                    return fail(format!(
                        "{file}: no bank released before the reconnect"
                    ));
                };
                let Some(first) = before
                    .iter()
                    .rev()
                    .find(|bank| bank.slot + COVERAGE_MARGIN_SLOTS <= last.slot)
                else {
                    return fail(format!(
                        "{file}: the segment before the reconnect is too short to re-send"
                    ));
                };
                let resent: Vec<usize> = before
                    .iter()
                    .filter(|bank| bank.slot >= first.slot && bank.slot <= last.slot)
                    .flat_map(|bank| capture.bank_frames(bank))
                    .collect();
                let order: Vec<usize> = capture
                    .identity()
                    .into_iter()
                    .flat_map(|i| {
                        if i == marker {
                            std::iter::once(i).chain(resent.iter().copied()).collect()
                        } else {
                            vec![i]
                        }
                    })
                    .collect();
                let (events, _) =
                    replay(desc, fixture, &capture.arranged(&order), config.clone())?;
                unique_rows(file, &rows(&events))?;
                heights_rise(file, &events)?;
                let settled = |events: &[DomainEvent]| -> Vec<String> {
                    rows(events)
                        .into_iter()
                        .filter(|row| row.slot <= last.slot)
                        .map(render_row)
                        .collect()
                };
                ensure(settled(&events) == settled(&baseline), || {
                    format!(
                        "{file}: banks re-sent after the reconnect up to slot {} must add no row",
                        last.slot
                    )
                })?;
                let verdicts = slotbook_verdicts(&capture, &order, &watch)?;
                let again = verdict_at(&order, &verdicts, last.meta, true);
                ensure(
                    matches!(again, Some(Verdict::Seal(SlotApply::Duplicate, _))),
                    || {
                        format!(
                            "{file}: the re-sent last released bank {} applied {again:?}, not Duplicate",
                            last.slot
                        )
                    },
                )?;
                for &frame in &last.txs {
                    if let Some(found) = verdict_at(&order, &verdicts, frame, true) {
                        ensure(found == &Verdict::Tx(TxApply::Duplicate), || {
                            format!(
                                "{file}: a re-sent transaction of slot {} applied {found:?}",
                                last.slot
                            )
                        })?;
                    }
                }
            }
        }
        ensure(overlaps > 0, || {
            "the stream fixtures hold no reconnect".into()
        })
    })
}

pub fn slot_gap_detection(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        let mut reconnects = 0usize;
        for fixture in fixtures {
            let file = fixture.file;
            let address = watched(desc, fixture)?;
            request_without_replay(file, fixture, &address)?;
            let capture = Capture::load(file)?;
            let markers = capture.markers().len();
            reconnects += markers;
            let (events, _) = replay(
                desc,
                fixture,
                &capture.frames,
                TransactionsConfig::default(),
            )?;
            let found: Vec<SlotGap> = gaps(&events);
            let shapes: Vec<ExpectGap> = found.iter().map(expect_gap).collect();
            ensure(shapes == fixture.gaps, || {
                format!(
                    "{file}: gaps {shapes:?}, the descriptor expects {:?}",
                    fixture.gaps
                )
            })?;
            let lost = events
                .iter()
                .filter(|event| matches!(event, DomainEvent::ConnectionLost { .. }))
                .count();
            let resumed = events
                .iter()
                .filter(|event| matches!(event, DomainEvent::ConnectionResumed))
                .count();
            ensure(
                lost == markers && resumed == markers && found.len() == markers,
                || {
                    format!(
                        "{file}: {markers} reconnects gave {lost} losses, {resumed} resumes and {} gaps",
                        found.len()
                    )
                },
            )?;
            for (position, event) in events.iter().enumerate() {
                let DomainEvent::SlotGap(gap) = event else {
                    continue;
                };
                ensure(
                    gap.blocks_lost() == gap.next_height - gap.prev_height - 1,
                    || format!("{file}: blocks_lost {} for {gap:?}", gap.blocks_lost()),
                )?;
                let before = ticks(&events[..position]).last().copied();
                let after = ticks(&events[position..]).first().copied();
                ensure(
                    before.map(|t| (t.slot, t.block_height, t.block_time_us))
                        == Some((gap.prev_slot, gap.prev_height, gap.prev_block_time_us))
                        && after.map(|t| (t.slot, t.block_height, t.block_time_us))
                            == Some((
                                gap.next_slot,
                                gap.next_height,
                                gap.next_block_time_us,
                            )),
                    || {
                        format!(
                            "{file}: {gap:?} is not bounded by the released banks {before:?} and {after:?}"
                        )
                    },
                )?;
                let resumed_at = events[..position]
                    .iter()
                    .rposition(|event| matches!(event, DomainEvent::ConnectionResumed));
                let previous_tick = events[..position]
                    .iter()
                    .rposition(|event| matches!(event, DomainEvent::SlotTick(_)));
                ensure(resumed_at > previous_tick, || {
                    format!("{file}: {gap:?} is not preceded by a resume")
                })?;
            }
            ledger_rows(desc, fixture, events, &found)?;
        }
        ensure(reconnects > 0, || {
            "the stream fixtures hold no reconnect".into()
        })
    })
}

fn request_without_replay(
    file: &str,
    fixture: &StreamFixture,
    address: &ChainAddress,
) -> Checked {
    let section: GrpcSection = toml::from_str(&format!(
        "endpoint = \"{PROVIDER_ENDPOINT}\"\ncommitment = \"{}\"",
        fixture.commitment
    ))
    .map_err(|e| Outcome::Fail(format!("{file}: grpc section: {e}")))?;
    let watch = WatchedAddress::new(*address, &TransactionsConfig::default());
    let request = StreamFilter::new([&watch], &section).request();
    let filter = request.transactions.get(TRANSACTIONS_FILTER);
    ensure(
        request.from_slot.is_none()
            && request.ping.is_none()
            && request.commitment == Some(commitment_level(fixture.commitment) as i32)
            && !request.blocks_meta.is_empty()
            && filter.is_some_and(|filter| {
                filter.vote == Some(false)
                    && filter.account_include == [fixture.watched.to_string()]
            }),
        || format!("{file}: the subscribe request {request:?}"),
    )?;
    let ping = ping_request(1);
    ensure(
        ping.from_slot.is_none() && ping.transactions.is_empty(),
        || format!("{file}: the ping request {ping:?}"),
    )
}

fn ledger_rows(
    desc: &VenueConformance,
    fixture: &StreamFixture,
    events: Vec<DomainEvent>,
    found: &[SlotGap],
) -> Checked {
    let file = fixture.file;
    let dir = tempfile::tempdir()
        .map_err(|e| Outcome::Fail(format!("{file}: temporary directory: {e}")))?;
    let ledger = GapLedger::new(dir.path(), desc.venue, fixture.watched);
    let mut append_errors = Vec::new();
    let mut forwarded = Vec::new();
    let (outcome, metrics) = run_stream_runtime(
        desc,
        vec![fixture.watched.to_string()],
        events,
        DeclaredSet::only(DeclaredDatatype::Transactions),
        None,
        |event| {
            if let ReconstructedEvent::SlotGap(gap) = event {
                let incident =
                    GapIncident::from_slot_gap(&gap, desc.venue, fixture.watched);
                if let Err(e) = ledger.append(&incident) {
                    append_errors.push(e.to_string());
                }
                forwarded.push(gap);
            }
        },
    )
    .map_err(|o| located(file, o))?;
    ensure(
        outcome == RuntimeOutcome::Finished && append_errors.is_empty(),
        || format!("{file}: runtime ended {outcome:?}, ledger errors {append_errors:?}"),
    )?;
    let blocks: u64 = found.iter().map(SlotGap::blocks_lost).sum();
    ensure(
        forwarded == found
            && metrics.slot_gaps == found.len() as u64
            && metrics.blocks_lost == blocks,
        || {
            format!(
                "{file}: the runtime forwarded {} gaps and counted {} gaps, {} blocks lost",
                forwarded.len(),
                metrics.slot_gaps,
                metrics.blocks_lost
            )
        },
    )?;
    if found.is_empty() {
        return ensure(!ledger.path().exists(), || {
            format!("{file}: a gap ledger without a gap")
        });
    }
    let text = std::fs::read_to_string(ledger.path())
        .map_err(|e| Outcome::Fail(format!("{file}: gap ledger unreadable: {e}")))?;
    let incidents: Vec<GapIncident> = text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()
        .map_err(|e| Outcome::Fail(format!("{file}: gap ledger line: {e}")))?;
    ensure(incidents.len() == found.len(), || {
        format!(
            "{file}: {} ledger rows for {} gaps",
            incidents.len(),
            found.len()
        )
    })?;
    for (incident, gap) in incidents.iter().zip(found) {
        ensure(
            incident.cause == GapCause::SlotGap
                && incident.exchange == desc.venue
                && incident.symbol == fixture.watched
                && incident.possible_dropped_trades.is_none()
                && incident.prev_slot == Some(gap.prev_slot)
                && incident.prev_height == Some(gap.prev_height)
                && incident.next_slot == Some(gap.next_slot)
                && incident.next_height == Some(gap.next_height)
                && incident.blocks_lost == Some(gap.blocks_lost())
                && incident.opened_epoch_us == gap.prev_block_time_us
                && incident.closed_epoch_us == gap.next_block_time_us,
            || format!("{file}: ledger row {incident:?} for {gap:?}"),
        )?;
    }
    Ok(())
}

struct Grid {
    rows: Vec<(u64, Vec<String>)>,
    closing: Vec<(u64, Vec<String>)>,
    snapshots: Vec<MarketSnapshot>,
    late: u64,
}

fn signatures_by_row(snapshots: &[MarketSnapshot]) -> Vec<(u64, Vec<String>)> {
    snapshots
        .iter()
        .map(|snapshot| {
            (
                snapshot.ts_us,
                snapshot
                    .transactions
                    .iter()
                    .map(|tx| tx.signature.clone())
                    .collect(),
            )
        })
        .collect()
}

fn grid(
    desc: &VenueConformance,
    fixture: &StreamFixture,
    capture: &Capture,
    delay_us: u64,
) -> Result<Grid, Outcome> {
    let (events, _) = replay(
        desc,
        fixture,
        &capture.frames,
        TransactionsConfig::default(),
    )?;
    let mut sync = MarketSynchronizer::slot_driven(GRID_PERIOD_US);
    sync.set_emission_delay_us(delay_us);
    let (outcome, _) = run_stream_runtime(
        desc,
        vec![fixture.watched.to_string()],
        events,
        DeclaredSet::only(DeclaredDatatype::Transactions),
        None,
        |event| match event {
            ReconstructedEvent::Transaction(tx) => sync.on_transaction(*tx),
            ReconstructedEvent::SlotTick(tick) => {
                sync.on_slot_tick(tick.block_time_us);
            }
            _ => {}
        },
    )
    .map_err(|o| located(fixture.file, o))?;
    ensure(outcome == RuntimeOutcome::Finished, || {
        format!("{}: runtime ended {outcome:?}", fixture.file)
    })?;
    let mut snapshots = sync.drain();
    sync.finalize();
    let closing = sync.drain();
    let rows = signatures_by_row(&snapshots);
    let closing_rows = signatures_by_row(&closing);
    snapshots.extend(closing);
    Ok(Grid {
        rows,
        closing: closing_rows,
        snapshots,
        late: sync.late_events_dropped,
    })
}

pub fn slot_tick_grid(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        let delay_us = registry()
            .get(desc.venue)
            .and_then(|adapter| adapter.default_emission_delay_us())
            .unwrap_or(0);
        let mut quiet = 0usize;
        for fixture in fixtures {
            let file = fixture.file;
            let capture = Capture::load(file)?;
            let first = grid(desc, fixture, &capture, delay_us)?;
            let second = grid(desc, fixture, &capture, delay_us)?;
            ensure(
                first.rows == second.rows && first.closing == second.closing,
                || format!("{file}: two replays of the capture built different grids"),
            )?;
            ensure(first.late == 0, || {
                format!(
                    "{file}: {} transactions arrived after their row",
                    first.late
                )
            })?;
            ensure(fixture.ticks == 0 || !first.snapshots.is_empty(), || {
                format!("{file}: {} SlotTicks anchored no grid", fixture.ticks)
            })?;
            let stamps: Vec<u64> = first.rows.iter().map(|(ts, _)| *ts).collect();
            ensure(
                stamps.iter().all(|ts| ts % GRID_PERIOD_US == 0)
                    && stamps.windows(2).all(|pair| pair[0] < pair[1]),
                || format!("{file}: grid rows {stamps:?} are not aligned and rising"),
            )?;
            for snapshot in first.snapshots.iter().take(first.rows.len()) {
                for tx in &snapshot.transactions {
                    ensure(
                        tx.source_tx_ts_us / GRID_PERIOD_US + 1
                            == snapshot.ts_us / GRID_PERIOD_US,
                        || {
                            format!(
                                "{file}: {} at {} landed in the row of {}",
                                tx.signature, tx.source_tx_ts_us, snapshot.ts_us
                            )
                        },
                    )?;
                }
            }
            let placed: usize = first
                .snapshots
                .iter()
                .map(|snapshot| snapshot.transactions.len())
                .sum();
            ensure(placed == fixture.rows, || {
                format!(
                    "{file}: the grid holds {placed} transactions, the replay {}",
                    fixture.rows
                )
            })?;
            if fixture.rows == 0 && fixture.ticks > 0 {
                quiet += 1;
                ensure(
                    decompose_snapshots(&first.snapshots)
                        .transactions
                        .is_empty(),
                    || format!("{file}: a quiet address decomposes to transaction rows"),
                )?;
            }
        }
        ensure(quiet > 0, || {
            "the stream fixtures hold no quiet address".into()
        })
    })
}

pub fn shared_connection_fanout(desc: &VenueConformance) -> Outcome {
    match stream_fixtures(desc) {
        Ok(_) => Outcome::NotApplicable(
            "a property of the live shared gRPC connection (attach set, per-attach queues, \
             one conn_epoch_us per physical connection): offline replay feeds one consumer, \
             and the geyser mock tests certify it"
                .into(),
        ),
        Err(outcome) => outcome,
    }
}

fn without_receipt(row: &Transaction) -> Transaction {
    Transaction {
        local_tx_ts_us: 0,
        source_tx_rtt_us: 0,
        recv_seq: 0,
        conn_epoch_us: 0,
        ..row.clone()
    }
}

fn roles_across(
    desc: &VenueConformance,
    fixture: &StreamFixture,
    capture: &Capture,
    seen: &mut BTreeSet<MatchRole>,
) -> Checked {
    let file = fixture.file;
    let ExpectInstrument::Address(chain) = desc.expect_instrument else {
        return fail(format!(
            "stream venue {} expects no address instrument",
            desc.venue
        ));
    };
    let covered: Vec<&WireTx> = capture
        .banks
        .iter()
        .filter(|bank| bank.covered)
        .flat_map(|bank| capture.bank_txs(bank))
        .collect();
    let mut chosen = BTreeSet::new();
    for tx in &covered {
        let Some(message) = message(&tx.info) else {
            continue;
        };
        let second_signer = (signer_count(message) > 1)
            .then(|| message.account_keys.get(1))
            .flatten();
        for key in [
            message.account_keys.first(),
            second_signer,
            message.account_keys.last(),
            loaded(&tx.info).next(),
        ]
        .into_iter()
        .flatten()
        {
            chosen.insert(encode_base58(key));
        }
    }
    let symbols: Vec<String> = chosen.into_iter().collect();
    let addresses: Vec<ChainAddress> = symbols
        .iter()
        .map(|symbol| ChainAddress::parse(chain, symbol))
        .collect::<Result<_, _>>()
        .map_err(|e| {
            Outcome::Fail(format!("{file}: a captured key does not parse: {e}"))
        })?;
    for required in [
        MatchRole::AccountKey,
        MatchRole::Signer,
        MatchRole::FeePayer,
    ] {
        let (events, _) = replay_stream_frames(
            desc,
            &symbols,
            fixture.commitment,
            &capture.frames,
            watching(required, true, false),
        )
        .map_err(|o| located(file, o))?;
        let found: BTreeSet<(String, String, MatchRole)> = rows(&events)
            .iter()
            .map(|row| {
                (
                    row.signature.clone(),
                    row.address.to_base58(),
                    row.match_role,
                )
            })
            .collect();
        let want: BTreeSet<(String, String, MatchRole)> = covered
            .iter()
            .filter(|tx| !tx.info.is_vote)
            .flat_map(|tx| {
                addresses.iter().filter_map(|address| {
                    role(&tx.info, address)
                        .filter(|role| role.satisfies(required))
                        .map(|role| (tx.signature.clone(), address.to_base58(), role))
                })
            })
            .collect();
        ensure(found == want, || {
            format!(
                "{file}: watching {} captured keys with match {required} gave {} rows, the updates name {}",
                symbols.len(),
                found.len(),
                want.len()
            )
        })?;
        unique_rows(file, &rows(&events))?;
        if required == MatchRole::AccountKey {
            seen.extend(found.iter().map(|(_, _, role)| *role));
        }
    }
    Ok(())
}

pub fn address_match_roles(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        let mut table_only = 0usize;
        let mut seen = BTreeSet::new();
        for fixture in fixtures {
            let file = fixture.file;
            let capture = Capture::load(file)?;
            let address = watched(desc, fixture)?;
            let unfiltered = fixtures.iter().any(|other| other.unfiltered == Some(file));
            let (events, metrics) = replay(
                desc,
                fixture,
                &capture.frames,
                TransactionsConfig::default(),
            )?;
            if !unfiltered {
                ensure(
                    metrics.filtered_out == 0 && metrics.votes_excluded == 0,
                    || {
                        format!(
                            "{file}: stage 2 dropped {} transactions and {} votes the server filter delivered",
                            metrics.filtered_out, metrics.votes_excluded
                        )
                    },
                )?;
            }
            let program = capture.txs().any(|tx| invokes(&tx.info, &address));
            let replayed = rows(&events);
            for row in &replayed {
                let tx = capture
                    .by_signature(&row.signature)
                    .map_err(|o| located(file, o))?;
                let want = role(&tx.info, &address);
                ensure(
                    row.address == address && Some(row.match_role) == want,
                    || {
                        format!(
                            "{file}: {} matched {} as {}, the update gives {want:?}",
                            row.signature, row.address, row.match_role
                        )
                    },
                )?;
                ensure(row.account_keys == account_keys(&tx.info), || {
                    format!(
                        "{file}: {} account_keys out of static, writable, readonly order",
                        row.signature
                    )
                })?;
                ensure(row.signers == signers(&tx.info), || {
                    format!(
                        "{file}: {} signers differ from the header's signer keys",
                        row.signature
                    )
                })?;
                ensure(!program || row.match_role == MatchRole::AccountKey, || {
                    format!(
                        "{file}: the program {address} holds role {} in {}",
                        row.match_role, row.signature
                    )
                })?;
                let in_static = message(&tx.info).is_some_and(|message| {
                    message
                        .account_keys
                        .iter()
                        .any(|key| key.as_slice() == address.pubkey().as_slice())
                });
                table_only += usize::from(!in_static);
                seen.insert(row.match_role);
            }
            for required in [MatchRole::Signer, MatchRole::FeePayer] {
                let (narrowed, _) = replay(
                    desc,
                    fixture,
                    &capture.frames,
                    watching(required, true, false),
                )?;
                let want: Vec<&str> = replayed
                    .iter()
                    .filter(|row| row.match_role.satisfies(required))
                    .map(|row| row.signature.as_str())
                    .collect();
                let found: Vec<String> = rows(&narrowed)
                    .into_iter()
                    .map(|row| row.signature.clone())
                    .collect();
                ensure(found == want, || {
                    format!(
                        "{file}: match {required} kept {found:?}, the roles admit {want:?}"
                    )
                })?;
            }
            if let Some(wide) = fixture.unfiltered {
                let wide_capture = Capture::load(wide)?;
                let (selected, _) = replay(
                    desc,
                    fixture,
                    &wide_capture.frames,
                    TransactionsConfig::default(),
                )?;
                let found: Vec<Transaction> =
                    rows(&selected).into_iter().map(without_receipt).collect();
                let want: Vec<Transaction> =
                    replayed.iter().map(|row| without_receipt(row)).collect();
                ensure(found == want, || {
                    format!(
                        "{file}: the unfiltered capture {wide} selects {} rows for {}, the server filter delivered {}",
                        found.len(),
                        fixture.watched,
                        want.len()
                    )
                })?;
            }
            if unfiltered {
                roles_across(desc, fixture, &capture, &mut seen)?;
            }
        }
        ensure(table_only > 0, || {
            "no row matches its address through an address lookup table only".into()
        })?;
        ensure(seen.len() == 3, || {
            format!("the replayed rows hold only the roles {seen:?}")
        })
    })
}

pub fn failed_inclusion(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        let mut failures = 0usize;
        for fixture in fixtures {
            let file = fixture.file;
            let address = watched(desc, fixture)?;
            let section: GrpcSection = toml::from_str(&format!(
                "endpoint = \"{PROVIDER_ENDPOINT}\"\ncommitment = \"{}\"",
                fixture.commitment
            ))
            .map_err(|e| Outcome::Fail(format!("{file}: grpc section: {e}")))?;
            for include_failed in [true, false] {
                let watch = WatchedAddress::new(
                    address,
                    &watching(MatchRole::AccountKey, include_failed, false),
                );
                let request = StreamFilter::new([&watch], &section).request();
                let sent = request
                    .transactions
                    .get(TRANSACTIONS_FILTER)
                    .map(|filter| filter.failed);
                ensure(sent == Some((!include_failed).then_some(false)), || {
                    format!(
                        "{file}: include_failed = {include_failed} sends failed = {sent:?}"
                    )
                })?;
            }
            let capture = Capture::load(file)?;
            let (all, all_metrics) = replay(
                desc,
                fixture,
                &capture.frames,
                TransactionsConfig::default(),
            )?;
            let (kept, kept_metrics) = replay(
                desc,
                fixture,
                &capture.frames,
                watching(MatchRole::AccountKey, false, false),
            )?;
            let all_rows = rows(&all);
            let config = TransactionsConfig::default();
            let want_failed = capture
                .banks
                .iter()
                .filter(|bank| bank.covered)
                .flat_map(|bank| capture.kept_in(bank, &address, &config))
                .filter(|tx| failed(&tx.info))
                .count();
            let found_failed = all_rows.iter().filter(|row| !row.success).count();
            ensure(found_failed == want_failed, || {
                format!(
                    "{file}: {found_failed} failed rows, the updates hold {want_failed}"
                )
            })?;
            for row in &all_rows {
                ensure(
                    row.success == row.failure.is_none()
                        && row
                            .failure
                            .as_ref()
                            .is_none_or(|failure| !failure.error.is_empty()),
                    || {
                        format!(
                            "{file}: {} failure columns against success {}",
                            row.signature, row.success
                        )
                    },
                )?;
            }
            let successful: Vec<&Transaction> =
                all_rows.iter().copied().filter(|row| row.success).collect();
            ensure(rows(&kept) == successful, || {
                format!(
                    "{file}: include_failed = false must keep exactly the successful rows"
                )
            })?;
            let dropped = capture
                .txs()
                .filter(|tx| {
                    !tx.info.is_vote
                        && failed(&tx.info)
                        && role(&tx.info, &address).is_some()
                })
                .count() as u64;
            ensure(
                kept_metrics.filtered_out == all_metrics.filtered_out + dropped,
                || {
                    format!(
                        "{file}: filtered_out {} against {} plus {dropped} failed",
                        kept_metrics.filtered_out, all_metrics.filtered_out
                    )
                },
            )?;
            failures += found_failed;
        }
        ensure(failures > 0, || {
            "the stream fixtures hold no failed transaction".into()
        })
    })
}

pub fn tx_invariants(desc: &VenueConformance) -> Outcome {
    outcome_of(desc, |desc, fixtures| {
        let dir = tempfile::tempdir()
            .map_err(|e| Outcome::Fail(format!("temporary directory: {e}")))?;
        for (i, fixture) in fixtures.iter().enumerate() {
            let file = fixture.file;
            let capture = Capture::load(file)?;
            let (events, _) = replay(
                desc,
                fixture,
                &capture.frames,
                TransactionsConfig::default(),
            )?;
            let rows: Vec<Transaction> = rows(&events).into_iter().cloned().collect();
            for row in &rows {
                ensure(row.check_invariants().is_ok(), || {
                    format!(
                        "{file}: {} breaks {:?}",
                        row.signature,
                        row.check_invariants()
                    )
                })?;
            }
            let checks = check_transactions(&rows);
            ensure(checks.passed() && checks.rows == rows.len() as u64, || {
                format!("{file}: {checks:?}")
            })?;
            if rows.is_empty() {
                continue;
            }
            let path = dir.path().join(format!("{i}.parquet"));
            write_transactions_parquet(&rows, &path).map_err(|e| {
                Outcome::Fail(format!("{file}: transactions.v1 write: {e}"))
            })?;
            let read = read_transactions_parquet(&path).map_err(|e| {
                Outcome::Fail(format!("{file}: transactions.v1 read: {e}"))
            })?;
            ensure(read == rows, || {
                format!("{file}: the transactions.v1 file reads back other rows")
            })?;
            let reread = check_transactions(&read);
            ensure(reread.passed(), || {
                format!("{file}: the file fails {reread:?}")
            })?;
        }
        Ok(())
    })
}

#[test]
fn stream_frame_lines_parse_strictly() {
    assert_eq!(
        stream_frame(r#"{"t_us":7,"upd":"CgA="}"#),
        Ok(ReplayFrame::Update {
            t_us: 7,
            bytes: vec![0x0a, 0x00],
        })
    );
    assert_eq!(
        stream_frame(r#"{"t_us":9,"reconnect":true}"#),
        Ok(ReplayFrame::Reconnect { t_us: 9 })
    );
    for line in [
        r#"{"t_us":9,"reconnect":false}"#,
        r#"{"t_us":9}"#,
        r#"{"upd":"CgA="}"#,
        r#"{"t_us":9,"upd":"CgA=","reconnect":true}"#,
        r#"{"t_us":9,"upd":"CgA*"}"#,
        r#"{"t_us":9,"upd":"CgA=","slot":1}"#,
        r#"{"t_us":-1,"upd":"CgA="}"#,
        "not json",
    ] {
        assert!(stream_frame(line).is_err(), "{line}");
    }
}
