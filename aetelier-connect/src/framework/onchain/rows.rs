use aetelier_types::onchain::{
    ChainAddress, Commitment, MatchRole, Transaction, encode_base58,
};

use super::decoded::DecodedTransaction;
use super::watch::WatchedAddress;
use crate::framework::slotbook::BlockContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowContext<'a> {
    pub exchange: &'a str,
    pub commitment: Commitment,
}

pub fn transaction_rows<'f>(
    block: BlockContext<'_>,
    tx: &DecodedTransaction,
    watched: impl IntoIterator<Item = &'f WatchedAddress>,
    context: RowContext<'_>,
) -> Vec<Transaction> {
    let matched: Vec<(&WatchedAddress, MatchRole)> = watched
        .into_iter()
        .filter_map(|watch| watch.matches(tx).map(|role| (watch, role)))
        .collect();
    let Some(&(first, first_role)) = matched.first() else {
        return Vec::new();
    };
    let requested = tx.requested_compute();
    let shared = Transaction {
        exchange: context.exchange.to_string(),
        address: first.address,
        match_role: first_role,
        source_tx_ts_us: block.block_time_us,
        local_tx_ts_us: tx.receipt.local_tx_ts_us,
        source_tx_rtt_us: tx.receipt.source_tx_rtt_us,
        recv_seq: tx.receipt.recv_seq,
        conn_epoch_us: tx.receipt.conn_epoch_us,
        commitment: context.commitment,
        slot: block.slot,
        block_height: block.block_height,
        blockhash: block.blockhash.to_string(),
        tx_index: tx.tx_index,
        signature: encode_base58(&tx.signature),
        version: tx.version(),
        success: tx.succeeded(),
        fee_lamports: tx.fee_lamports,
        compute_units_consumed: tx.compute_units_consumed,
        compute_unit_limit: requested.compute_unit_limit,
        compute_unit_price_micro_lamports: requested.compute_unit_price_micro_lamports,
        priority_fee_lamports: requested.priority_fee_lamports,
        cost_units: tx.cost_units,
        signers: tx.signers().iter().map(ChainAddress::to_base58).collect(),
        account_keys: tx.account_keys().map(ChainAddress::to_base58).collect(),
        log_messages: None,
        failure: tx.failure(),
    };
    matched
        .into_iter()
        .map(|(watch, role)| {
            let row = Transaction {
                address: watch.address,
                match_role: role,
                log_messages: if watch.keep_logs {
                    tx.log_messages.clone()
                } else {
                    None
                },
                ..shared.clone()
            };
            debug_assert!(row.check_invariants().is_ok(), "{row:?}");
            row
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::framework::onchain::COMPUTE_BUDGET_PROGRAM_ID;
    use crate::framework::onchain::decoded::fixtures::{decoded, key};
    use crate::framework::onchain::decoded::{
        CompiledInstruction, DecodedError, ExecutionError, InstructionFault,
        MessageConfig,
    };
    use crate::framework::slotbook::{
        BlockMeta, CoverageStart, SlotBookConfig, SourcedSlotbook,
    };
    use aetelier_types::onchain::TransactionVersion;

    const BLOCKHASH: &str = "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn";

    fn block(slot: u64) -> BlockContext<'static> {
        BlockContext {
            slot,
            block_height: slot - 22_000_000,
            block_time_us: 1_791_321_067_000_000,
            blockhash: BLOCKHASH,
        }
    }

    fn context() -> RowContext<'static> {
        RowContext {
            exchange: "solana",
            commitment: Commitment::Finalized,
        }
    }

    fn watching(address: ChainAddress, required_role: MatchRole) -> WatchedAddress {
        WatchedAddress {
            address,
            required_role,
            include_failed: true,
            keep_logs: false,
        }
    }

    fn failed(mut tx: DecodedTransaction) -> DecodedTransaction {
        tx.error = Some(ExecutionError {
            raw: vec![8, 0, 0, 0, 0],
            decoded: Some(DecodedError {
                error: "InstructionError(0, Custom(1))".to_string(),
                instruction: Some(InstructionFault {
                    index: 0,
                    error: "Custom(1)".to_string(),
                }),
            }),
        });
        tx
    }

    #[test]
    fn one_row_per_matched_address_with_its_strongest_role() {
        let mut tx = decoded(4);
        tx.loaded_readonly = vec![key(6)];
        let watched = [
            watching(key(1), MatchRole::AccountKey),
            watching(key(2), MatchRole::Signer),
            watching(key(6), MatchRole::AccountKey),
            watching(key(6), MatchRole::Signer),
            watching(key(9), MatchRole::AccountKey),
        ];
        let rows = transaction_rows(block(380_000_123), &tx, &watched, context());
        let roles: Vec<(ChainAddress, MatchRole)> = rows
            .iter()
            .map(|row| (row.address, row.match_role))
            .collect();
        assert_eq!(
            roles,
            vec![
                (key(1), MatchRole::FeePayer),
                (key(2), MatchRole::Signer),
                (key(6), MatchRole::AccountKey),
            ]
        );
        assert!(rows.iter().all(|row| row.signature == rows[0].signature));
        assert_eq!(rows[0].signature, encode_base58(&tx.signature));
        assert_eq!(
            rows[0].signers,
            vec![key(1).to_base58(), key(2).to_base58()]
        );
        assert_eq!(rows[0].account_keys.len(), 5);
        assert_eq!(rows[0].account_keys[4], key(6).to_base58());
        assert!(transaction_rows(block(380_000_000), &tx, &[], context()).is_empty());
    }

    #[test]
    fn rows_carry_the_receipt_of_their_own_update_and_the_block_context() {
        let tx = decoded(9);
        let rows = transaction_rows(
            block(380_000_123),
            &tx,
            &[watching(key(3), MatchRole::AccountKey)],
            context(),
        );
        let row = &rows[0];
        assert_eq!(row.local_tx_ts_us, tx.receipt.local_tx_ts_us);
        assert_eq!(row.recv_seq, tx.receipt.recv_seq);
        assert_eq!(row.source_tx_rtt_us, 900);
        assert_eq!(row.conn_epoch_us, tx.receipt.conn_epoch_us);
        assert_eq!(row.source_tx_ts_us, 1_791_321_067_000_000);
        assert_eq!((row.slot, row.block_height), (380_000_123, 358_000_123));
        assert_eq!(row.blockhash, BLOCKHASH);
        assert_eq!(
            (row.exchange.as_str(), row.commitment),
            ("solana", Commitment::Finalized)
        );
        assert_eq!(row.tx_index, 9);
    }

    #[test]
    fn logs_are_stored_only_for_watches_that_keep_them() {
        let tx = decoded(0);
        let keep = WatchedAddress {
            keep_logs: true,
            ..watching(key(1), MatchRole::AccountKey)
        };
        let drop = watching(key(2), MatchRole::AccountKey);
        let rows = transaction_rows(block(380_000_000), &tx, &[keep, drop], context());
        assert_eq!(rows[0].log_messages, tx.log_messages);
        assert_eq!(rows[1].log_messages, None);
        let mut silent = decoded(1);
        silent.log_messages = None;
        let rows = transaction_rows(block(380_000_000), &silent, &[keep], context());
        assert_eq!(rows[0].log_messages, None);
    }

    #[test]
    fn rows_satisfy_the_record_invariants_for_every_message_version() {
        let mut legacy = decoded(0);
        legacy.static_keys.push(COMPUTE_BUDGET_PROGRAM_ID);
        legacy.instructions = vec![
            CompiledInstruction {
                program_id_index: 4,
                data: vec![2, 0x40, 0x0d, 0x03, 0x00],
            },
            CompiledInstruction {
                program_id_index: 4,
                data: vec![3, 0xe8, 0x03, 0, 0, 0, 0, 0, 0],
            },
        ];
        let mut v0 = failed(decoded(1));
        v0.versioned = true;
        v0.static_keys.truncate(3);
        v0.loaded_writable = vec![key(4)];
        v0.instructions = vec![CompiledInstruction {
            program_id_index: 2,
            data: vec![9, 9],
        }];
        let mut v1 = decoded(2);
        v1.versioned = true;
        v1.config = Some(MessageConfig {
            priority_fee: Some(12_000),
            compute_unit_limit: None,
        });
        v1.static_keys.push(COMPUTE_BUDGET_PROGRAM_ID);
        v1.instructions = legacy.instructions.clone();
        let watch = [watching(key(4), MatchRole::AccountKey)];
        let rows: Vec<Transaction> = [&legacy, &v0, &v1]
            .into_iter()
            .flat_map(|tx| transaction_rows(block(380_000_000), tx, &watch, context()))
            .collect();
        let shape: Vec<_> = rows
            .iter()
            .map(|row| {
                (
                    row.version,
                    row.compute_unit_limit,
                    row.compute_unit_price_micro_lamports,
                    row.priority_fee_lamports,
                    row.success,
                )
            })
            .collect();
        assert_eq!(
            shape,
            vec![
                (
                    TransactionVersion::Legacy,
                    Some(200_000),
                    Some(1_000),
                    None,
                    true
                ),
                (TransactionVersion::V0, None, None, None, false),
                (TransactionVersion::V1, Some(0), None, Some(12_000), true),
            ]
        );
        assert_eq!(
            rows[1].failure.as_ref().unwrap().program_id,
            Some(key(3).to_base58())
        );
        for row in &rows {
            assert_eq!(row.check_invariants(), Ok(()), "{row:?}");
        }
    }

    #[test]
    fn released_banks_yield_rows_in_slot_and_tx_index_order_per_address() {
        let mut book = SourcedSlotbook::new(SlotBookConfig::default());
        book.open_coverage(CoverageStart::AtSlot(380_000_000));
        let watch = [
            watching(key(1), MatchRole::AccountKey),
            watching(key(3), MatchRole::AccountKey),
        ];
        for (slot, indexes) in [(380_000_002u64, [7u64, 2]), (380_000_001, [5, 0])] {
            for tx_index in indexes {
                let mut tx = decoded(tx_index);
                tx.signature[8..16].copy_from_slice(&slot.to_le_bytes());
                book.on_transaction(slot, 0, tx_index, tx);
            }
        }
        let mut released = book
            .on_block_meta(BlockMeta {
                slot: 380_000_002,
                bank_id: 0,
                parent_slot: 380_000_001,
                block_height: Some(358_000_002),
                block_time_secs: Some(1_791_321_068),
                blockhash: BLOCKHASH.to_string(),
            })
            .released;
        released.extend(
            book.on_block_meta(BlockMeta {
                slot: 380_000_001,
                bank_id: 0,
                parent_slot: 379_999_999,
                block_height: Some(358_000_001),
                block_time_secs: Some(1_791_321_067),
                blockhash: BLOCKHASH.to_string(),
            })
            .released,
        );
        let rows: Vec<Transaction> = released
            .iter()
            .flat_map(|bank| {
                bank.txs
                    .iter()
                    .flat_map(|tx| transaction_rows(bank.block(), tx, &watch, context()))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(rows.len(), 8);
        let mut last: HashMap<ChainAddress, (u64, u64)> = HashMap::new();
        let mut keys = HashSet::new();
        for row in &rows {
            assert_eq!(row.check_invariants(), Ok(()));
            assert!(keys.insert(row.row_key()), "{:?} repeated", row.row_key());
            if let Some(previous) = last.insert(row.address, row.position()) {
                assert!(
                    previous < row.position(),
                    "{previous:?} then {:?}",
                    row.position()
                );
            }
        }
    }
}
