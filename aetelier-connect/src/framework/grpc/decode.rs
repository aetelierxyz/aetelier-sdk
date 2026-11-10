use aetelier_types::onchain::{Chain, ChainAddress, PUBKEY_LEN};
use solana_transaction_error::TransactionError;
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdateTransaction,
    SubscribeUpdateTransactionInfo, subscribe_update::UpdateOneof,
};

use crate::framework::onchain::{
    CompiledInstruction, DecodedError, DecodedTransaction, ExecutionError,
    InstructionFault, MessageConfig, MessageHeader, ReceiptStamp, SIGNATURE_LEN,
};
use crate::framework::slotbook::BlockMeta;

#[derive(Debug)]
pub enum GrpcUpdate {
    Transaction(TransactionUpdate),
    BlockMeta(BlockMeta),
    Ping,
    Pong { id: i32 },
    Unrequested(&'static str),
}

#[derive(Debug)]
pub struct TransactionUpdate {
    pub slot: u64,
    pub bank_id: u64,
    pub decoded: Result<DecodedTransaction, MalformedTransaction>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MalformedTransaction {
    #[error("transaction update carries no {0}")]
    Missing(&'static str),
    #[error("signature is {len} bytes, expected {}", SIGNATURE_LEN)]
    SignatureLength { len: usize },
    #[error("{list} key {index} is {len} bytes, expected {}", PUBKEY_LEN)]
    KeyLength {
        list: &'static str,
        index: usize,
        len: usize,
    },
}

pub fn decode_update(update: SubscribeUpdate, receipt: ReceiptStamp) -> GrpcUpdate {
    match update.update_oneof {
        Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction,
            slot,
            bank_id,
        })) => GrpcUpdate::Transaction(TransactionUpdate {
            slot,
            bank_id,
            decoded: transaction
                .ok_or(MalformedTransaction::Missing("transaction"))
                .and_then(|info| decode_transaction(info, receipt)),
        }),
        Some(UpdateOneof::BlockMeta(meta)) => GrpcUpdate::BlockMeta(block_meta(meta)),
        Some(UpdateOneof::Ping(_)) => GrpcUpdate::Ping,
        Some(UpdateOneof::Pong(pong)) => GrpcUpdate::Pong { id: pong.id },
        Some(UpdateOneof::Account(_)) => GrpcUpdate::Unrequested("account"),
        Some(UpdateOneof::Slot(_)) => GrpcUpdate::Unrequested("slot"),
        Some(UpdateOneof::TransactionStatus(_)) => {
            GrpcUpdate::Unrequested("transaction_status")
        }
        Some(UpdateOneof::Block(_)) => GrpcUpdate::Unrequested("block"),
        Some(UpdateOneof::Entry(_)) => GrpcUpdate::Unrequested("entry"),
        Some(UpdateOneof::BlockFooter(_)) => GrpcUpdate::Unrequested("block_footer"),
        Some(UpdateOneof::EntryUpdateParent(_)) => {
            GrpcUpdate::Unrequested("entry_update_parent")
        }
        None => GrpcUpdate::Unrequested("empty"),
    }
}

pub fn decode_transaction(
    info: SubscribeUpdateTransactionInfo,
    receipt: ReceiptStamp,
) -> Result<DecodedTransaction, MalformedTransaction> {
    let SubscribeUpdateTransactionInfo {
        signature,
        is_vote,
        transaction,
        meta,
        index,
    } = info;
    let signature = <[u8; SIGNATURE_LEN]>::try_from(signature)
        .map_err(|raw| MalformedTransaction::SignatureLength { len: raw.len() })?;
    let message = transaction
        .ok_or(MalformedTransaction::Missing("transaction.transaction"))?
        .message
        .ok_or(MalformedTransaction::Missing(
            "transaction.transaction.message",
        ))?;
    let header = message.header.ok_or(MalformedTransaction::Missing(
        "transaction.transaction.message.header",
    ))?;
    let meta = meta.ok_or(MalformedTransaction::Missing("transaction.meta"))?;
    Ok(DecodedTransaction {
        signature,
        is_vote,
        tx_index: index,
        versioned: message.versioned,
        config: message.config.map(|config| MessageConfig {
            priority_fee: config.priority_fee,
            compute_unit_limit: config.compute_unit_limit,
        }),
        header: MessageHeader {
            num_required_signatures: header.num_required_signatures,
            num_readonly_signed_accounts: header.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts,
        },
        static_keys: keys("account_keys", message.account_keys)?,
        loaded_writable: keys(
            "loaded_writable_addresses",
            meta.loaded_writable_addresses,
        )?,
        loaded_readonly: keys(
            "loaded_readonly_addresses",
            meta.loaded_readonly_addresses,
        )?,
        instructions: message
            .instructions
            .into_iter()
            .map(|instruction| CompiledInstruction {
                program_id_index: instruction.program_id_index,
                data: instruction.data,
            })
            .collect(),
        fee_lamports: meta.fee,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
        error: meta.err.map(|err| execution_error(err.err)),
        log_messages: (!meta.log_messages_none).then_some(meta.log_messages),
        receipt,
    })
}

pub fn block_meta(meta: SubscribeUpdateBlockMeta) -> BlockMeta {
    BlockMeta {
        slot: meta.slot,
        bank_id: meta.bank_id,
        parent_slot: meta.parent_slot,
        block_height: meta.block_height.map(|height| height.block_height),
        block_time_secs: meta.block_time.map(|time| time.timestamp),
        blockhash: meta.blockhash,
    }
}

pub fn execution_error(raw: Vec<u8>) -> ExecutionError {
    let decoded = wincode::deserialize_exact::<TransactionError>(&raw)
        .ok()
        .map(|error| DecodedError {
            instruction: match &error {
                TransactionError::InstructionError(index, fault) => {
                    Some(InstructionFault {
                        index: u32::from(*index),
                        error: format!("{fault:?}"),
                    })
                }
                _ => None,
            },
            error: format!("{error:?}"),
        });
    ExecutionError { raw, decoded }
}

fn keys(
    list: &'static str,
    raw: Vec<Vec<u8>>,
) -> Result<Vec<ChainAddress>, MalformedTransaction> {
    raw.into_iter()
        .enumerate()
        .map(|(index, bytes)| {
            <[u8; PUBKEY_LEN]>::try_from(bytes)
                .map(|pubkey| ChainAddress::new(Chain::Solana, pubkey))
                .map_err(|bytes| MalformedTransaction::KeyLength {
                    list,
                    index,
                    len: bytes.len(),
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::grpc::{BLOCKS_META_FILTER, TRANSACTIONS_FILTER};
    use crate::framework::onchain::decoded::fixtures::{decoded, key};
    use crate::framework::onchain::{
        COMPUTE_BUDGET_PROGRAM_ID, RequestedCompute, RowContext, WatchedAddress,
        transaction_rows,
    };
    use crate::framework::slotbook::{
        BlockContext, CoverageStart, SlotApply, SlotBookConfig, SourcedSlotbook, TxApply,
    };
    use aetelier_types::onchain::{
        Commitment, MatchRole, Transaction, TransactionFailure, TransactionVersion,
        encode_base58,
    };
    use solana_instruction_error::InstructionError;
    use yellowstone_grpc_proto::prelude::{
        BlockHeight, CompiledInstruction as WireInstruction, Message as WireMessage,
        MessageAddressTableLookup as WireLookup, MessageHeader as WireHeader,
        SubscribeUpdateAccount, SubscribeUpdateBlock, SubscribeUpdateBlockFooter,
        SubscribeUpdateEntry, SubscribeUpdateEntryUpdateParent, SubscribeUpdatePing,
        SubscribeUpdatePong, SubscribeUpdateSlot, SubscribeUpdateTransactionStatus,
        Transaction as WireTransaction, TransactionConfig as WireConfig,
        TransactionError as WireError, TransactionStatusMeta as WireMeta, UnixTimestamp,
    };
    use yellowstone_grpc_proto::prost::Message as _;

    const SLOT: u64 = 380_000_123;
    const BLOCK_TIME_SECS: i64 = 1_791_321_067;
    const BLOCKHASH: &str = "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn";
    const CUSTOM_6001: [u8; 13] = [8, 0, 0, 0, 1, 25, 0, 0, 0, 113, 23, 0, 0];

    fn wire_key(address: ChainAddress) -> Vec<u8> {
        address.pubkey().to_vec()
    }

    fn wire_info(tx_index: u64) -> SubscribeUpdateTransactionInfo {
        let mut signature = vec![7u8; SIGNATURE_LEN];
        signature[..8].copy_from_slice(&tx_index.to_le_bytes());
        SubscribeUpdateTransactionInfo {
            signature: signature.clone(),
            is_vote: false,
            transaction: Some(WireTransaction {
                signatures: vec![signature],
                message: Some(WireMessage {
                    header: Some(WireHeader {
                        num_required_signatures: 2,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    }),
                    account_keys: [key(1), key(2), key(3), key(4)]
                        .into_iter()
                        .map(wire_key)
                        .collect(),
                    recent_blockhash: vec![9u8; PUBKEY_LEN],
                    instructions: Vec::new(),
                    versioned: false,
                    address_table_lookups: Vec::new(),
                    config: None,
                }),
            }),
            meta: Some(WireMeta {
                err: None,
                fee: 5_000,
                log_messages: vec![format!("Program {} invoke [1]", key(4))],
                log_messages_none: false,
                compute_units_consumed: Some(41_215),
                cost_units: Some(1_492),
                ..Default::default()
            }),
            index: tx_index,
        }
    }

    fn wire_update(
        slot: u64,
        bank_id: u64,
        info: SubscribeUpdateTransactionInfo,
    ) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![TRANSACTIONS_FILTER.to_string()],
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                transaction: Some(info),
                slot,
                bank_id,
            })),
            created_at: None,
        }
    }

    fn wire_meta(slot: u64, bank_id: u64) -> SubscribeUpdateBlockMeta {
        SubscribeUpdateBlockMeta {
            slot,
            blockhash: BLOCKHASH.to_string(),
            block_time: Some(UnixTimestamp {
                timestamp: BLOCK_TIME_SECS,
            }),
            block_height: Some(BlockHeight {
                block_height: slot - 22_000_000,
            }),
            parent_slot: slot - 1,
            executed_transaction_count: 2,
            bank_id,
            ..Default::default()
        }
    }

    fn wire_meta_update(meta: SubscribeUpdateBlockMeta) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![BLOCKS_META_FILTER.to_string()],
            update_oneof: Some(UpdateOneof::BlockMeta(meta)),
            created_at: None,
        }
    }

    fn message_mut(info: &mut SubscribeUpdateTransactionInfo) -> &mut WireMessage {
        info.transaction
            .as_mut()
            .and_then(|transaction| transaction.message.as_mut())
            .expect("wire message")
    }

    fn meta_mut(info: &mut SubscribeUpdateTransactionInfo) -> &mut WireMeta {
        info.meta.as_mut().expect("wire meta")
    }

    fn instruction(program_id_index: u32, data: &[u8]) -> WireInstruction {
        WireInstruction {
            program_id_index,
            accounts: vec![0, 1],
            data: data.to_vec(),
        }
    }

    fn decode(info: SubscribeUpdateTransactionInfo) -> DecodedTransaction {
        decode_transaction(info, ReceiptStamp::default()).expect("well-formed")
    }

    fn watching(address: ChainAddress) -> WatchedAddress {
        WatchedAddress {
            address,
            required_role: MatchRole::AccountKey,
            include_failed: true,
            keep_logs: true,
        }
    }

    fn context() -> RowContext<'static> {
        RowContext {
            exchange: "solana",
            commitment: Commitment::Finalized,
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn a_legacy_message_maps_onto_the_shared_fixture() {
        let tx = decode_transaction(wire_info(9), decoded(9).receipt);
        assert_eq!(tx, Ok(decoded(9)));
        assert_eq!(tx.unwrap().version(), TransactionVersion::Legacy);
    }

    #[test]
    fn compute_budget_instructions_resolve_through_the_mapped_keys() {
        let mut info = wire_info(0);
        let message = message_mut(&mut info);
        message
            .account_keys
            .push(wire_key(COMPUTE_BUDGET_PROGRAM_ID));
        message.instructions = vec![
            instruction(4, &[2, 0x40, 0x0d, 0x03, 0x00]),
            instruction(4, &[3, 0xe8, 0x03, 0, 0, 0, 0, 0, 0]),
            instruction(3, &[2, 0x01, 0x00, 0x00, 0x00]),
        ];
        let tx = decode(info);
        assert_eq!(tx.key_at(4), Some(&COMPUTE_BUDGET_PROGRAM_ID));
        assert_eq!(
            tx.instructions[0],
            CompiledInstruction {
                program_id_index: 4,
                data: vec![2, 0x40, 0x0d, 0x03, 0x00],
            }
        );
        assert_eq!(
            tx.requested_compute(),
            RequestedCompute {
                compute_unit_limit: Some(200_000),
                compute_unit_price_micro_lamports: Some(1_000),
                priority_fee_lamports: None,
            }
        );
    }

    #[test]
    fn a_v0_message_appends_loaded_writable_then_loaded_readonly_keys() {
        let mut info = wire_info(1);
        let message = message_mut(&mut info);
        message.versioned = true;
        message.address_table_lookups = vec![WireLookup {
            account_key: wire_key(key(8)),
            writable_indexes: vec![0],
            readonly_indexes: vec![1, 2],
        }];
        let meta = meta_mut(&mut info);
        meta.loaded_writable_addresses = vec![wire_key(key(5))];
        meta.loaded_readonly_addresses = vec![wire_key(key(6)), wire_key(key(7))];
        let tx = decode(info);
        assert_eq!(tx.version(), TransactionVersion::V0);
        assert_eq!(tx.loaded_writable, vec![key(5)]);
        assert_eq!(tx.loaded_readonly, vec![key(6), key(7)]);
        let order: Vec<ChainAddress> = tx.account_keys().copied().collect();
        assert_eq!(
            order,
            vec![key(1), key(2), key(3), key(4), key(5), key(6), key(7)]
        );
        assert_eq!(tx.key_at(4), Some(&key(5)));
        assert_eq!(tx.key_at(6), Some(&key(7)));
        assert_eq!(tx.role_of(&key(6)), Some(MatchRole::AccountKey));
        assert_eq!(tx.role_of(&key(8)), None);
    }

    #[test]
    fn a_message_config_marks_v1_and_carries_its_budget() {
        let mut info = wire_info(2);
        let message = message_mut(&mut info);
        message.versioned = true;
        message.config = Some(WireConfig {
            priority_fee: Some(12_000),
            compute_unit_limit: Some(300_000),
            loaded_accounts_data_size_limit: Some(65_536),
            heap_size: None,
        });
        message
            .account_keys
            .push(wire_key(COMPUTE_BUDGET_PROGRAM_ID));
        message.instructions = vec![instruction(4, &[3, 0xe8, 0x03, 0, 0, 0, 0, 0, 0])];
        let tx = decode(info);
        assert_eq!(
            tx.config,
            Some(MessageConfig {
                priority_fee: Some(12_000),
                compute_unit_limit: Some(300_000),
            })
        );
        assert_eq!(tx.version(), TransactionVersion::V1);
        assert_eq!(
            tx.requested_compute(),
            RequestedCompute {
                compute_unit_limit: Some(300_000),
                compute_unit_price_micro_lamports: None,
                priority_fee_lamports: Some(12_000),
            }
        );
        let mut bare = wire_info(3);
        message_mut(&mut bare).config = Some(WireConfig::default());
        let tx = decode(bare);
        assert_eq!(tx.config, Some(MessageConfig::default()));
        assert_eq!(tx.version(), TransactionVersion::V1);
        let legacy = decode(wire_info(4));
        assert_eq!(
            (legacy.config, legacy.version()),
            (None, TransactionVersion::Legacy)
        );
    }

    #[test]
    fn logs_absent_on_the_wire_stay_absent_and_an_empty_list_stays_present() {
        let mut absent = wire_info(0);
        let meta = meta_mut(&mut absent);
        meta.log_messages = Vec::new();
        meta.log_messages_none = true;
        assert_eq!(decode(absent).log_messages, None);
        let mut empty = wire_info(0);
        meta_mut(&mut empty).log_messages = Vec::new();
        assert_eq!(decode(empty).log_messages, Some(Vec::new()));
        assert_eq!(
            decode(wire_info(0)).log_messages,
            Some(vec![format!("Program {} invoke [1]", key(4))])
        );
    }

    #[test]
    fn bincode_error_bytes_decode_to_their_debug_text_and_instruction_fault() {
        let fault = |index: u32, error: &str| {
            Some(InstructionFault {
                index,
                error: error.to_string(),
            })
        };
        let pins = [
            (
                CUSTOM_6001.to_vec(),
                "InstructionError(1, Custom(6001))",
                fault(1, "Custom(6001)"),
            ),
            (
                vec![8, 0, 0, 0, 0, 44, 0, 0, 0],
                "InstructionError(0, BorshIoError)",
                fault(0, "BorshIoError"),
            ),
            (vec![0, 0, 0, 0], "AccountInUse", None),
            (
                vec![31, 0, 0, 0, 7],
                "InsufficientFundsForRent { account_index: 7 }",
                None,
            ),
            (vec![30, 0, 0, 0, 3], "DuplicateInstruction(3)", None),
        ];
        for (raw, error, instruction) in pins {
            assert_eq!(
                execution_error(raw.clone()),
                ExecutionError {
                    raw,
                    decoded: Some(DecodedError {
                        error: error.to_string(),
                        instruction,
                    }),
                }
            );
        }
        assert_eq!(
            wincode::serialize(&TransactionError::InstructionError(
                1,
                InstructionError::Custom(6001)
            ))
            .unwrap(),
            CUSTOM_6001.to_vec()
        );

        let mut info = wire_info(4);
        message_mut(&mut info).instructions = vec![
            instruction(3, &[2, 0, 0, 1, 0]),
            instruction(2, &[0xe5, 0x17, 0xcb, 0x97]),
        ];
        let meta = meta_mut(&mut info);
        meta.err = Some(WireError {
            err: CUSTOM_6001.to_vec(),
        });
        meta.log_messages = vec![
            format!("Program {} invoke [1]", key(3)),
            format!("Program {} failed: custom program error: 0x1771", key(3)),
        ];
        let tx = decode(info);
        assert!(!tx.succeeded());
        assert_eq!(
            tx.failure(),
            Some(TransactionFailure {
                error: "InstructionError(1, Custom(6001))".to_string(),
                instruction_index: Some(1),
                program_id: Some(key(3).to_base58()),
                instruction_error: Some("Custom(6001)".to_string()),
                instruction_data: Some("e517cb97".to_string()),
                log_program_id: Some(key(3).to_base58()),
                log_message: Some("custom program error: 0x1771".to_string()),
            })
        );
    }

    #[test]
    fn undecodable_error_bytes_keep_the_raw_bytes_and_the_row() {
        let mut trailing = CUSTOM_6001.to_vec();
        trailing.push(0);
        let block = BlockContext {
            slot: SLOT,
            block_height: SLOT - 22_000_000,
            block_time_us: BLOCK_TIME_SECS as u64 * 1_000_000,
            blockhash: BLOCKHASH,
        };
        for raw in [
            vec![200, 0, 0, 0],
            vec![8, 0, 0, 0, 1],
            trailing,
            Vec::new(),
        ] {
            assert_eq!(
                execution_error(raw.clone()),
                ExecutionError {
                    raw: raw.clone(),
                    decoded: None,
                }
            );
            let mut info = wire_info(5);
            meta_mut(&mut info).err = Some(WireError { err: raw.clone() });
            let tx = decode(info);
            let failure = tx.failure().expect("failed transaction");
            assert_eq!(failure.error, format!("0x{}", hex(&raw)));
            assert_eq!(failure.instruction_index, None);
            let rows = transaction_rows(block, &tx, &[watching(key(1))], context());
            assert_eq!(rows.len(), 1, "{raw:?}");
            assert!(!rows[0].success);
            assert_eq!(rows[0].failure, Some(failure));
            assert_eq!(rows[0].check_invariants(), Ok(()));
        }
    }

    #[test]
    fn fee_vote_flag_index_and_unit_counts_copy_through() {
        let mut info = wire_info(42);
        info.is_vote = true;
        message_mut(&mut info).header = Some(WireHeader {
            num_required_signatures: 3,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 2,
        });
        let meta = meta_mut(&mut info);
        meta.fee = 7_500;
        meta.compute_units_consumed = None;
        meta.cost_units = Some(3_000);
        let tx = decode(info);
        assert!(tx.is_vote);
        assert_eq!(tx.tx_index, 42);
        assert_eq!(tx.fee_lamports, 7_500);
        assert_eq!(
            (tx.compute_units_consumed, tx.cost_units),
            (None, Some(3_000))
        );
        assert_eq!(
            tx.header,
            MessageHeader {
                num_required_signatures: 3,
                num_readonly_signed_accounts: 1,
                num_readonly_unsigned_accounts: 2,
            }
        );
        assert!(tx.succeeded());
    }

    #[test]
    fn the_receipt_rides_on_the_decoded_transaction() {
        let receipt = ReceiptStamp {
            local_tx_ts_us: 1_791_321_068_123_456,
            source_tx_rtt_us: 850,
            recv_seq: 77,
            conn_epoch_us: 1_791_321_000_000_000,
        };
        let GrpcUpdate::Transaction(update) =
            decode_update(wire_update(SLOT, 4, wire_info(3)), receipt)
        else {
            panic!("expected a transaction update");
        };
        assert_eq!((update.slot, update.bank_id), (SLOT, 4));
        assert_eq!(update.decoded.expect("well-formed").receipt, receipt);
    }

    #[test]
    fn malformed_transactions_name_the_field_and_keep_slot_and_bank() {
        let edited = |edit: &dyn Fn(&mut SubscribeUpdateTransactionInfo)| {
            let mut info = wire_info(6);
            edit(&mut info);
            wire_update(SLOT, 3, info)
        };
        let no_transaction = SubscribeUpdate {
            filters: vec![TRANSACTIONS_FILTER.to_string()],
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                transaction: None,
                slot: SLOT,
                bank_id: 3,
            })),
            created_at: None,
        };
        let malformed = [
            (no_transaction, MalformedTransaction::Missing("transaction")),
            (
                edited(&|info| info.transaction = None),
                MalformedTransaction::Missing("transaction.transaction"),
            ),
            (
                edited(&|info| info.transaction.as_mut().unwrap().message = None),
                MalformedTransaction::Missing("transaction.transaction.message"),
            ),
            (
                edited(&|info| message_mut(info).header = None),
                MalformedTransaction::Missing("transaction.transaction.message.header"),
            ),
            (
                edited(&|info| info.meta = None),
                MalformedTransaction::Missing("transaction.meta"),
            ),
            (
                edited(&|info| info.signature.truncate(63)),
                MalformedTransaction::SignatureLength { len: 63 },
            ),
            (
                edited(&|info| message_mut(info).account_keys[2].truncate(31)),
                MalformedTransaction::KeyLength {
                    list: "account_keys",
                    index: 2,
                    len: 31,
                },
            ),
            (
                edited(&|info| {
                    meta_mut(info).loaded_writable_addresses = vec![vec![5u8; 33]]
                }),
                MalformedTransaction::KeyLength {
                    list: "loaded_writable_addresses",
                    index: 0,
                    len: 33,
                },
            ),
            (
                edited(&|info| {
                    meta_mut(info).loaded_readonly_addresses =
                        vec![wire_key(key(6)), Vec::new()]
                }),
                MalformedTransaction::KeyLength {
                    list: "loaded_readonly_addresses",
                    index: 1,
                    len: 0,
                },
            ),
        ];
        for (update, expected) in malformed {
            let GrpcUpdate::Transaction(update) =
                decode_update(update, ReceiptStamp::default())
            else {
                panic!("expected a transaction update");
            };
            assert_eq!((update.slot, update.bank_id), (SLOT, 3));
            assert_eq!(update.decoded, Err(expected));

            let mut book =
                SourcedSlotbook::<DecodedTransaction>::new(SlotBookConfig::default());
            book.open_coverage(CoverageStart::AtSlot(SLOT));
            let sound = decode(wire_info(0));
            assert_eq!(book.on_transaction(SLOT, 3, 0, sound), TxApply::Buffered);
            assert_eq!(
                book.on_undecodable(update.slot, update.bank_id),
                TxApply::Poisoned
            );
            let GrpcUpdate::BlockMeta(meta) = decode_update(
                wire_meta_update(wire_meta(SLOT, 3)),
                ReceiptStamp::default(),
            ) else {
                panic!("expected a block meta");
            };
            let seal = book.on_block_meta(meta);
            assert_eq!(seal.apply, SlotApply::Partial);
            assert!(seal.released.is_empty());
            assert_eq!(book.counters().partial_banks_discarded, 1);
        }
        assert_eq!(
            MalformedTransaction::Missing("transaction.meta").to_string(),
            "transaction update carries no transaction.meta"
        );
        assert_eq!(
            MalformedTransaction::SignatureLength { len: 63 }.to_string(),
            "signature is 63 bytes, expected 64"
        );
        assert_eq!(
            MalformedTransaction::KeyLength {
                list: "account_keys",
                index: 2,
                len: 31,
            }
            .to_string(),
            "account_keys key 2 is 31 bytes, expected 32"
        );
    }

    #[test]
    fn block_meta_keeps_bank_and_optional_time_and_height() {
        assert_eq!(
            block_meta(wire_meta(SLOT, 2)),
            BlockMeta {
                slot: SLOT,
                bank_id: 2,
                parent_slot: SLOT - 1,
                block_height: Some(SLOT - 22_000_000),
                block_time_secs: Some(BLOCK_TIME_SECS),
                blockhash: BLOCKHASH.to_string(),
            }
        );
        let bare = block_meta(SubscribeUpdateBlockMeta {
            block_time: None,
            block_height: None,
            blockhash: String::new(),
            ..wire_meta(SLOT + 1, 0)
        });
        assert_eq!(
            (
                bare.block_height,
                bare.block_time_secs,
                bare.blockhash.as_str()
            ),
            (None, None, "")
        );
        let zeroed = block_meta(SubscribeUpdateBlockMeta {
            block_time: Some(UnixTimestamp { timestamp: 0 }),
            block_height: Some(BlockHeight { block_height: 0 }),
            ..wire_meta(SLOT + 2, 1)
        });
        assert_eq!(
            (zeroed.bank_id, zeroed.block_height, zeroed.block_time_secs),
            (1, Some(0), Some(0))
        );
        let mut book =
            SourcedSlotbook::<DecodedTransaction>::new(SlotBookConfig::default());
        book.open_coverage(CoverageStart::AtSlot(SLOT));
        assert_eq!(book.on_block_meta(bare).apply, SlotApply::Partial);
        assert_eq!(book.on_block_meta(zeroed).apply, SlotApply::Partial);
    }

    #[test]
    fn every_update_kind_has_an_explicit_mapping() {
        let unrequested = [
            (
                Some(UpdateOneof::Account(SubscribeUpdateAccount::default())),
                "account",
            ),
            (
                Some(UpdateOneof::Slot(SubscribeUpdateSlot::default())),
                "slot",
            ),
            (
                Some(UpdateOneof::TransactionStatus(
                    SubscribeUpdateTransactionStatus::default(),
                )),
                "transaction_status",
            ),
            (
                Some(UpdateOneof::Block(SubscribeUpdateBlock::default())),
                "block",
            ),
            (
                Some(UpdateOneof::Entry(SubscribeUpdateEntry::default())),
                "entry",
            ),
            (
                Some(UpdateOneof::BlockFooter(
                    SubscribeUpdateBlockFooter::default(),
                )),
                "block_footer",
            ),
            (
                Some(UpdateOneof::EntryUpdateParent(
                    SubscribeUpdateEntryUpdateParent::default(),
                )),
                "entry_update_parent",
            ),
            (None, "empty"),
        ];
        for (update_oneof, kind) in unrequested {
            let update = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof,
                created_at: None,
            };
            assert!(
                matches!(
                    decode_update(update, ReceiptStamp::default()),
                    GrpcUpdate::Unrequested(found) if found == kind
                ),
                "{kind}"
            );
        }
        let ping = SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        };
        assert!(matches!(
            decode_update(ping, ReceiptStamp::default()),
            GrpcUpdate::Ping
        ));
        let pong = SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(UpdateOneof::Pong(SubscribeUpdatePong { id: 5 })),
            created_at: None,
        };
        assert!(matches!(
            decode_update(pong, ReceiptStamp::default()),
            GrpcUpdate::Pong { id: 5 }
        ));
        assert!(matches!(
            decode_update(
                wire_meta_update(wire_meta(SLOT, 0)),
                ReceiptStamp::default()
            ),
            GrpcUpdate::BlockMeta(BlockMeta { slot: SLOT, .. })
        ));
        assert!(matches!(
            decode_update(wire_update(SLOT, 0, wire_info(1)), ReceiptStamp::default()),
            GrpcUpdate::Transaction(TransactionUpdate {
                slot: SLOT,
                decoded: Ok(_),
                ..
            })
        ));
    }

    #[test]
    fn prost_round_trips_decode_like_the_original() {
        let mut rich = wire_info(7);
        let message = message_mut(&mut rich);
        message.versioned = true;
        message.config = Some(WireConfig {
            priority_fee: Some(9_000),
            compute_unit_limit: None,
            loaded_accounts_data_size_limit: None,
            heap_size: Some(65_536),
        });
        message.instructions = vec![instruction(2, &[0xe5, 0x17])];
        let meta = meta_mut(&mut rich);
        meta.err = Some(WireError {
            err: CUSTOM_6001.to_vec(),
        });
        meta.loaded_writable_addresses = vec![wire_key(key(5))];
        meta.loaded_readonly_addresses = vec![wire_key(key(6))];
        let mut short = wire_info(8);
        short.signature.truncate(10);
        let receipt = decoded(7).receipt;
        let updates = [
            wire_update(SLOT, 1, rich),
            wire_update(SLOT, 1, short),
            wire_meta_update(wire_meta(SLOT, 1)),
            SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Pong(SubscribeUpdatePong { id: 3 })),
                created_at: None,
            },
            SubscribeUpdate::default(),
        ];
        for update in updates {
            let bytes = update.encode_to_vec();
            let replayed =
                SubscribeUpdate::decode(bytes.as_slice()).expect("prost decode");
            assert_eq!(replayed, update);
            assert_eq!(
                format!("{:?}", decode_update(replayed, receipt)),
                format!("{:?}", decode_update(update, receipt))
            );
        }
    }

    #[test]
    fn a_decoded_bank_releases_rows_with_its_block_context() {
        let watches = [watching(key(3))];
        let mut book = SourcedSlotbook::new(SlotBookConfig::default());
        book.open_coverage(CoverageStart::AtSlot(SLOT));
        for (tx_index, recv_seq) in [(5u64, 1u64), (2, 2)] {
            let receipt = ReceiptStamp {
                local_tx_ts_us: 1_791_321_068_000_000 + recv_seq,
                source_tx_rtt_us: 900,
                recv_seq,
                conn_epoch_us: 1_791_321_000_000_000,
            };
            let GrpcUpdate::Transaction(TransactionUpdate {
                slot,
                bank_id,
                decoded,
            }) = decode_update(wire_update(SLOT, 0, wire_info(tx_index)), receipt)
            else {
                panic!("expected a transaction update");
            };
            let tx = decoded.expect("well-formed");
            assert_eq!(
                book.on_transaction(slot, bank_id, tx.tx_index, tx),
                TxApply::Buffered
            );
        }
        let GrpcUpdate::BlockMeta(meta) = decode_update(
            wire_meta_update(wire_meta(SLOT, 0)),
            ReceiptStamp::default(),
        ) else {
            panic!("expected a block meta");
        };
        let seal = book.on_block_meta(meta);
        assert_eq!(seal.apply, SlotApply::Finalized);
        let bank = &seal.released[0];
        let rows: Vec<Transaction> = bank
            .txs
            .iter()
            .flat_map(|tx| transaction_rows(bank.block(), tx, &watches, context()))
            .collect();
        let order: Vec<(u64, u64)> = rows
            .iter()
            .map(|row| (row.tx_index, row.recv_seq))
            .collect();
        assert_eq!(order, vec![(2, 2), (5, 1)]);
        for row in &rows {
            assert_eq!(row.check_invariants(), Ok(()));
            assert_eq!((row.slot, row.block_height), (SLOT, SLOT - 22_000_000));
            assert_eq!(row.blockhash, BLOCKHASH);
            assert_eq!(row.source_tx_ts_us, BLOCK_TIME_SECS as u64 * 1_000_000);
            assert_eq!(row.match_role, MatchRole::AccountKey);
            assert_eq!(
                row.signature,
                encode_base58(&decoded(row.tx_index).signature)
            );
            assert_eq!(row.local_tx_ts_us, 1_791_321_068_000_000 + row.recv_seq);
            assert_eq!(
                row.log_messages,
                Some(vec![format!("Program {} invoke [1]", key(4))])
            );
        }
    }
}
