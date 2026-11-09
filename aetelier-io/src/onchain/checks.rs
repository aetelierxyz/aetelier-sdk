use std::collections::{HashMap, HashSet};

use aetelier_types::onchain::{ChainAddress, Transaction};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionChecks {
    pub rows: u64,
    pub duplicate_keys: u64,
    pub out_of_order: u64,
    pub invariant_violations: u64,
    pub first_duplicate: Option<String>,
    pub first_out_of_order: Option<String>,
    pub first_invariant_violation: Option<String>,
}

impl TransactionChecks {
    pub fn passed(&self) -> bool {
        self.duplicate_keys == 0
            && self.out_of_order == 0
            && self.invariant_violations == 0
    }
}

pub fn check_transactions(rows: &[Transaction]) -> TransactionChecks {
    let mut checks = TransactionChecks {
        rows: rows.len() as u64,
        ..TransactionChecks::default()
    };
    let mut keys: HashSet<(ChainAddress, &str)> = HashSet::with_capacity(rows.len());
    let mut last_position: HashMap<ChainAddress, (u64, u64)> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        if !keys.insert(row.row_key()) {
            checks.duplicate_keys += 1;
            checks.first_duplicate.get_or_insert_with(|| {
                format!("row {i}: ({}, {}) repeated", row.address, row.signature)
            });
        }
        if let Some(previous) = last_position.insert(row.address, row.position())
            && previous >= row.position()
        {
            checks.out_of_order += 1;
            checks.first_out_of_order.get_or_insert_with(|| {
                format!(
                    "row {i}: {} at (slot {}, index {}) after (slot {}, index {})",
                    row.address, row.slot, row.tx_index, previous.0, previous.1
                )
            });
        }
        if let Err(err) = row.check_invariants() {
            checks.invariant_violations += 1;
            checks
                .first_invariant_violation
                .get_or_insert_with(|| format!("row {i}: {} {err}", row.signature));
        }
    }
    checks
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::onchain::{
        Commitment, MatchRole, TransactionFailure, TransactionVersion,
    };

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
    const WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

    fn row(address: &str, slot: u64, tx_index: u64, signature: &str) -> Transaction {
        Transaction {
            exchange: "solana".to_string(),
            address: ChainAddress::solana(address).unwrap(),
            match_role: MatchRole::AccountKey,
            source_tx_ts_us: 1_791_321_067_000_000,
            local_tx_ts_us: 0,
            source_tx_rtt_us: 0,
            recv_seq: 0,
            conn_epoch_us: 0,
            commitment: Commitment::Finalized,
            slot,
            block_height: slot - 22_000_000,
            blockhash: "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn".to_string(),
            tx_index,
            signature: signature.to_string(),
            version: TransactionVersion::Legacy,
            success: true,
            fee_lamports: 5_000,
            compute_units_consumed: None,
            compute_unit_limit: None,
            compute_unit_price_micro_lamports: None,
            priority_fee_lamports: None,
            cost_units: None,
            signers: Vec::new(),
            account_keys: Vec::new(),
            log_messages: None,
            failure: None,
        }
    }

    #[test]
    fn ordered_unique_complete_rows_pass() {
        let rows = vec![
            row(JUPITER_V6, 380_000_000, 1, "a"),
            row(JUPITER_V6, 380_000_000, 7, "b"),
            row(WHIRLPOOL, 380_000_000, 7, "b"),
            row(JUPITER_V6, 380_000_002, 0, "c"),
        ];
        let checks = check_transactions(&rows);
        assert!(checks.passed(), "{checks:?}");
        assert_eq!(checks.rows, 4);
    }

    #[test]
    fn a_repeated_address_signature_pair_fails_uniqueness_and_order() {
        let rows = vec![
            row(JUPITER_V6, 380_000_000, 1, "a"),
            row(JUPITER_V6, 380_000_000, 1, "a"),
        ];
        let checks = check_transactions(&rows);
        assert_eq!((checks.duplicate_keys, checks.out_of_order), (1, 1));
        assert!(checks.first_duplicate.unwrap().contains("repeated"));
    }

    #[test]
    fn a_position_going_backwards_for_one_address_is_out_of_order() {
        let rows = vec![
            row(JUPITER_V6, 380_000_001, 0, "a"),
            row(WHIRLPOOL, 380_000_000, 3, "b"),
            row(JUPITER_V6, 380_000_000, 9, "c"),
        ];
        let checks = check_transactions(&rows);
        assert_eq!(checks.out_of_order, 1);
        assert_eq!(checks.duplicate_keys, 0);
        assert!(checks.first_out_of_order.unwrap().starts_with("row 2:"));
    }

    #[test]
    fn missing_block_context_or_failure_mismatch_counts_as_invariant_violation() {
        let mut no_height = row(JUPITER_V6, 380_000_000, 1, "a");
        no_height.block_height = 0;
        let mut failed_without_detail = row(JUPITER_V6, 380_000_000, 2, "b");
        failed_without_detail.success = false;
        let mut failed_with_detail = row(JUPITER_V6, 380_000_000, 3, "c");
        failed_with_detail.success = false;
        failed_with_detail.failure = Some(TransactionFailure {
            error: "InsufficientFundsForFee".to_string(),
            ..TransactionFailure::default()
        });
        let checks =
            check_transactions(&[no_height, failed_without_detail, failed_with_detail]);
        assert_eq!(checks.invariant_violations, 2);
        assert!(!checks.passed());
    }
}
