use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::errors::BuildError;
use crate::onchain::address::ChainAddress;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MatchRole {
    #[default]
    AccountKey,
    Signer,
    FeePayer,
}

impl MatchRole {
    pub const ALL: [MatchRole; 3] = [
        MatchRole::AccountKey,
        MatchRole::Signer,
        MatchRole::FeePayer,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            MatchRole::AccountKey => "account_key",
            MatchRole::Signer => "signer",
            MatchRole::FeePayer => "fee_payer",
        }
    }

    pub fn satisfies(self, required: MatchRole) -> bool {
        self >= required
    }
}

impl fmt::Display for MatchRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MatchRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        MatchRole::ALL
            .into_iter()
            .find(|role| role.as_str() == s)
            .ok_or_else(|| format!("unknown match role: {s}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    Confirmed,
    #[default]
    Finalized,
}

impl Commitment {
    pub const ALL: [Commitment; 2] = [Commitment::Confirmed, Commitment::Finalized];

    pub const fn as_str(self) -> &'static str {
        match self {
            Commitment::Confirmed => "confirmed",
            Commitment::Finalized => "finalized",
        }
    }
}

impl fmt::Display for Commitment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Commitment {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Commitment::ALL
            .into_iter()
            .find(|c| c.as_str() == s)
            .ok_or_else(|| format!("unsupported commitment: {s}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionVersion {
    Legacy,
    V0,
    V1,
}

impl TransactionVersion {
    pub const ALL: [TransactionVersion; 3] = [
        TransactionVersion::Legacy,
        TransactionVersion::V0,
        TransactionVersion::V1,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            TransactionVersion::Legacy => "legacy",
            TransactionVersion::V0 => "v0",
            TransactionVersion::V1 => "v1",
        }
    }
}

impl fmt::Display for TransactionVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TransactionVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TransactionVersion::ALL
            .into_iter()
            .find(|v| v.as_str() == s)
            .ok_or_else(|| format!("unknown transaction version: {s}"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TransactionFailure {
    pub error: String,
    #[serde(default)]
    pub instruction_index: Option<u32>,
    #[serde(default)]
    pub program_id: Option<String>,
    #[serde(default)]
    pub instruction_error: Option<String>,
    #[serde(default)]
    pub instruction_data: Option<String>,
    #[serde(default)]
    pub log_program_id: Option<String>,
    #[serde(default)]
    pub log_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub exchange: String,
    pub address: ChainAddress,
    pub match_role: MatchRole,
    pub source_tx_ts_us: u64,
    #[serde(default)]
    pub local_tx_ts_us: u64,
    #[serde(default)]
    pub source_tx_rtt_us: u64,
    #[serde(default)]
    pub recv_seq: u64,
    #[serde(default)]
    pub conn_epoch_us: u64,
    pub commitment: Commitment,
    pub slot: u64,
    pub block_height: u64,
    pub blockhash: String,
    pub tx_index: u64,
    pub signature: String,
    pub version: TransactionVersion,
    pub success: bool,
    pub fee_lamports: u64,
    #[serde(default)]
    pub compute_units_consumed: Option<u64>,
    #[serde(default)]
    pub compute_unit_limit: Option<u32>,
    #[serde(default)]
    pub compute_unit_price_micro_lamports: Option<u64>,
    #[serde(default)]
    pub priority_fee_lamports: Option<u64>,
    #[serde(default)]
    pub cost_units: Option<u64>,
    #[serde(default)]
    pub signers: Vec<String>,
    #[serde(default)]
    pub account_keys: Vec<String>,
    #[serde(default)]
    pub log_messages: Option<Vec<String>>,
    #[serde(default)]
    pub failure: Option<TransactionFailure>,
}

impl Transaction {
    pub fn effective_ts_us(&self) -> u64 {
        if self.source_tx_ts_us > 0 {
            self.source_tx_ts_us
        } else {
            self.local_tx_ts_us
        }
    }

    pub fn row_key(&self) -> (ChainAddress, &str) {
        (self.address, self.signature.as_str())
    }

    pub fn position(&self) -> (u64, u64) {
        (self.slot, self.tx_index)
    }

    pub fn check_invariants(&self) -> Result<(), BuildError> {
        let invalid = |field: &'static str, reason: &str| {
            Err(BuildError::InvalidField {
                field,
                reason: reason.to_string(),
            })
        };
        if self.signature.is_empty() {
            return invalid("signature", "empty");
        }
        if self.source_tx_ts_us == 0 {
            return invalid("source_tx_ts_us", "zero: block time must be present");
        }
        if self.blockhash.is_empty() {
            return invalid("blockhash", "empty: block context must be present");
        }
        if self.block_height == 0 {
            return invalid("block_height", "zero: block context must be present");
        }
        if self.success == self.failure.is_some() {
            return invalid(
                "failure",
                "must be present exactly when the transaction failed",
            );
        }
        if matches!(self.failure, Some(ref f) if f.error.is_empty()) {
            return invalid("failure.error", "empty");
        }
        if self.version == TransactionVersion::V1
            && self.compute_unit_price_micro_lamports.is_some()
        {
            return invalid(
                "compute_unit_price_micro_lamports",
                "v1 transactions carry priority_fee_lamports instead",
            );
        }
        if self.version != TransactionVersion::V1 && self.priority_fee_lamports.is_some()
        {
            return invalid(
                "priority_fee_lamports",
                "only v1 transactions carry a total priority fee",
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Transaction {
        Transaction {
            exchange: "solana".to_string(),
            address: ChainAddress::solana("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4")
                .unwrap(),
            match_role: MatchRole::AccountKey,
            source_tx_ts_us: 1_791_321_067_000_000,
            local_tx_ts_us: 1_791_321_067_420_000,
            source_tx_rtt_us: 0,
            recv_seq: 7,
            conn_epoch_us: 1_791_321_000_000_000,
            commitment: Commitment::Finalized,
            slot: 380_000_123,
            block_height: 358_000_001,
            blockhash: "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn".to_string(),
            tx_index: 42,
            signature: "5h6xBEauJ3PK6SWCZ1PGjBvj8vDdWG3KpwATGy1ARAXFSDwt8GFXM7W5Ncn16wmqokgpiKRLuS83KUxyZyv2sUYv".to_string(),
            version: TransactionVersion::V0,
            success: true,
            fee_lamports: 5_000,
            compute_units_consumed: Some(41_215),
            compute_unit_limit: Some(200_000),
            compute_unit_price_micro_lamports: Some(1_000),
            priority_fee_lamports: None,
            cost_units: Some(1_492),
            signers: vec!["11111111111111111111111111111111".to_string()],
            account_keys: vec!["11111111111111111111111111111111".to_string()],
            log_messages: None,
            failure: None,
        }
    }

    #[test]
    fn match_roles_nest_from_account_key_to_fee_payer() {
        assert!(MatchRole::FeePayer.satisfies(MatchRole::Signer));
        assert!(MatchRole::FeePayer.satisfies(MatchRole::AccountKey));
        assert!(MatchRole::Signer.satisfies(MatchRole::AccountKey));
        assert!(!MatchRole::Signer.satisfies(MatchRole::FeePayer));
        assert!(!MatchRole::AccountKey.satisfies(MatchRole::Signer));
        assert_eq!(MatchRole::default(), MatchRole::AccountKey);
    }

    #[test]
    fn enum_strings_round_trip_and_reject_unknown_values() {
        for role in MatchRole::ALL {
            assert_eq!(role.as_str().parse::<MatchRole>().unwrap(), role);
            assert_eq!(
                serde_json::to_string(&role).unwrap(),
                format!("\"{}\"", role.as_str())
            );
        }
        for commitment in Commitment::ALL {
            assert_eq!(
                commitment.as_str().parse::<Commitment>().unwrap(),
                commitment
            );
        }
        for version in TransactionVersion::ALL {
            assert_eq!(
                version.as_str().parse::<TransactionVersion>().unwrap(),
                version
            );
        }
        assert!("processed".parse::<Commitment>().is_err());
        assert!("v2".parse::<TransactionVersion>().is_err());
        assert!("owner".parse::<MatchRole>().is_err());
        assert_eq!(Commitment::default(), Commitment::Finalized);
    }

    #[test]
    fn a_complete_successful_row_passes_the_invariants() {
        assert_eq!(sample().check_invariants(), Ok(()));
    }

    #[test]
    fn failure_presence_must_match_success() {
        let mut tx = sample();
        tx.success = false;
        assert!(tx.check_invariants().is_err());
        tx.failure = Some(TransactionFailure {
            error: "InstructionError".to_string(),
            instruction_index: Some(2),
            ..TransactionFailure::default()
        });
        assert_eq!(tx.check_invariants(), Ok(()));
        tx.success = true;
        assert!(tx.check_invariants().is_err());
    }

    #[test]
    fn block_context_must_be_present() {
        let mut tx = sample();
        tx.block_height = 0;
        assert!(tx.check_invariants().is_err());
        let mut tx = sample();
        tx.source_tx_ts_us = 0;
        assert!(tx.check_invariants().is_err());
        let mut tx = sample();
        tx.blockhash.clear();
        assert!(tx.check_invariants().is_err());
    }

    #[test]
    fn fee_fields_follow_the_message_version() {
        let mut tx = sample();
        tx.version = TransactionVersion::V1;
        assert!(tx.check_invariants().is_err());
        tx.compute_unit_price_micro_lamports = None;
        tx.priority_fee_lamports = Some(12_000);
        assert_eq!(tx.check_invariants(), Ok(()));
        tx.version = TransactionVersion::Legacy;
        assert!(tx.check_invariants().is_err());
    }

    #[test]
    fn serde_round_trips_and_defaults_optional_fields() {
        let tx = sample();
        let json = serde_json::to_string(&tx).unwrap();
        let back: Transaction = serde_json::from_str(&json).unwrap();
        assert_eq!(back, tx);
        let mut value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let obj = value.as_object_mut().unwrap();
        for optional in [
            "local_tx_ts_us",
            "compute_units_consumed",
            "log_messages",
            "failure",
            "signers",
        ] {
            obj.remove(optional);
        }
        let sparse: Transaction = serde_json::from_value(value).unwrap();
        assert_eq!(sparse.local_tx_ts_us, 0);
        assert_eq!(sparse.compute_units_consumed, None);
        assert!(sparse.signers.is_empty());
    }

    #[test]
    fn effective_time_falls_back_to_receipt_time() {
        let mut tx = sample();
        assert_eq!(tx.effective_ts_us(), 1_791_321_067_000_000);
        tx.source_tx_ts_us = 0;
        assert_eq!(tx.effective_ts_us(), 1_791_321_067_420_000);
    }
}
