use aetelier_types::onchain::{ChainAddress, MatchRole, TransactionVersion};

pub const SIGNATURE_LEN: usize = 64;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MessageHeader {
    pub num_required_signatures: u32,
    pub num_readonly_signed_accounts: u32,
    pub num_readonly_unsigned_accounts: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MessageConfig {
    pub priority_fee: Option<u64>,
    pub compute_unit_limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledInstruction {
    pub program_id_index: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionFault {
    pub index: u32,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedError {
    pub error: String,
    pub instruction: Option<InstructionFault>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionError {
    pub raw: Vec<u8>,
    pub decoded: Option<DecodedError>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiptStamp {
    pub local_tx_ts_us: u64,
    pub source_tx_rtt_us: u64,
    pub recv_seq: u64,
    pub conn_epoch_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedTransaction {
    pub signature: [u8; SIGNATURE_LEN],
    pub is_vote: bool,
    pub tx_index: u64,
    pub versioned: bool,
    pub config: Option<MessageConfig>,
    pub header: MessageHeader,
    pub static_keys: Vec<ChainAddress>,
    pub loaded_writable: Vec<ChainAddress>,
    pub loaded_readonly: Vec<ChainAddress>,
    pub instructions: Vec<CompiledInstruction>,
    pub fee_lamports: u64,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
    pub error: Option<ExecutionError>,
    pub log_messages: Option<Vec<String>>,
    pub receipt: ReceiptStamp,
}

impl DecodedTransaction {
    pub fn version(&self) -> TransactionVersion {
        match (self.config.is_some(), self.versioned) {
            (true, _) => TransactionVersion::V1,
            (false, true) => TransactionVersion::V0,
            (false, false) => TransactionVersion::Legacy,
        }
    }

    pub fn succeeded(&self) -> bool {
        self.error.is_none()
    }

    pub fn account_keys(&self) -> impl Iterator<Item = &ChainAddress> {
        self.static_keys
            .iter()
            .chain(&self.loaded_writable)
            .chain(&self.loaded_readonly)
    }

    pub fn key_at(&self, index: u32) -> Option<&ChainAddress> {
        self.account_keys().nth(index as usize)
    }

    pub fn signers(&self) -> &[ChainAddress] {
        let count =
            (self.header.num_required_signatures as usize).min(self.static_keys.len());
        &self.static_keys[..count]
    }

    pub fn role_of(&self, address: &ChainAddress) -> Option<MatchRole> {
        let signer_count = self.signers().len();
        let static_role = self
            .static_keys
            .iter()
            .enumerate()
            .filter(|(_, key)| *key == address)
            .map(|(index, _)| match index {
                0 if signer_count > 0 => MatchRole::FeePayer,
                index if index < signer_count => MatchRole::Signer,
                _ => MatchRole::AccountKey,
            })
            .max();
        static_role.or_else(|| {
            self.loaded_writable
                .iter()
                .chain(&self.loaded_readonly)
                .any(|key| key == address)
                .then_some(MatchRole::AccountKey)
        })
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    use aetelier_types::onchain::Chain;

    pub(crate) fn key(seed: u8) -> ChainAddress {
        let mut pubkey = [0u8; 32];
        pubkey[0] = seed;
        pubkey[31] = seed;
        ChainAddress::new(Chain::Solana, pubkey)
    }

    pub(crate) fn decoded(tx_index: u64) -> DecodedTransaction {
        let mut signature = [7u8; SIGNATURE_LEN];
        signature[..8].copy_from_slice(&tx_index.to_le_bytes());
        DecodedTransaction {
            signature,
            is_vote: false,
            tx_index,
            versioned: false,
            config: None,
            header: MessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            static_keys: vec![key(1), key(2), key(3), key(4)],
            loaded_writable: Vec::new(),
            loaded_readonly: Vec::new(),
            instructions: Vec::new(),
            fee_lamports: 5_000,
            compute_units_consumed: Some(41_215),
            cost_units: Some(1_492),
            error: None,
            log_messages: Some(vec![format!("Program {} invoke [1]", key(4))]),
            receipt: ReceiptStamp {
                local_tx_ts_us: 1_791_321_068_000_000 + tx_index,
                source_tx_rtt_us: 900,
                recv_seq: 40 + tx_index,
                conn_epoch_us: 1_791_321_000_000_000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{decoded, key};
    use super::*;

    #[test]
    fn version_is_v1_with_a_message_config_else_follows_the_versioned_flag() {
        let mut tx = decoded(0);
        assert_eq!(tx.version(), TransactionVersion::Legacy);
        tx.versioned = true;
        assert_eq!(tx.version(), TransactionVersion::V0);
        tx.config = Some(MessageConfig::default());
        assert_eq!(tx.version(), TransactionVersion::V1);
    }

    #[test]
    fn roles_rank_fee_payer_over_signer_over_account_key() {
        let mut tx = decoded(0);
        tx.loaded_writable = vec![key(5)];
        tx.loaded_readonly = vec![key(6)];
        assert_eq!(tx.role_of(&key(1)), Some(MatchRole::FeePayer));
        assert_eq!(tx.role_of(&key(2)), Some(MatchRole::Signer));
        assert_eq!(tx.role_of(&key(3)), Some(MatchRole::AccountKey));
        assert_eq!(tx.role_of(&key(5)), Some(MatchRole::AccountKey));
        assert_eq!(tx.role_of(&key(6)), Some(MatchRole::AccountKey));
        assert_eq!(tx.role_of(&key(9)), None);
    }

    #[test]
    fn a_message_without_required_signatures_has_no_fee_payer_or_signer() {
        let mut tx = decoded(0);
        tx.header.num_required_signatures = 0;
        assert!(tx.signers().is_empty());
        assert_eq!(tx.role_of(&key(1)), Some(MatchRole::AccountKey));
        tx.header.num_required_signatures = 9;
        assert_eq!(tx.signers().len(), 4);
    }

    #[test]
    fn account_keys_list_static_then_loaded_writable_then_loaded_readonly() {
        let mut tx = decoded(0);
        tx.loaded_writable = vec![key(5)];
        tx.loaded_readonly = vec![key(6)];
        let order: Vec<ChainAddress> = tx.account_keys().copied().collect();
        assert_eq!(order, vec![key(1), key(2), key(3), key(4), key(5), key(6)]);
        assert_eq!(tx.key_at(4), Some(&key(5)));
        assert_eq!(tx.key_at(5), Some(&key(6)));
        assert_eq!(tx.key_at(6), None);
        assert_eq!(tx.signers(), &[key(1), key(2)]);
    }
}
