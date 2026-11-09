use aetelier_types::onchain::{Chain, ChainAddress};

use super::decoded::DecodedTransaction;

pub const COMPUTE_BUDGET_PROGRAM_ID: ChainAddress = ChainAddress::new(
    Chain::Solana,
    [
        3, 6, 70, 111, 229, 33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140,
        229, 187, 197, 247, 18, 107, 44, 67, 155, 58, 64, 0, 0, 0,
    ],
);

const SET_COMPUTE_UNIT_LIMIT: u8 = 2;
const SET_COMPUTE_UNIT_PRICE: u8 = 3;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestedCompute {
    pub compute_unit_limit: Option<u32>,
    pub compute_unit_price_micro_lamports: Option<u64>,
    pub priority_fee_lamports: Option<u64>,
}

impl DecodedTransaction {
    pub fn requested_compute(&self) -> RequestedCompute {
        match self.config {
            Some(config) => RequestedCompute {
                compute_unit_limit: Some(config.compute_unit_limit.unwrap_or(0)),
                compute_unit_price_micro_lamports: None,
                priority_fee_lamports: Some(config.priority_fee.unwrap_or(0)),
            },
            None => self.compute_budget_instructions(),
        }
    }

    fn compute_budget_instructions(&self) -> RequestedCompute {
        let mut requested = RequestedCompute::default();
        let budget_instructions = self.instructions.iter().filter(|instruction| {
            self.key_at(instruction.program_id_index) == Some(&COMPUTE_BUDGET_PROGRAM_ID)
        });
        for instruction in budget_instructions {
            match instruction.data.split_first() {
                Some((&SET_COMPUTE_UNIT_LIMIT, payload))
                    if requested.compute_unit_limit.is_none() =>
                {
                    requested.compute_unit_limit =
                        <[u8; 4]>::try_from(payload).ok().map(u32::from_le_bytes);
                }
                Some((&SET_COMPUTE_UNIT_PRICE, payload))
                    if requested.compute_unit_price_micro_lamports.is_none() =>
                {
                    requested.compute_unit_price_micro_lamports =
                        <[u8; 8]>::try_from(payload).ok().map(u64::from_le_bytes);
                }
                _ => {}
            }
        }
        requested
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::onchain::decoded::fixtures::{decoded, key};
    use crate::framework::onchain::decoded::{CompiledInstruction, MessageConfig};

    fn budget(data: &[u8]) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: 4,
            data: data.to_vec(),
        }
    }

    fn with_budget_program(instructions: Vec<CompiledInstruction>) -> DecodedTransaction {
        let mut tx = decoded(0);
        tx.static_keys.push(COMPUTE_BUDGET_PROGRAM_ID);
        tx.instructions = instructions;
        tx
    }

    #[test]
    fn the_program_id_is_the_canonical_compute_budget_address() {
        assert_eq!(
            COMPUTE_BUDGET_PROGRAM_ID.to_base58(),
            "ComputeBudget111111111111111111111111111111"
        );
    }

    #[test]
    fn limit_and_price_parse_little_endian_payloads() {
        let tx = with_budget_program(vec![
            budget(&[2, 0x40, 0x0d, 0x03, 0x00]),
            budget(&[3, 0xe8, 0x03, 0, 0, 0, 0, 0, 0]),
        ]);
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
    fn absent_malformed_or_foreign_instructions_leave_the_request_null() {
        assert_eq!(
            with_budget_program(Vec::new()).requested_compute(),
            RequestedCompute::default()
        );
        let malformed = with_budget_program(vec![
            budget(&[2, 1, 2]),
            budget(&[3, 1, 2, 3]),
            budget(&[1]),
        ]);
        assert_eq!(malformed.requested_compute(), RequestedCompute::default());
        let mut foreign = with_budget_program(vec![CompiledInstruction {
            program_id_index: 3,
            data: vec![2, 0x40, 0x0d, 0x03, 0x00],
        }]);
        assert_eq!(foreign.requested_compute(), RequestedCompute::default());
        foreign.instructions[0].program_id_index = 9;
        assert_eq!(foreign.requested_compute(), RequestedCompute::default());
    }

    #[test]
    fn the_first_well_formed_instruction_of_each_kind_wins() {
        let tx = with_budget_program(vec![
            budget(&[2, 1]),
            budget(&[2, 0x10, 0x27, 0, 0]),
            budget(&[2, 0x20, 0x4e, 0, 0]),
        ]);
        assert_eq!(tx.requested_compute().compute_unit_limit, Some(10_000));
    }

    #[test]
    fn instruction_program_ids_resolve_through_the_combined_key_list() {
        let mut tx = decoded(0);
        tx.loaded_readonly = vec![key(8), COMPUTE_BUDGET_PROGRAM_ID];
        tx.instructions = vec![CompiledInstruction {
            program_id_index: 5,
            data: vec![2, 0x40, 0x0d, 0x03, 0x00],
        }];
        assert_eq!(tx.requested_compute().compute_unit_limit, Some(200_000));
    }

    #[test]
    fn v1_reads_the_message_config_and_ignores_budget_instructions() {
        let mut tx = with_budget_program(vec![
            budget(&[2, 0x40, 0x0d, 0x03, 0x00]),
            budget(&[3, 0xe8, 0x03, 0, 0, 0, 0, 0, 0]),
        ]);
        tx.versioned = true;
        tx.config = Some(MessageConfig {
            priority_fee: Some(12_000),
            compute_unit_limit: Some(600_000),
        });
        assert_eq!(
            tx.requested_compute(),
            RequestedCompute {
                compute_unit_limit: Some(600_000),
                compute_unit_price_micro_lamports: None,
                priority_fee_lamports: Some(12_000),
            }
        );
        tx.config = Some(MessageConfig::default());
        assert_eq!(
            tx.requested_compute(),
            RequestedCompute {
                compute_unit_limit: Some(0),
                compute_unit_price_micro_lamports: None,
                priority_fee_lamports: Some(0),
            }
        );
    }
}
