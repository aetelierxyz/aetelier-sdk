use std::fmt::Write as _;

use aetelier_types::onchain::{ChainAddress, TransactionFailure};

use super::decoded::DecodedTransaction;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FailedProgramLog {
    program_id: ChainAddress,
    message: String,
}

fn failed_program_log(logs: &[String]) -> Option<FailedProgramLog> {
    logs.iter().find_map(|line| {
        let (program, message) =
            line.strip_prefix("Program ")?.split_once(" failed: ")?;
        let program_id = ChainAddress::solana(program).ok()?;
        Some(FailedProgramLog {
            program_id,
            message: message.to_string(),
        })
    })
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn raw_error_text(raw: &[u8]) -> String {
    format!("0x{}", hex_bytes(raw))
}

impl DecodedTransaction {
    pub fn failure(&self) -> Option<TransactionFailure> {
        let error = self.error.as_ref()?;
        let decoded = error
            .decoded
            .as_ref()
            .filter(|decoded| !decoded.error.is_empty());
        let fault = decoded.and_then(|decoded| decoded.instruction.as_ref());
        let instruction =
            fault.and_then(|fault| self.instructions.get(fault.index as usize));
        let logged = self.log_messages.as_deref().and_then(failed_program_log);
        Some(TransactionFailure {
            error: decoded.map_or_else(
                || raw_error_text(&error.raw),
                |decoded| decoded.error.clone(),
            ),
            instruction_index: fault.map(|fault| fault.index),
            program_id: instruction
                .and_then(|instruction| self.key_at(instruction.program_id_index))
                .map(ChainAddress::to_base58),
            instruction_error: fault.map(|fault| fault.error.clone()),
            instruction_data: instruction.map(|instruction| hex_bytes(&instruction.data)),
            log_program_id: logged.as_ref().map(|log| log.program_id.to_base58()),
            log_message: logged.map(|log| log.message),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::onchain::decoded::fixtures::{decoded, key};
    use crate::framework::onchain::decoded::{
        CompiledInstruction, DecodedError, ExecutionError, InstructionFault,
    };

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
    const WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

    fn failed_at(index: u32) -> DecodedTransaction {
        let mut tx = decoded(0);
        tx.instructions = vec![
            CompiledInstruction {
                program_id_index: 3,
                data: vec![2, 0, 0, 1, 0],
            },
            CompiledInstruction {
                program_id_index: 2,
                data: vec![0xe5, 0x17, 0xcb, 0x97],
            },
        ];
        tx.error = Some(ExecutionError {
            raw: vec![8, 0, 0, 0, 1, 25, 0, 0, 0, 113, 23, 0, 0],
            decoded: Some(DecodedError {
                error: format!("InstructionError({index}, Custom(6001))"),
                instruction: Some(InstructionFault {
                    index,
                    error: "Custom(6001)".to_string(),
                }),
            }),
        });
        tx.log_messages = Some(vec![
            format!("Program {JUPITER_V6} invoke [1]"),
            "Program log: Error: something failed: in a log line".to_string(),
            format!("Program {WHIRLPOOL} invoke [2]"),
            format!("Program {WHIRLPOOL} failed: custom program error: 0x1771"),
            format!("Program {JUPITER_V6} failed: custom program error: 0x1771"),
        ]);
        tx
    }

    #[test]
    fn a_successful_transaction_has_no_failure() {
        assert_eq!(decoded(0).failure(), None);
    }

    #[test]
    fn an_instruction_error_resolves_program_and_data_of_that_instruction() {
        let failure = failed_at(1).failure().unwrap();
        assert_eq!(failure.error, "InstructionError(1, Custom(6001))");
        assert_eq!(failure.instruction_index, Some(1));
        assert_eq!(failure.program_id, Some(key(3).to_base58()));
        assert_eq!(failure.instruction_error.as_deref(), Some("Custom(6001)"));
        assert_eq!(failure.instruction_data.as_deref(), Some("e517cb97"));
        assert_eq!(failure.log_program_id.as_deref(), Some(WHIRLPOOL));
        assert_eq!(
            failure.log_message.as_deref(),
            Some("custom program error: 0x1771")
        );
    }

    #[test]
    fn an_out_of_range_instruction_index_keeps_the_index_without_a_program() {
        let failure = failed_at(7).failure().unwrap();
        assert_eq!(failure.instruction_index, Some(7));
        assert_eq!(failure.program_id, None);
        assert_eq!(failure.instruction_data, None);
    }

    #[test]
    fn a_transaction_level_error_has_no_instruction_fields() {
        let mut tx = decoded(0);
        tx.error = Some(ExecutionError {
            raw: vec![1, 0, 0, 0],
            decoded: Some(DecodedError {
                error: "AccountInUse".to_string(),
                instruction: None,
            }),
        });
        tx.log_messages = None;
        let failure = tx.failure().unwrap();
        assert_eq!(
            failure,
            TransactionFailure {
                error: "AccountInUse".to_string(),
                ..TransactionFailure::default()
            }
        );
    }

    #[test]
    fn an_undecodable_error_keeps_its_raw_bytes_as_hex() {
        let mut tx = failed_at(1);
        tx.error = Some(ExecutionError {
            raw: vec![0xff, 0x00, 0x7f],
            decoded: None,
        });
        let failure = tx.failure().unwrap();
        assert_eq!(failure.error, "0xff007f");
        assert_eq!(failure.instruction_index, None);
        assert_eq!(failure.log_program_id.as_deref(), Some(WHIRLPOOL));
        tx.error = Some(ExecutionError {
            raw: Vec::new(),
            decoded: Some(DecodedError {
                error: String::new(),
                instruction: None,
            }),
        });
        assert_eq!(tx.failure().unwrap().error, "0x");
    }

    #[test]
    fn absent_or_truncated_logs_leave_the_log_fields_null() {
        let mut tx = failed_at(1);
        tx.log_messages = None;
        let failure = tx.failure().unwrap();
        assert_eq!((failure.log_program_id, failure.log_message), (None, None));
        tx.log_messages = Some(vec![
            format!("Program {JUPITER_V6} invoke [1]"),
            "Log truncated".to_string(),
        ]);
        assert_eq!(tx.failure().unwrap().log_program_id, None);
    }

    #[test]
    fn a_failed_line_must_name_a_valid_program_address() {
        let logs = vec![
            "Program log: custom failed: not a program".to_string(),
            "Program 0OIl failed: bad base58".to_string(),
            format!("Program {JUPITER_V6} failed: custom program error: 0x1"),
        ];
        let log = failed_program_log(&logs).unwrap();
        assert_eq!(log.program_id.to_base58(), JUPITER_V6);
        assert_eq!(log.message, "custom program error: 0x1");
    }
}
