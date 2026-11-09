#[cfg(feature = "parquet")]
mod roundtrip {
    use aetelier_io::naming::{FileKind, parse_file_name};
    use aetelier_io::onchain::{
        read_transaction_keys, read_transactions_parquet, write_transactions_parquet,
        write_transactions_parquet_timestamped,
    };
    use aetelier_types::onchain::{
        ChainAddress, Commitment, MatchRole, Transaction, TransactionFailure,
        TransactionVersion,
    };
    use tempfile::tempdir;

    const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
    const WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

    fn base(address: &str, slot: u64, tx_index: u64, signature: &str) -> Transaction {
        Transaction {
            exchange: "solana".to_string(),
            address: ChainAddress::solana(address).unwrap(),
            match_role: MatchRole::AccountKey,
            source_tx_ts_us: 1_791_321_067_000_000,
            local_tx_ts_us: 1_791_321_067_420_000,
            source_tx_rtt_us: 0,
            recv_seq: tx_index + 1,
            conn_epoch_us: 1_791_321_000_000_000,
            commitment: Commitment::Finalized,
            slot,
            block_height: slot - 22_000_000,
            blockhash: "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn".to_string(),
            tx_index,
            signature: signature.to_string(),
            version: TransactionVersion::V0,
            success: true,
            fee_lamports: 5_000,
            compute_units_consumed: Some(41_215),
            compute_unit_limit: Some(200_000),
            compute_unit_price_micro_lamports: Some(1_000),
            priority_fee_lamports: None,
            cost_units: Some(1_492),
            signers: vec!["9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM".to_string()],
            account_keys: vec![
                "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM".to_string(),
                JUPITER_V6.to_string(),
                "ComputeBudget111111111111111111111111111111".to_string(),
            ],
            log_messages: None,
            failure: None,
        }
    }

    fn corpus() -> Vec<Transaction> {
        let success = base(JUPITER_V6, 380_000_123, 4, "sig-a");

        let mut failed = base(JUPITER_V6, 380_000_123, 9, "sig-b");
        failed.success = false;
        failed.match_role = MatchRole::FeePayer;
        failed.compute_units_consumed = None;
        failed.log_messages = Some(vec![
            "Program JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 invoke [1]".to_string(),
            "Program log: Error: slippage tolerance exceeded ✗".to_string(),
        ]);
        failed.failure = Some(TransactionFailure {
            error: "InstructionError".to_string(),
            instruction_index: Some(2),
            program_id: Some(JUPITER_V6.to_string()),
            instruction_error: Some("Custom(6001)".to_string()),
            instruction_data: Some("3Bxs4h24hBtQy9rw".to_string()),
            log_program_id: Some(JUPITER_V6.to_string()),
            log_message: Some("Error: slippage tolerance exceeded ✗".to_string()),
        });

        let mut v1 = base(WHIRLPOOL, 380_000_124, 0, "sig-c");
        v1.version = TransactionVersion::V1;
        v1.match_role = MatchRole::Signer;
        v1.compute_unit_price_micro_lamports = None;
        v1.compute_unit_limit = Some(0);
        v1.priority_fee_lamports = Some(12_000);
        v1.cost_units = None;
        v1.log_messages = Some(Vec::new());
        v1.signers = Vec::new();

        vec![success, failed, v1]
    }

    #[test]
    fn every_column_round_trips_including_nulls_and_empty_lists() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("txs.parquet");
        let written = corpus();
        for tx in &written {
            tx.check_invariants().unwrap();
        }
        write_transactions_parquet(&written, &path).unwrap();
        assert_eq!(read_transactions_parquet(&path).unwrap(), written);
    }

    #[test]
    fn keys_reader_returns_each_row_key_in_file_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("txs.parquet");
        let mut written = corpus();
        written.push(base(JUPITER_V6, 380_000_125, 0, "sig-c"));
        write_transactions_parquet(&written, &path).unwrap();

        let keys = read_transaction_keys(&path).unwrap();

        let address = |a: &str| ChainAddress::solana(a).unwrap();
        assert_eq!(
            keys,
            vec![
                (address(JUPITER_V6), "sig-a".to_string()),
                (address(JUPITER_V6), "sig-b".to_string()),
                (address(WHIRLPOOL), "sig-c".to_string()),
                (address(JUPITER_V6), "sig-c".to_string()),
            ]
        );
    }

    #[test]
    fn timestamped_writer_names_files_by_the_base58_address() {
        let dir = tempdir().unwrap();
        let written = corpus();
        let path =
            write_transactions_parquet_timestamped(&written[..1], dir.path(), "sync")
                .unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        let parsed = parse_file_name(name).unwrap();
        assert_eq!(
            (parsed.exchange, parsed.symbol, parsed.kind, parsed.mode),
            ("solana", JUPITER_V6, FileKind::Transactions, "sync")
        );
        assert_eq!(parsed.stamp.timestamp_micros(), 1_791_321_067_000_000);
        assert_eq!(read_transactions_parquet(&path).unwrap(), written[..1]);
    }

    fn rewrite_column(
        source: &std::path::Path,
        target: &std::path::Path,
        name: &str,
        value: &str,
    ) {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::sync::Arc;

        let batch = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(source).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
        let at = batch.schema().index_of(name).unwrap();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns[at] = Arc::new(StringArray::from(vec![value; batch.num_rows()]));
        let tampered = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(target).unwrap(),
            tampered.schema(),
            None,
        )
        .unwrap();
        writer.write(&tampered).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn reader_rejects_unknown_enum_values_and_malformed_addresses() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("good.parquet");
        write_transactions_parquet(&corpus()[..1], &path).unwrap();

        for (column, value, expected) in [
            ("commitment", "processed", "unknown commitment 'processed'"),
            ("version", "v2", "unknown version 'v2'"),
            ("match_role", "owner", "unknown match role 'owner'"),
            ("address", "not-base58", "malformed address 'not-base58'"),
        ] {
            let tampered = dir.path().join(format!("{column}.parquet"));
            rewrite_column(&path, &tampered, column, value);
            let err = read_transactions_parquet(&tampered)
                .unwrap_err()
                .to_string();
            assert!(err.contains(expected), "{column}: {err}");
        }
    }

    #[test]
    fn reader_names_a_missing_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("trades.parquet");
        let trade = aetelier_types::trades::Trade {
            source_trade_ts_us: 1,
            local_trade_ts_us: 1,
            source_trade_rtt_us: 0,
            pair: aetelier_types::trading_pair::TradingPair::new("SOL", "USDC"),
            side: aetelier_types::TradeSide::Buy,
            amount: aetelier_types::orderbooks::f64_to_decimal(1.0),
            price: aetelier_types::orderbooks::f64_to_decimal(1.0),
            exchange: "binance".to_string(),
            id: "t".to_string(),
            origin: Default::default(),
        };
        aetelier_io::trades::write_trades_parquet(&[trade], &path).unwrap();
        let err = read_transactions_parquet(&path).unwrap_err().to_string();
        assert!(err.contains("missing column 'source_tx_ts_us'"), "{err}");
    }
}
