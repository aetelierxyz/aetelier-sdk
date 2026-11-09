use std::path::{Path, PathBuf};

use aetelier_types::errors::PersistError;
use aetelier_types::onchain::Transaction;

pub const DATATYPE_DIR: &str = "transactions";

#[cfg(feature = "parquet")]
fn schema() -> arrow::datatypes::Schema {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let utf8_list =
        || DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    Schema::new(vec![
        Field::new("source_tx_ts_us", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("match_role", DataType::Utf8, false),
        Field::new("local_tx_ts_us", DataType::UInt64, false),
        Field::new("source_tx_rtt_us", DataType::UInt64, false),
        Field::new("recv_seq", DataType::UInt64, false),
        Field::new("conn_epoch_us", DataType::UInt64, false),
        Field::new("commitment", DataType::Utf8, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("block_height", DataType::UInt64, false),
        Field::new("blockhash", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt64, false),
        Field::new("signature", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("success", DataType::Boolean, false),
        Field::new("fee_lamports", DataType::UInt64, false),
        Field::new("compute_units_consumed", DataType::UInt64, true),
        Field::new("compute_unit_limit", DataType::UInt32, true),
        Field::new("compute_unit_price_micro_lamports", DataType::UInt64, true),
        Field::new("priority_fee_lamports", DataType::UInt64, true),
        Field::new("cost_units", DataType::UInt64, true),
        Field::new("signers", utf8_list(), false),
        Field::new("account_keys", utf8_list(), false),
        Field::new("log_messages", utf8_list(), true),
        Field::new("failure_error", DataType::Utf8, true),
        Field::new("failure_instruction_index", DataType::UInt32, true),
        Field::new("failure_program_id", DataType::Utf8, true),
        Field::new("failure_instruction_error", DataType::Utf8, true),
        Field::new("failure_instruction_data", DataType::Utf8, true),
        Field::new("failure_log_program_id", DataType::Utf8, true),
        Field::new("failure_log_message", DataType::Utf8, true),
    ])
}

#[cfg(feature = "parquet")]
fn utf8_list_array<'a, I>(rows: I, nullable: bool) -> arrow::array::ListArray
where
    I: IntoIterator<Item = Option<&'a [String]>>,
{
    use arrow::array::{ListBuilder, StringBuilder};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    let mut builder = ListBuilder::new(StringBuilder::new())
        .with_field(Arc::new(Field::new("item", DataType::Utf8, false)));
    for row in rows {
        match row {
            Some(values) => {
                for value in values {
                    builder.values().append_value(value);
                }
                builder.append(true);
            }
            None if nullable => builder.append_null(),
            None => builder.append(true),
        }
    }
    builder.finish()
}

#[cfg(feature = "parquet")]
pub fn write_transactions_parquet(
    records: &[Transaction],
    path: &Path,
) -> Result<(), PersistError> {
    use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt32Array, UInt64Array};
    use arrow::record_batch::RecordBatch;
    use parquet::{
        arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
    };
    use std::{fs::File, sync::Arc};

    let u64s = |f: fn(&Transaction) -> u64| -> ArrayRef {
        Arc::new(UInt64Array::from_iter_values(records.iter().map(f)))
    };
    let opt_u64s = |f: fn(&Transaction) -> Option<u64>| -> ArrayRef {
        Arc::new(records.iter().map(f).collect::<UInt64Array>())
    };
    let strs = |f: for<'a> fn(&'a Transaction) -> &'a str| -> ArrayRef {
        Arc::new(records.iter().map(|t| Some(f(t))).collect::<StringArray>())
    };
    let failure_strs = |f: for<'a> fn(&'a Transaction) -> Option<&'a str>| -> ArrayRef {
        Arc::new(records.iter().map(f).collect::<StringArray>())
    };
    let addresses: Vec<String> = records.iter().map(|t| t.address.to_base58()).collect();

    let columns: Vec<ArrayRef> = vec![
        u64s(|t| t.source_tx_ts_us),
        Arc::new(StringArray::from_iter_values(addresses.iter())),
        strs(|t| t.exchange.as_str()),
        strs(|t| t.match_role.as_str()),
        u64s(|t| t.local_tx_ts_us),
        u64s(|t| t.source_tx_rtt_us),
        u64s(|t| t.recv_seq),
        u64s(|t| t.conn_epoch_us),
        strs(|t| t.commitment.as_str()),
        u64s(|t| t.slot),
        u64s(|t| t.block_height),
        strs(|t| t.blockhash.as_str()),
        u64s(|t| t.tx_index),
        strs(|t| t.signature.as_str()),
        strs(|t| t.version.as_str()),
        Arc::new(BooleanArray::from_iter(
            records.iter().map(|t| Some(t.success)),
        )),
        u64s(|t| t.fee_lamports),
        opt_u64s(|t| t.compute_units_consumed),
        Arc::new(
            records
                .iter()
                .map(|t| t.compute_unit_limit)
                .collect::<UInt32Array>(),
        ),
        opt_u64s(|t| t.compute_unit_price_micro_lamports),
        opt_u64s(|t| t.priority_fee_lamports),
        opt_u64s(|t| t.cost_units),
        Arc::new(utf8_list_array(
            records.iter().map(|t| Some(t.signers.as_slice())),
            false,
        )),
        Arc::new(utf8_list_array(
            records.iter().map(|t| Some(t.account_keys.as_slice())),
            false,
        )),
        Arc::new(utf8_list_array(
            records.iter().map(|t| t.log_messages.as_deref()),
            true,
        )),
        failure_strs(|t| t.failure.as_ref().map(|f| f.error.as_str())),
        Arc::new(
            records
                .iter()
                .map(|t| t.failure.as_ref().and_then(|f| f.instruction_index))
                .collect::<UInt32Array>(),
        ),
        failure_strs(|t| t.failure.as_ref().and_then(|f| f.program_id.as_deref())),
        failure_strs(|t| {
            t.failure
                .as_ref()
                .and_then(|f| f.instruction_error.as_deref())
        }),
        failure_strs(|t| {
            t.failure
                .as_ref()
                .and_then(|f| f.instruction_data.as_deref())
        }),
        failure_strs(|t| t.failure.as_ref().and_then(|f| f.log_program_id.as_deref())),
        failure_strs(|t| t.failure.as_ref().and_then(|f| f.log_message.as_deref())),
    ];

    let batch = RecordBatch::try_new(Arc::new(schema()), columns)
        .map_err(crate::parquet_err::from_arrow)?;
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(crate::parquet_err::from_parquet)?;
    writer
        .write(&batch)
        .map_err(crate::parquet_err::from_parquet)?;
    writer.close().map_err(crate::parquet_err::from_parquet)?;
    Ok(())
}

#[cfg(not(feature = "parquet"))]
pub fn write_transactions_parquet(
    _records: &[Transaction],
    _path: &Path,
) -> Result<(), PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

#[cfg(feature = "parquet")]
struct Columns<'a> {
    batch: &'a arrow::record_batch::RecordBatch,
    path: &'a Path,
}

#[cfg(feature = "parquet")]
impl<'a> Columns<'a> {
    fn get(&self, name: &str) -> Result<&'a arrow::array::ArrayRef, PersistError> {
        self.batch.column_by_name(name).ok_or_else(|| {
            PersistError::Parse(format!(
                "missing column '{name}' in {}",
                self.path.display()
            ))
        })
    }

    fn mismatch(&self, name: &str, want: &str) -> PersistError {
        PersistError::Parse(format!(
            "column '{name}' is not {want} in {}",
            self.path.display()
        ))
    }

    fn u64s(&self, name: &str) -> Result<&'a arrow::array::UInt64Array, PersistError> {
        use arrow::array::AsArray;
        self.get(name)?
            .as_primitive_opt::<arrow::datatypes::UInt64Type>()
            .ok_or_else(|| self.mismatch(name, "UInt64"))
    }

    fn u32s(&self, name: &str) -> Result<&'a arrow::array::UInt32Array, PersistError> {
        use arrow::array::AsArray;
        self.get(name)?
            .as_primitive_opt::<arrow::datatypes::UInt32Type>()
            .ok_or_else(|| self.mismatch(name, "UInt32"))
    }

    fn strs(&self, name: &str) -> Result<&'a arrow::array::StringArray, PersistError> {
        use arrow::array::AsArray;
        self.get(name)?
            .as_string_opt::<i32>()
            .ok_or_else(|| self.mismatch(name, "Utf8"))
    }

    fn bools(&self, name: &str) -> Result<&'a arrow::array::BooleanArray, PersistError> {
        use arrow::array::AsArray;
        self.get(name)?
            .as_boolean_opt()
            .ok_or_else(|| self.mismatch(name, "Boolean"))
    }

    fn lists(&self, name: &str) -> Result<&'a arrow::array::ListArray, PersistError> {
        use arrow::array::AsArray;
        self.get(name)?
            .as_list_opt::<i32>()
            .ok_or_else(|| self.mismatch(name, "List<Utf8>"))
    }
}

#[cfg(feature = "parquet")]
fn list_row(
    list: &arrow::array::ListArray,
    i: usize,
    name: &str,
    path: &Path,
) -> Result<Option<Vec<String>>, PersistError> {
    use arrow::array::{Array, AsArray};
    if list.is_null(i) {
        return Ok(None);
    }
    let values = list.value(i);
    let strings = values.as_string_opt::<i32>().ok_or_else(|| {
        PersistError::Parse(format!(
            "column '{name}' items are not Utf8 in {}",
            path.display()
        ))
    })?;
    Ok(Some(
        strings
            .iter()
            .map(|s| s.unwrap_or_default().to_string())
            .collect(),
    ))
}

#[cfg(feature = "parquet")]
fn opt_value<A: arrow::array::Array, T>(
    array: &A,
    i: usize,
    value: impl Fn(usize) -> T,
) -> Option<T> {
    array.is_valid(i).then(|| value(i))
}

#[cfg(feature = "parquet")]
pub fn read_transactions_parquet(path: &Path) -> Result<Vec<Transaction>, PersistError> {
    use aetelier_types::onchain::{ChainAddress, TransactionFailure};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
        .map_err(crate::parquet_err::from_parquet)?
        .build()
        .map_err(crate::parquet_err::from_parquet)?;

    let mut records = Vec::new();
    for batch in reader {
        let batch = batch.map_err(crate::parquet_err::from_arrow)?;
        let cols = Columns {
            batch: &batch,
            path,
        };
        let source_ts = cols.u64s("source_tx_ts_us")?;
        let addresses = cols.strs("address")?;
        let exchanges = cols.strs("exchange")?;
        let roles = cols.strs("match_role")?;
        let local_ts = cols.u64s("local_tx_ts_us")?;
        let rtts = cols.u64s("source_tx_rtt_us")?;
        let recv_seqs = cols.u64s("recv_seq")?;
        let epochs = cols.u64s("conn_epoch_us")?;
        let commitments = cols.strs("commitment")?;
        let slots = cols.u64s("slot")?;
        let heights = cols.u64s("block_height")?;
        let blockhashes = cols.strs("blockhash")?;
        let tx_indexes = cols.u64s("tx_index")?;
        let signatures = cols.strs("signature")?;
        let versions = cols.strs("version")?;
        let successes = cols.bools("success")?;
        let fees = cols.u64s("fee_lamports")?;
        let consumed = cols.u64s("compute_units_consumed")?;
        let limits = cols.u32s("compute_unit_limit")?;
        let prices = cols.u64s("compute_unit_price_micro_lamports")?;
        let priority_fees = cols.u64s("priority_fee_lamports")?;
        let cost_units = cols.u64s("cost_units")?;
        let signers = cols.lists("signers")?;
        let account_keys = cols.lists("account_keys")?;
        let logs = cols.lists("log_messages")?;
        let f_error = cols.strs("failure_error")?;
        let f_ix = cols.u32s("failure_instruction_index")?;
        let f_program = cols.strs("failure_program_id")?;
        let f_ix_error = cols.strs("failure_instruction_error")?;
        let f_ix_data = cols.strs("failure_instruction_data")?;
        let f_log_program = cols.strs("failure_log_program_id")?;
        let f_log_message = cols.strs("failure_log_message")?;

        let base = records.len();
        let parse_err = |i: usize, what: &str, value: &str| {
            PersistError::Parse(format!(
                "row {}: {what} '{value}' in {}",
                base + i,
                path.display()
            ))
        };
        let opt_str = |array: &arrow::array::StringArray, i: usize| {
            opt_value(array, i, |i| array.value(i).to_string())
        };

        for i in 0..batch.num_rows() {
            let failure = opt_value(f_error, i, |i| TransactionFailure {
                error: f_error.value(i).to_string(),
                instruction_index: opt_value(f_ix, i, |i| f_ix.value(i)),
                program_id: opt_str(f_program, i),
                instruction_error: opt_str(f_ix_error, i),
                instruction_data: opt_str(f_ix_data, i),
                log_program_id: opt_str(f_log_program, i),
                log_message: opt_str(f_log_message, i),
            });
            records.push(Transaction {
                exchange: exchanges.value(i).to_string(),
                address: ChainAddress::solana(addresses.value(i))
                    .map_err(|_| parse_err(i, "malformed address", addresses.value(i)))?,
                match_role: roles
                    .value(i)
                    .parse()
                    .map_err(|_| parse_err(i, "unknown match role", roles.value(i)))?,
                source_tx_ts_us: source_ts.value(i),
                local_tx_ts_us: local_ts.value(i),
                source_tx_rtt_us: rtts.value(i),
                recv_seq: recv_seqs.value(i),
                conn_epoch_us: epochs.value(i),
                commitment: commitments.value(i).parse().map_err(|_| {
                    parse_err(i, "unknown commitment", commitments.value(i))
                })?,
                slot: slots.value(i),
                block_height: heights.value(i),
                blockhash: blockhashes.value(i).to_string(),
                tx_index: tx_indexes.value(i),
                signature: signatures.value(i).to_string(),
                version: versions
                    .value(i)
                    .parse()
                    .map_err(|_| parse_err(i, "unknown version", versions.value(i)))?,
                success: successes.value(i),
                fee_lamports: fees.value(i),
                compute_units_consumed: opt_value(consumed, i, |i| consumed.value(i)),
                compute_unit_limit: opt_value(limits, i, |i| limits.value(i)),
                compute_unit_price_micro_lamports: opt_value(prices, i, |i| {
                    prices.value(i)
                }),
                priority_fee_lamports: opt_value(priority_fees, i, |i| {
                    priority_fees.value(i)
                }),
                cost_units: opt_value(cost_units, i, |i| cost_units.value(i)),
                signers: list_row(signers, i, "signers", path)?.unwrap_or_default(),
                account_keys: list_row(account_keys, i, "account_keys", path)?
                    .unwrap_or_default(),
                log_messages: list_row(logs, i, "log_messages", path)?,
                failure,
            });
        }
    }
    Ok(records)
}

#[cfg(not(feature = "parquet"))]
pub fn read_transactions_parquet(_path: &Path) -> Result<Vec<Transaction>, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

#[cfg(feature = "parquet")]
pub fn write_transactions_parquet_timestamped(
    records: &[Transaction],
    output_dir: &Path,
    mode: &str,
) -> Result<PathBuf, PersistError> {
    let file_ts =
        crate::naming::batch_stamp(records.iter().map(Transaction::effective_ts_us));
    let address = records
        .first()
        .map(|t| t.address.to_base58())
        .unwrap_or_else(|| "unknown".to_string());
    let exchange = records
        .first()
        .map(|t| t.exchange.as_str())
        .unwrap_or("unknown");
    let filename = crate::naming::file_name(
        exchange,
        &address,
        crate::naming::FileKind::Transactions,
        mode,
        file_ts,
    );
    let path = crate::naming::unique_path(output_dir, &filename);
    write_transactions_parquet(records, &path)?;
    Ok(path)
}

#[cfg(not(feature = "parquet"))]
pub fn write_transactions_parquet_timestamped(
    _records: &[Transaction],
    _output_dir: &Path,
    _mode: &str,
) -> Result<PathBuf, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}
