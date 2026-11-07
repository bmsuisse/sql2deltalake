//! Arrow types Delta can store: what arrowsql delivers is cast batch by batch on the way to the
//! writer (no large types, `time` as `HH:MM:SS[.f...]` text, UTC timestamps, `tinyint` as int16).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};

/// `time` without a length is `time(7)`.
pub const TIME_DEFAULT_SCALE: u8 = 7;

fn target_type(t: &DataType) -> DataType {
    match t {
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::Time32(_) | DataType::Time64(_) => DataType::Utf8,
        DataType::Timestamp(unit, Some(tz)) if tz.as_ref() != "UTC" => DataType::Timestamp(*unit, Some("UTC".into())),
        DataType::UInt8 => DataType::Int16,
        other => other.clone(),
    }
}

/// Casts here fail on a value they cannot convert instead of turning it into NULL (arrow's
/// default), as pyarrow's cast did: a wrong schema must stop the load, not write NULLs.
const STRICT: arrow_cast::CastOptions<'static> =
    arrow_cast::CastOptions { safe: false, format_options: arrow_cast::display::FormatOptions::new() };

/// `arrow_cast::cast` that fails on a value it cannot convert instead of making it NULL.
pub fn strict_cast(col: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    arrow_cast::cast_with_options(col, to, &STRICT)
}

/// The schema written for a source schema; equal to it if nothing needs a cast.
pub fn normalize_schema(schema: &Schema) -> Schema {
    let fields: Vec<Field> =
        schema.fields().iter().map(|f| f.as_ref().clone().with_data_type(target_type(f.data_type()))).collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

pub fn needs_normalize(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| &target_type(f.data_type()) != f.data_type())
}

pub fn has_time_columns(schema: &Schema) -> bool {
    schema.fields().iter().any(|f| matches!(f.data_type(), DataType::Time32(_) | DataType::Time64(_)))
}

fn time_nanos(col: &ArrayRef) -> Vec<Option<i64>> {
    let (values, mul): (Vec<Option<i64>>, i64) = match col.data_type() {
        DataType::Time32(TimeUnit::Second) => {
            (col.as_primitive::<Time32SecondType>().iter().map(|v| v.map(i64::from)).collect(), 1_000_000_000)
        }
        DataType::Time32(_) => {
            (col.as_primitive::<Time32MillisecondType>().iter().map(|v| v.map(i64::from)).collect(), 1_000_000)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            (col.as_primitive::<Time64MicrosecondType>().iter().collect(), 1_000)
        }
        _ => (col.as_primitive::<Time64NanosecondType>().iter().collect(), 1),
    };
    values.into_iter().map(|v| v.map(|x| x * mul)).collect()
}

/// `HH:MM:SS[.f...]` with `scale` fractional digits, SQL Server's text of a `time(scale)`.
pub fn format_time(col: &ArrayRef, scale: u8) -> ArrayRef {
    let scale = scale.min(9) as u32;
    let mut b = StringBuilder::with_capacity(col.len(), col.len() * (9 + scale as usize));
    for v in time_nanos(col) {
        match v {
            None => b.append_null(),
            Some(ns) => {
                let secs = ns.div_euclid(1_000_000_000);
                let mut s = format!("{:02}:{:02}:{:02}", secs / 3600, (secs / 60) % 60, secs % 60);
                if scale > 0 {
                    let frac = ns.rem_euclid(1_000_000_000) / 10i64.pow(9 - scale);
                    s.push_str(&format!(".{frac:0width$}", width = scale as usize));
                }
                b.append_value(s);
            }
        }
    }
    Arc::new(b.finish())
}

/// Casts one batch to `target` (from [`normalize_schema`]). `time_scales`: scale of the time
/// columns by position, `time(7)` if missing.
pub fn normalize_batch(
    batch: &RecordBatch,
    target: &SchemaRef,
    time_scales: &HashMap<usize, u8>,
) -> Result<RecordBatch, ArrowError> {
    let columns = batch
        .columns()
        .iter()
        .zip(target.fields())
        .enumerate()
        .map(|(i, (col, field))| match col.data_type() {
            DataType::Time32(_) | DataType::Time64(_) if field.data_type() == &DataType::Utf8 => {
                Ok(format_time(col, *time_scales.get(&i).unwrap_or(&TIME_DEFAULT_SCALE)))
            }
            t if t == field.data_type() => Ok(col.clone()),
            _ => strict_cast(col, field.data_type()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(
        target.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

/// A reader that normalizes the batches of `inner` while they stream through.
pub struct NormalizedReader {
    inner: Box<dyn RecordBatchReader + Send>,
    target: SchemaRef,
    time_scales: HashMap<usize, u8>,
}

impl NormalizedReader {
    pub fn new(inner: Box<dyn RecordBatchReader + Send>, time_scales: HashMap<usize, u8>) -> Self {
        let target = Arc::new(normalize_schema(&inner.schema()));
        NormalizedReader { inner, target, time_scales }
    }

    /// `inner` unchanged if its schema needs no cast.
    pub fn wrap(
        inner: Box<dyn RecordBatchReader + Send>,
        time_scales: HashMap<usize, u8>,
    ) -> Box<dyn RecordBatchReader + Send> {
        if needs_normalize(&inner.schema()) { Box::new(NormalizedReader::new(inner, time_scales)) } else { inner }
    }
}

impl Iterator for NormalizedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|b| b.and_then(|b| normalize_batch(&b, &self.target, &self.time_scales)))
    }
}

impl RecordBatchReader for NormalizedReader {
    fn schema(&self) -> SchemaRef {
        self.target.clone()
    }
}

/// A reader casting the batches of `inner` to `target` (names and types of `target`), for
/// example to the Arrow schema a Spark data source has to deliver. Columns are matched by name
/// (exact, else case-insensitive when that is unambiguous), never by position: a schema planned
/// for `SELECT a, b` must not take the values of `SELECT b, a`. A value the cast cannot convert
/// is an error.
pub struct ConformReader {
    inner: Box<dyn RecordBatchReader + Send>,
    target: SchemaRef,
    /// position in `inner` of each column of `target`
    sources: Vec<usize>,
}

/// The position in `source` of each field of `target`, by name.
pub fn match_by_name(source: &Schema, target: &Schema) -> Result<Vec<usize>, ArrowError> {
    if source.fields().len() != target.fields().len() {
        return Err(ArrowError::SchemaError(format!(
            "the source has {} columns, the target schema {}",
            source.fields().len(),
            target.fields().len()
        )));
    }
    let names: Vec<&str> = source.fields().iter().map(|f| f.name().as_str()).collect();
    let mut used = vec![false; names.len()];
    let mut out = Vec::with_capacity(names.len());
    for f in target.fields() {
        let exact: Vec<usize> = (0..names.len()).filter(|&i| names[i] == f.name()).collect();
        let found = if exact.len() == 1 {
            exact
        } else if exact.is_empty() {
            (0..names.len()).filter(|&i| names[i].eq_ignore_ascii_case(f.name())).collect()
        } else {
            exact
        };
        match found.as_slice() {
            [i] if !used[*i] => {
                used[*i] = true;
                out.push(*i);
            }
            [] => {
                return Err(ArrowError::SchemaError(format!(
                    "column {:?} of the target schema is not in the source (source columns: {})",
                    f.name(),
                    names.join(", ")
                )));
            }
            _ => {
                return Err(ArrowError::SchemaError(format!(
                    "column {:?} of the target schema matches more than one source column",
                    f.name()
                )));
            }
        }
    }
    Ok(out)
}

impl ConformReader {
    pub fn wrap(inner: Box<dyn RecordBatchReader + Send>, target: SchemaRef) -> Result<Self, ArrowError> {
        let sources = match_by_name(&inner.schema(), &target)?;
        Ok(ConformReader { inner, target, sources })
    }
}

impl Iterator for ConformReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|b| {
            b.and_then(|b| {
                let columns = self
                    .sources
                    .iter()
                    .zip(self.target.fields())
                    .map(|(&i, f)| {
                        let c = b.column(i);
                        if c.data_type() == f.data_type() { Ok(c.clone()) } else { strict_cast(c, f.data_type()) }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                RecordBatch::try_new_with_options(
                    self.target.clone(),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(b.num_rows())),
                )
            })
        })
    }
}

impl RecordBatchReader for ConformReader {
    fn schema(&self) -> SchemaRef {
        self.target.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Time64NanosecondArray, UInt8Array};

    #[test]
    fn time_keeps_digits_per_scale() {
        let t: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![
            Some((10 * 3600 + 1) * 1_000_000_000 + 123_456_700),
            None,
            Some(23 * 3600 * 1_000_000_000 + 500_000_000),
        ]));
        let s7 = format_time(&t, 7);
        let s7 = s7.as_string::<i32>();
        assert_eq!(s7.value(0), "10:00:01.1234567");
        assert!(s7.is_null(1));
        assert_eq!(s7.value(2), "23:00:00.5000000");
        let s3 = format_time(&t, 3);
        assert_eq!(s3.as_string::<i32>().value(0), "10:00:01.123");
        let s0 = format_time(&t, 0);
        assert_eq!(s0.as_string::<i32>().value(2), "23:00:00");
    }

    #[test]
    fn schema_and_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::LargeUtf8, true),
            Field::new("n", DataType::UInt8, true),
            Field::new("dto", DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())), true),
            Field::new("d", DataType::Decimal128(18, 2), true),
        ]));
        let target = Arc::new(normalize_schema(&schema));
        assert_eq!(target.field(0).data_type(), &DataType::Utf8);
        assert_eq!(target.field(1).data_type(), &DataType::Int16);
        assert_eq!(target.field(2).data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
        assert_eq!(target.field(3).data_type(), &DataType::Decimal128(18, 2));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::LargeStringArray::from(vec![Some("a"), None])),
                Arc::new(UInt8Array::from(vec![Some(255), None])),
                Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![Some(1), None]).with_timezone("+00:00")),
                Arc::new(arrow_array::Decimal128Array::from(vec![None, None]).with_precision_and_scale(18, 2).unwrap()),
            ],
        )
        .unwrap();
        let out = normalize_batch(&batch, &target, &HashMap::new()).unwrap();
        assert_eq!(out.schema(), target);
        assert_eq!(out.column(1).as_primitive::<arrow_array::types::Int16Type>().value(0), 255);
    }

    fn ints(name: &str, values: Vec<i32>) -> (Field, ArrayRef) {
        (Field::new(name, DataType::Int32, true), Arc::new(arrow_array::Int32Array::from(values)))
    }

    #[test]
    fn conform_matches_columns_by_name() {
        let (fb, b) = ints("b", vec![20, 21]);
        let (fa, a) = ints("A", vec![10, 11]);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![fb, fa])), vec![b, a]).unwrap();
        let inner = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let target =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true), Field::new("b", DataType::Int64, true)]));
        let mut reader = ConformReader::wrap(Box::new(inner), target.clone()).unwrap();
        let out = reader.next().unwrap().unwrap();
        assert_eq!(out.schema(), target);
        let col = |i: usize| out.column(i).as_primitive::<arrow_array::types::Int64Type>().values().to_vec();
        assert_eq!(col(0), vec![10, 11]);
        assert_eq!(col(1), vec![20, 21]);
    }

    #[test]
    fn conform_rejects_a_missing_column() {
        let (fa, a) = ints("a", vec![1]);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![fa])), vec![a]).unwrap();
        let inner = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let target = Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, true)]));
        let err = ConformReader::wrap(Box::new(inner), target).err().unwrap();
        assert!(err.to_string().contains("\"c\""), "{err}");
    }

    #[test]
    fn conform_fails_on_a_value_the_cast_cannot_convert() {
        let field = Field::new("a", DataType::Int64, true);
        let col: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![Some(1), Some(1 << 40)]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![col]).unwrap();
        let inner = arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let target = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let mut reader = ConformReader::wrap(Box::new(inner), target).unwrap();
        let err = reader.next().unwrap().expect_err("an out-of-range value must not become NULL");
        assert!(err.to_string().contains("1099511627776"), "{err}");
    }

    #[test]
    fn normalize_fails_on_a_value_the_cast_cannot_convert() {
        let source = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let col: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![Some("12"), Some("x")]));
        let batch = RecordBatch::try_new(Arc::new(source), vec![col]).unwrap();
        let target = Arc::new(Schema::new(vec![Field::new("s", DataType::Int32, true)]));
        assert!(normalize_batch(&batch, &target, &HashMap::new()).is_err());
    }
}
