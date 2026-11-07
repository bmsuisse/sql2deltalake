//! Whole tables in memory: the PK/delta-value snapshots are small next to the data.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchIterator, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;

use crate::host::Batches;
use crate::{Error, Result};

pub fn collect(reader: Batches) -> Result<RecordBatch> {
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(concat_batches(&schema, batches.iter())?)
}

/// Index of the column `name`; like DuckDB, an exact match first, then ignoring case.
pub fn index_of(schema: &Schema, name: &str) -> Result<usize> {
    if let Ok(i) = schema.index_of(name) {
        return Ok(i);
    }
    schema.fields().iter().position(|f| f.name().eq_ignore_ascii_case(name)).ok_or_else(|| {
        Error::msg(format!(
            "column {name} not found, columns: {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        ))
    })
}

pub fn project(batch: &RecordBatch, names: &[String]) -> Result<RecordBatch> {
    let idx = names.iter().map(|n| index_of(&batch.schema(), n)).collect::<Result<Vec<_>>>()?;
    let fields: Vec<Field> =
        idx.iter().zip(names).map(|(i, n)| batch.schema().field(*i).clone().with_name(n.clone())).collect();
    let cols: Vec<ArrayRef> = idx.iter().map(|i| batch.column(*i).clone()).collect();
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

pub fn nullable_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema.fields().iter().map(|f| f.as_ref().clone().with_nullable(true)).collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

/// `batch` with `schema`'s types (same column count), casting where they differ.
pub fn cast_to(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let cols = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(c, f)| {
            if c.data_type() == f.data_type() { Ok(c.clone()) } else { crate::normalize::strict_cast(c, f.data_type()) }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

/// Concatenates batches of the same columns, cast to the types of the first.
pub fn union_all(parts: &[RecordBatch]) -> Result<RecordBatch> {
    let schema = nullable_schema(&parts[0].schema());
    let cast = parts.iter().map(|p| cast_to(p, &schema)).collect::<Result<Vec<_>>>()?;
    Ok(concat_batches(&schema, cast.iter())?)
}

pub fn filter(batch: &RecordBatch, mask: &BooleanArray) -> Result<RecordBatch> {
    Ok(arrow_select::filter::filter_record_batch(batch, mask)?)
}

pub fn and(a: &BooleanArray, b: &BooleanArray) -> Result<BooleanArray> {
    Ok(arrow_arith::boolean::and(a, b)?)
}

pub fn single(batch: RecordBatch) -> Batches {
    let schema = batch.schema();
    Box::new(RecordBatchIterator::new([Ok(batch)], schema))
}

pub fn empty(schema: SchemaRef) -> Batches {
    Box::new(RecordBatchIterator::new(std::iter::empty(), schema))
}

/// Rows where a boolean column is true (null counts as false).
pub fn true_mask(col: &ArrayRef) -> Result<BooleanArray> {
    let b = arrow_cast::cast(col, &DataType::Boolean)?;
    let b = b.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| Error::msg("not a boolean column"))?;
    Ok(b.iter().map(|v| Some(v == Some(true))).collect())
}
