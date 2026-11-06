//! The key-state steps of a load whose Delta writes happen elsewhere (the Spark data source):
//! the PK snapshot against `latest_pk_version` before the load, and the next `latest_pk_version`
//! from the rows the load appended. Whole tables in memory, hash passes over Arrow row keys.

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, Scalar};
use arrow_ord::cmp;

use crate::keys::{self, KeyEncoder, KeySet};
use crate::table::{self, index_of, project};
use crate::{Error, Result};

/// The PK snapshot of the source against the keys of the last load.
pub struct PkDiff {
    /// Keys of `previous` that the snapshot no longer has (rows deleted in the source).
    pub deletes: RecordBatch,
    /// Keys and delta value of snapshot rows that are new or changed although their delta value is
    /// not above the watermark (or is NULL): a delta read by the watermark would miss them.
    pub strange: RecordBatch,
    /// Row count of the snapshot rows that are new or changed.
    pub changed: usize,
    /// The smallest delta value of `strange` and the largest of the snapshot, as one-row columns
    /// (NULL if there is none).
    pub strange_min: ArrayRef,
    pub snapshot_max: ArrayRef,
}

fn one_row(col: &ArrayRef, idx: Option<usize>) -> ArrayRef {
    match idx {
        Some(i) => col.slice(i, 1),
        None => arrow_array::new_null_array(col.data_type(), 1),
    }
}

/// `previous` and `snapshot` hold the key columns `keys` and the delta column `delta` (types may
/// differ, the snapshot's are used); `watermark` is a one-element array, cast to the delta type.
pub fn pk_diff(
    snapshot: &RecordBatch,
    previous: &RecordBatch,
    keys: &[String],
    delta: &str,
    watermark: &ArrayRef,
) -> Result<PkDiff> {
    let diff = keys::diff_snapshot(snapshot, previous, keys, delta)?;
    let gone: BooleanArray = diff.kept.iter().map(|k| Some(!k)).collect();
    let deletes = table::filter(&project(previous, keys)?, &gone)?;
    let mut names = keys.to_vec();
    names.push(delta.to_string());
    let changed = table::filter(&project(snapshot, &names)?, &diff.changed)?;
    let value = changed.column(keys.len()).clone();
    if watermark.len() != 1 {
        return Err(Error::msg("the watermark must be one value"));
    }
    let bound = arrow_cast::cast(watermark, value.data_type())?;
    let below = cmp::lt_eq(&value, &Scalar::new(bound))?;
    let strange_mask: BooleanArray =
        (0..value.len()).map(|i| Some(value.is_null(i) || (below.is_valid(i) && below.value(i)))).collect();
    let strange = table::filter(&changed, &strange_mask)?;
    let strange_col = strange.column(keys.len()).clone();
    let snapshot_col = snapshot.column(index_of(&snapshot.schema(), delta)?).clone();
    Ok(PkDiff {
        deletes,
        changed: changed.num_rows(),
        strange_min: one_row(&strange_col, keys::argmin(&strange_col)?),
        snapshot_max: one_row(&snapshot_col, keys::argmax(&snapshot_col)?),
        strange,
    })
}

/// Rows of `rows` whose key `previous` does not have.
pub fn count_new_keys(rows: &RecordBatch, previous: &RecordBatch, keys: &[String]) -> Result<usize> {
    let enc = KeyEncoder::new(rows, keys)?;
    Ok(KeySet::build(&enc, previous)?.contains(&enc, rows)?.iter().filter(|c| !**c).count())
}

/// The next `latest_pk_version`: per key the latest appended row (by `order`, then the delta
/// value), plus the rows of `previous` whose key the load neither appended nor deleted.
/// `loaded` holds the keys, the delta column, the `order` column and the boolean `deleted`
/// column of the rows this load appended; `previous` is `None` after a full load.
pub fn merge_latest(
    previous: Option<&RecordBatch>,
    loaded: &RecordBatch,
    keys: &[String],
    delta: &str,
    order: &str,
    deleted: &str,
) -> Result<RecordBatch> {
    let is_deleted = table::true_mask(loaded.column(index_of(&loaded.schema(), deleted)?))?;
    let not_deleted: BooleanArray = is_deleted.iter().map(|v| Some(v != Some(true))).collect();
    let updates = table::filter(loaded, &not_deleted)?;
    let deletes = table::filter(loaded, &is_deleted)?;
    let mut names = keys.to_vec();
    names.push(delta.to_string());
    let enc = KeyEncoder::new(loaded, keys)?;
    let order_cols = [
        updates.column(index_of(&updates.schema(), order)?).clone(),
        updates.column(index_of(&updates.schema(), delta)?).clone(),
    ];
    let latest_idx = keys::latest_per_key(&enc, &updates, &order_cols)?;
    let take = arrow_array::UInt32Array::from(latest_idx.iter().map(|i| *i as u32).collect::<Vec<_>>());
    let latest = arrow_select::take::take_record_batch(&project(&updates, &names)?, &take)?;
    let Some(previous) = previous else {
        return Ok(latest);
    };
    let previous = project(previous, &names)?;
    let mut keep = keys::not_in(&enc, &previous, &updates)?;
    if deletes.num_rows() > 0 {
        keep = table::and(&keep, &keys::not_in(&enc, &previous, &deletes)?)?;
    }
    let kept = table::filter(&previous, &keep)?;
    let schema = table::nullable_schema(&previous.schema());
    let parts = [table::cast_to(&kept, &schema)?, table::cast_to(&latest, &schema)?];
    Ok(arrow_select::concat::concat_batches(&schema, parts.iter())?)
}

/// The largest value of column `name` as a one-row column (NULL if none).
pub fn max_of(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let col = batch.column(index_of(&batch.schema(), name)?).clone();
    Ok(one_row(&col, keys::argmax(&col)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{BooleanArray, Int32Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(ids: &[i32], ts: &[Option<i64>]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("ts", DataType::Int64, true),
            ])),
            vec![Arc::new(Int32Array::from(ids.to_vec())), Arc::new(Int64Array::from(ts.to_vec()))],
        )
        .unwrap()
    }

    fn ids(b: &RecordBatch) -> Vec<i32> {
        let mut v: Vec<i32> = b.column(0).as_primitive::<arrow_array::types::Int32Type>().values().to_vec();
        v.sort();
        v
    }

    #[test]
    fn diff_splits_deletes_and_strange_updates() {
        // 2: changed above the watermark (a normal update), 3: changed below it (strange),
        // 5: new with NULL (strange), 4: deleted
        let previous = batch(&[1, 2, 3, 4], &[Some(10), Some(20), Some(30), Some(40)]);
        let snapshot = batch(&[1, 2, 3, 5], &[Some(10), Some(60), Some(25), None]);
        let wm: ArrayRef = Arc::new(Int64Array::from(vec![50]));
        let d = pk_diff(&snapshot, &previous, &["id".into()], "ts", &wm).unwrap();
        assert_eq!(ids(&d.deletes), vec![4]);
        assert_eq!(ids(&d.strange), vec![3, 5]);
        assert_eq!(d.changed, 3);
        assert_eq!(d.strange_min.as_primitive::<Int64Type>().value(0), 25);
        assert_eq!(d.snapshot_max.as_primitive::<Int64Type>().value(0), 60);
    }

    #[test]
    fn merge_keeps_unloaded_and_drops_deleted_keys() {
        let previous = batch(&[1, 2, 3], &[Some(10), Some(20), Some(30)]);
        let loaded = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("ts", DataType::Int64, true),
                Field::new("__timestamp", DataType::Int64, true),
                Field::new("__is_deleted", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![2, 2, 4, 3])),
                Arc::new(Int64Array::from(vec![Some(21), Some(22), Some(40), None])),
                Arc::new(Int64Array::from(vec![100, 101, 100, 99])),
                Arc::new(BooleanArray::from(vec![false, false, false, true])),
            ],
        )
        .unwrap();
        let out = merge_latest(Some(&previous), &loaded, &["id".into()], "ts", "__timestamp", "__is_deleted").unwrap();
        assert_eq!(ids(&out), vec![1, 2, 4]);
        let two = (0..out.num_rows())
            .find(|i| out.column(0).as_primitive::<arrow_array::types::Int32Type>().value(*i) == 2)
            .unwrap();
        assert_eq!(out.column(1).as_primitive::<Int64Type>().value(two), 22);
        assert_eq!(max_of(&out, "ts").unwrap().as_primitive::<Int64Type>().value(0), 40);
        let full = merge_latest(None, &loaded, &["id".into()], "ts", "__timestamp", "__is_deleted").unwrap();
        assert_eq!(ids(&full), vec![2, 4]);
    }

    #[test]
    fn new_keys_are_counted() {
        let previous = batch(&[1, 2], &[None, None]);
        let rows = batch(&[2, 3, 4], &[None, None, None]);
        assert_eq!(count_new_keys(&rows, &previous, &["id".into()]).unwrap(), 2);
    }
}
