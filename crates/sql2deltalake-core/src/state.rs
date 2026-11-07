//! The key-state steps of a load whose Delta writes happen elsewhere (the Spark data source):
//! the PK snapshot against `latest_pk_version` before the load, and the next `latest_pk_version`
//! from the rows the load appended. Whole tables in memory, hash passes over Arrow row keys.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, Scalar};
use arrow_ord::cmp;
use arrow_schema::{DataType, Field, Schema};

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

/// The next `latest_pk_version` and what changed in it.
pub struct Merged {
    /// Per key the latest appended row (by `order`, then the delta value), plus the rows of
    /// `previous` whose key the load neither appended nor deleted.
    pub latest: RecordBatch,
    /// The keys the load touched: their row of `latest` (`deleted` false), or for a deleted key
    /// that `latest` no longer has, the key with `deleted` true. Columns: keys, delta, `deleted`.
    pub changes: RecordBatch,
}

/// The next `latest_pk_version` (see [`Merged`]). `loaded` holds the keys, the delta column, the
/// `order` column and the boolean `deleted` column of the rows this load appended; `previous`
/// is `None` after a full load.
pub fn merge_latest(
    previous: Option<&RecordBatch>,
    loaded: &RecordBatch,
    keys: &[String],
    delta: &str,
    order: &str,
    deleted: &str,
) -> Result<Merged> {
    let is_deleted = table::true_mask(loaded.column(index_of(&loaded.schema(), deleted)?))?;
    let not_deleted: BooleanArray = is_deleted.iter().map(|v| Some(v != Some(true))).collect();
    let mut names = keys.to_vec();
    names.push(delta.to_string());
    let enc = KeyEncoder::new(loaded, keys)?;
    // Updates and delete markers compete by order: a delete marker after the key's last update
    // removes the key, an update after it brings the key back. At the same order the update wins
    // (as in `replay`, where the delete marker has no delta value and sorts first).
    let order_cols = [
        loaded.column(index_of(&loaded.schema(), order)?).clone(),
        Arc::new(not_deleted.clone()) as ArrayRef,
        loaded.column(index_of(&loaded.schema(), delta)?).clone(),
    ];
    let latest_idx = keys::latest_per_key(&enc, loaded, &order_cols)?;
    let take = arrow_array::UInt64Array::from(latest_idx.iter().map(|i| *i as u64).collect::<Vec<_>>());
    let winners = arrow_select::take::take_record_batch(loaded, &take)?;
    let won_by_delete = table::true_mask(winners.column(index_of(&winners.schema(), deleted)?))?;
    let won_by_update: BooleanArray = won_by_delete.iter().map(|v| Some(v != Some(true))).collect();
    let winners = project(&winners, &names)?;
    let latest = table::filter(&winners, &won_by_update)?;
    let gone = table::filter(&winners, &won_by_delete)?;
    let changes = table::union_all(&[with_flag(&latest, deleted, false)?, with_flag(&gone, deleted, true)?])?;
    let Some(previous) = previous else {
        return Ok(Merged { latest, changes });
    };
    let previous = project(previous, &names)?;
    let kept = table::filter(&previous, &keys::not_in(&enc, &previous, loaded)?)?;
    let schema = table::nullable_schema(&previous.schema());
    let parts = [table::cast_to(&kept, &schema)?, table::cast_to(&latest, &schema)?];
    let latest = arrow_select::concat::concat_batches(&schema, parts.iter())?;
    Ok(Merged { latest, changes })
}

fn with_flag(batch: &RecordBatch, name: &str, value: bool) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(name, DataType::Boolean, true));
    let mut cols = batch.columns().to_vec();
    cols.push(Arc::new(BooleanArray::from(vec![value; batch.num_rows()])));
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

/// `latest_pk_version` from its append-only form: the rows with `seq` 0 are the state as of
/// the last rewrite, every later `seq` holds the [`Merged::changes`] of one load. Per key the
/// row of the highest `seq` wins; a key whose last change is `deleted` is gone. Rows of `seq` 0
/// whose key no later load touched are kept as they are (repeated keys included).
pub fn replay(log: &RecordBatch, keys: &[String], delta: &str, seq: &str, deleted: &str) -> Result<RecordBatch> {
    let mut names = keys.to_vec();
    names.push(delta.to_string());
    let seq_col = log.column(index_of(&log.schema(), seq)?).clone();
    let zero = arrow_cast::cast(&(Arc::new(arrow_array::Int64Array::from(vec![0])) as ArrayRef), seq_col.data_type())?;
    let is_base = cmp::eq(&seq_col, &Scalar::new(zero))?;
    let is_base: BooleanArray = is_base.iter().map(|v| Some(v == Some(true))).collect();
    let base = project(&table::filter(log, &is_base)?, &names)?;
    let later: BooleanArray = is_base.iter().map(|v| Some(v != Some(true))).collect();
    let changes = table::filter(log, &later)?;
    if changes.num_rows() == 0 {
        return Ok(base);
    }
    let enc = KeyEncoder::new(&changes, keys)?;
    let last = keys::latest_per_key(&enc, &changes, &[changes.column(index_of(&changes.schema(), seq)?).clone()])?;
    let take = arrow_array::UInt64Array::from(last.iter().map(|i| *i as u64).collect::<Vec<_>>());
    let last = arrow_select::take::take_record_batch(&changes, &take)?;
    let gone = table::true_mask(last.column(index_of(&last.schema(), deleted)?))?;
    let present: BooleanArray = gone.iter().map(|v| Some(v != Some(true))).collect();
    let current = project(&table::filter(&last, &present)?, &names)?;
    let kept = table::filter(&base, &keys::not_in(&enc, &base, &last)?)?;
    table::union_all(&[kept, current])
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
        let merged =
            merge_latest(Some(&previous), &loaded, &["id".into()], "ts", "__timestamp", "__is_deleted").unwrap();
        let out = merged.latest;
        assert_eq!(ids(&out), vec![1, 2, 4]);
        // 2 and 4 written, 3 deleted
        let changes = merged.changes;
        assert_eq!(ids(&changes), vec![2, 3, 4]);
        let flags: Vec<bool> = changes.column(2).as_boolean().iter().map(|v| v.unwrap()).collect();
        assert_eq!(flags.iter().filter(|f| **f).count(), 1);
        let two = (0..out.num_rows())
            .find(|i| out.column(0).as_primitive::<arrow_array::types::Int32Type>().value(*i) == 2)
            .unwrap();
        assert_eq!(out.column(1).as_primitive::<Int64Type>().value(two), 22);
        assert_eq!(max_of(&out, "ts").unwrap().as_primitive::<Int64Type>().value(0), 40);
        let full = merge_latest(None, &loaded, &["id".into()], "ts", "__timestamp", "__is_deleted").unwrap();
        assert_eq!(ids(&full.latest), vec![2, 4]);
    }

    fn loaded(ids: &[i32], ts: &[Option<i64>], order: &[i64], deleted: &[bool]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("ts", DataType::Int64, true),
                Field::new("__timestamp", DataType::Int64, true),
                Field::new("__is_deleted", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(Int64Array::from(order.to_vec())),
                Arc::new(BooleanArray::from(deleted.to_vec())),
            ],
        )
        .unwrap()
    }

    fn flags_by_id(changes: &RecordBatch) -> Vec<(i32, bool)> {
        let ids = changes.column(0).as_primitive::<arrow_array::types::Int32Type>();
        let flags = changes.column(2).as_boolean();
        let mut v: Vec<(i32, bool)> = (0..changes.num_rows()).map(|i| (ids.value(i), flags.value(i))).collect();
        v.sort();
        v
    }

    #[test]
    fn merge_a_later_delete_wins_over_an_update_of_the_same_load() {
        // 7: updated at 100, deleted at 105; 8: deleted at 100, inserted again at 105
        let previous = batch(&[7, 8, 9], &[Some(10), Some(11), Some(12)]);
        let loaded = loaded(
            &[7, 7, 8, 8],
            &[Some(50), None, None, Some(60)],
            &[100, 105, 100, 105],
            &[false, true, true, false],
        );
        let keys = ["id".to_string()];
        let merged = merge_latest(Some(&previous), &loaded, &keys, "ts", "__timestamp", "__is_deleted").unwrap();
        assert_eq!(ids(&merged.latest), vec![8, 9]);
        assert_eq!(flags_by_id(&merged.changes), vec![(7, true), (8, false)]);
        let full = merge_latest(None, &loaded, &keys, "ts", "__timestamp", "__is_deleted").unwrap();
        assert_eq!(ids(&full.latest), vec![8]);
        assert_eq!(flags_by_id(&full.changes), vec![(7, true), (8, false)]);
    }

    #[test]
    fn merge_an_update_and_a_delete_at_the_same_order_keep_the_update() {
        let previous = batch(&[7], &[Some(10)]);
        let loaded = loaded(&[7, 7], &[Some(50), None], &[100, 100], &[false, true]);
        let keys = ["id".to_string()];
        let merged = merge_latest(Some(&previous), &loaded, &keys, "ts", "__timestamp", "__is_deleted").unwrap();
        assert_eq!(ids(&merged.latest), vec![7]);
        assert_eq!(flags_by_id(&merged.changes), vec![(7, false)]);
    }

    fn log(ids: &[i32], ts: &[Option<i64>], seq: &[i64], deleted: &[bool]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("ts", DataType::Int64, true),
                Field::new("__state_seq", DataType::Int64, true),
                Field::new("__is_deleted", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(Int64Array::from(seq.to_vec())),
                Arc::new(BooleanArray::from(deleted.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn replay_applies_the_changes_in_order() {
        // base 1, 2, 3 (2 twice); load 1: 2 updated, 3 deleted, 4 new; load 2: 4 deleted, 3 back
        let log = log(
            &[1, 2, 2, 3, 2, 3, 4, 4, 3],
            &[Some(10), Some(20), Some(20), Some(30), Some(21), None, Some(40), None, Some(31)],
            &[0, 0, 0, 0, 5, 5, 5, 7, 7],
            &[false, false, false, false, false, true, false, true, false],
        );
        let out = replay(&log, &["id".into()], "ts", "__state_seq", "__is_deleted").unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(ids(&out), vec![1, 2, 3]);
        let ts = |id: i32| {
            (0..out.num_rows())
                .find(|i| out.column(0).as_primitive::<arrow_array::types::Int32Type>().value(*i) == id)
                .map(|i| out.column(1).as_primitive::<Int64Type>().value(i))
                .unwrap()
        };
        assert_eq!((ts(2), ts(3)), (21, 31));
        // untouched base rows stay as they are, repeated keys included
        let base = replay(&log.slice(0, 4), &["id".into()], "ts", "__state_seq", "__is_deleted").unwrap();
        assert_eq!(base.num_rows(), 4);
    }

    #[test]
    fn new_keys_are_counted() {
        let previous = batch(&[1, 2], &[None, None]);
        let rows = batch(&[2, 3, 4], &[None, None, None]);
        assert_eq!(count_new_keys(&rows, &previous, &["id".into()]).unwrap(), 2);
    }
}
