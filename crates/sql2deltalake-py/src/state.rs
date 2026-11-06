//! The key-state steps of the Spark data source (`sql2deltalake-core/src/state.rs`) on Arrow
//! objects from Python (pyarrow tables, Spark's `toArrow()`); the GIL is released while they run.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sql2deltalake_core::state;
use sql2deltalake_core::table::{self, collect};

use crate::arrow_ffi::{ArrowStream, import};
use crate::load::to_py;

fn batch(source: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    collect(import(source)?).map_err(to_py)
}

fn stream(batch: RecordBatch) -> ArrowStream {
    ArrowStream::new(table::single(batch))
}

/// A one-row column as a one-column table `v`.
fn value(col: ArrayRef) -> PyResult<ArrowStream> {
    let schema = Arc::new(Schema::new(vec![Field::new("v", col.data_type().clone(), true)]));
    Ok(stream(RecordBatch::try_new(schema, vec![col]).map_err(|e| to_py(e.into()))?))
}

/// `snapshot` against `previous` (both: key columns and the delta column); `watermark`: a
/// one-row table. Returns `deletes` (keys), `strange` (keys and delta value), `changed` (count),
/// `strange_min` and `snapshot_max` (one-row tables `v`).
#[pyfunction]
pub fn pk_diff<'py>(
    py: Python<'py>,
    snapshot: &Bound<'py, PyAny>,
    previous: &Bound<'py, PyAny>,
    keys: Vec<String>,
    delta: String,
    watermark: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let (snapshot, previous, watermark) = (batch(snapshot)?, batch(previous)?, batch(watermark)?);
    let wm = watermark.column(0).clone();
    let d = py.detach(|| state::pk_diff(&snapshot, &previous, &keys, &delta, &wm)).map_err(to_py)?;
    let out = PyDict::new(py);
    out.set_item("deletes", stream(d.deletes))?;
    out.set_item("strange", stream(d.strange))?;
    out.set_item("changed", d.changed)?;
    out.set_item("strange_min", value(d.strange_min)?)?;
    out.set_item("snapshot_max", value(d.snapshot_max)?)?;
    Ok(out)
}

/// Rows of `rows` whose key `previous` does not have.
#[pyfunction]
pub fn count_new_keys(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    previous: &Bound<'_, PyAny>,
    keys: Vec<String>,
) -> PyResult<usize> {
    let (rows, previous) = (batch(rows)?, batch(previous)?);
    py.detach(|| state::count_new_keys(&rows, &previous, &keys)).map_err(to_py)
}

/// The next `latest_pk_version` (see `merge_latest` in the core) and its `max` delta value.
#[pyfunction]
#[pyo3(signature = (previous, loaded, keys, delta, order="__timestamp", deleted="__is_deleted"))]
pub fn merge_latest<'py>(
    py: Python<'py>,
    previous: Option<&Bound<'py, PyAny>>,
    loaded: &Bound<'py, PyAny>,
    keys: Vec<String>,
    delta: String,
    order: &str,
    deleted: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let previous = previous.map(batch).transpose()?;
    let loaded = batch(loaded)?;
    let (latest, max) = py
        .detach(|| {
            let latest = state::merge_latest(previous.as_ref(), &loaded, &keys, &delta, order, deleted)?;
            let max = state::max_of(&latest, &delta)?;
            Ok::<_, sql2deltalake_core::Error>((latest, max))
        })
        .map_err(to_py)?;
    let out = PyDict::new(py);
    out.set_item("count", latest.num_rows())?;
    out.set_item("latest", stream(latest))?;
    out.set_item("max", value(max)?)?;
    Ok(out)
}
