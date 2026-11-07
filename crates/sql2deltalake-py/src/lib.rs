//! `odbc2deltalake._core`: the Python face of sql2deltalake-core.

mod arrow_ffi;
#[cfg(feature = "kernel")]
mod kernel;
mod load;
mod scalar;
#[cfg(feature = "arrowsql-core")]
mod source;
mod sql;
mod state;

use std::collections::HashMap;

use pyo3::prelude::*;
use sql2deltalake_core::normalize::{ConformReader, NormalizedReader};

/// The batches of `source` cast to types Delta can store (see `normalize.rs`); `time_scales`
/// maps the position of a time column to its scale (default 7). With `target` (an Arrow schema
/// with as many columns), the batches are then cast to it by position.
#[pyfunction]
#[pyo3(signature = (source, time_scales=None, target=None))]
fn normalize(
    source: &Bound<'_, PyAny>,
    time_scales: Option<HashMap<usize, u8>>,
    target: Option<&Bound<'_, PyAny>>,
) -> PyResult<arrow_ffi::ArrowStream> {
    let reader = NormalizedReader::wrap(arrow_ffi::import(source)?, time_scales.unwrap_or_default());
    let reader: arrow_ffi::Batches = match target {
        None => reader,
        Some(t) => Box::new(
            ConformReader::wrap(reader, arrow_ffi::import_schema(t)?)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
        ),
    };
    Ok(arrow_ffi::ArrowStream::new(reader))
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", sql2deltalake_core::VERSION)?;
    m.add_class::<arrow_ffi::ArrowStream>()?;
    m.add_class::<sql::SqlBuilder>()?;
    m.add_function(wrap_pyfunction!(sql::load_timestamp_literal, m)?)?;
    m.add_function(wrap_pyfunction!(sql::compat_name, m)?)?;
    m.add_function(wrap_pyfunction!(sql::partition_sql, m)?)?;
    m.add_function(wrap_pyfunction!(sql::partition_bounds, m)?)?;
    m.add_function(wrap_pyfunction!(normalize, m)?)?;
    m.add_function(wrap_pyfunction!(load::run_load, m)?)?;
    m.add_function(wrap_pyfunction!(load::restore_last_pk, m)?)?;
    #[cfg(feature = "arrowsql-core")]
    m.add_function(wrap_pyfunction!(source::read_sql, m)?)?;
    m.add_function(wrap_pyfunction!(state::pk_diff, m)?)?;
    m.add_function(wrap_pyfunction!(state::count_new_keys, m)?)?;
    m.add_function(wrap_pyfunction!(state::merge_latest, m)?)?;
    m.add_function(wrap_pyfunction!(state::replay_state, m)?)?;
    Ok(())
}
