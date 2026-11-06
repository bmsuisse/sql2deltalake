//! `odbc2deltalake._core`: the Python face of sql2deltalake-core.

mod arrow_ffi;
mod scalar;
mod sql;

use std::collections::HashMap;

use pyo3::prelude::*;
use sql2deltalake_core::normalize::NormalizedReader;

/// The batches of `source` cast to types Delta can store (see `normalize.rs`); `time_scales`
/// maps the position of a time column to its scale (default 7).
#[pyfunction]
#[pyo3(signature = (source, time_scales=None))]
fn normalize(source: &Bound<'_, PyAny>, time_scales: Option<HashMap<usize, u8>>) -> PyResult<arrow_ffi::ArrowStream> {
    let reader = arrow_ffi::import(source)?;
    Ok(arrow_ffi::ArrowStream::new(NormalizedReader::wrap(reader, time_scales.unwrap_or_default())))
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", sql2deltalake_core::VERSION)?;
    m.add_class::<arrow_ffi::ArrowStream>()?;
    m.add_class::<sql::SqlBuilder>()?;
    m.add_function(wrap_pyfunction!(sql::load_timestamp_literal, m)?)?;
    m.add_function(wrap_pyfunction!(sql::compat_name, m)?)?;
    m.add_function(wrap_pyfunction!(normalize, m)?)?;
    Ok(())
}
