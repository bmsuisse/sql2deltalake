//! `odbc2deltalake._core`: the Python face of sql2deltalake-core.

use pyo3::prelude::*;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", sql2deltalake_core::VERSION)?;
    Ok(())
}
