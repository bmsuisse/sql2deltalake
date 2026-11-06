//! Runs a load in Rust against a Python host object (`odbc2deltalake._native.Host`), with the
//! GIL released except inside the host callbacks.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sql2deltalake_core::host::{Batches, Drift, Host, Level, LogArgs, Mode};
use sql2deltalake_core::load::{self, LoadMode, LoadResult, LoadSpec, State};
use sql2deltalake_core::scalar::Scalar;
use sql2deltalake_core::sqlgen::SourceSql;
use sql2deltalake_core::{Error, Result};

use crate::arrow_ffi::{ArrowStream, import};
use crate::sql;

/// A Python exception raised in a host callback, carried through the core unchanged.
#[derive(Debug)]
struct PyFailure(PyErr);

impl std::fmt::Display for PyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for PyFailure {}

fn external(e: PyErr) -> Error {
    Error::External(Box::new(PyFailure(e)))
}

/// The original Python exception if the error came from a callback.
pub fn to_py(e: Error) -> PyErr {
    match e {
        Error::External(b) => match b.downcast::<PyFailure>() {
            Ok(f) => f.0,
            Err(other) => PyRuntimeError::new_err(other.to_string()),
        },
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

pub struct PyHost {
    obj: Py<PyAny>,
    /// Local path of the target folder: its Delta tables are read with delta-kernel-rs.
    kernel_root: Option<String>,
}

impl PyHost {
    fn new(py: Python<'_>, obj: Py<PyAny>) -> PyResult<PyHost> {
        let kernel_root = if cfg!(feature = "kernel") && std::env::var("SQL2DELTA_KERNEL").map_or(true, |v| v != "0") {
            obj.bind(py).call_method0("kernel_root")?.extract()?
        } else {
            None
        };
        Ok(PyHost { obj, kernel_root })
    }

    #[cfg_attr(not(feature = "kernel"), allow(dead_code))]
    fn local(&self, rel: &str) -> Option<String> {
        self.kernel_root.as_ref().map(|root| format!("{}/{rel}", root.trim_end_matches('/')))
    }
}

fn mode(m: Mode) -> &'static str {
    match m {
        Mode::Overwrite => "overwrite",
        Mode::Append => "append",
    }
}

fn drift_obj(py: Python<'_>, d: Drift) -> Py<PyAny> {
    match d {
        Drift::No => false.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Drift::Yes => true.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Drift::NewOnly => "new_only".into_pyobject(py).unwrap().into_any().unbind(),
    }
}

impl PyHost {
    fn call<T>(&self, f: impl FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<T>) -> Result<T> {
        Python::attach(|py| f(py, self.obj.bind(py))).map_err(external)
    }

    fn stream(&self, method: &str, sql: &str) -> Result<Option<Batches>> {
        self.call(|_, h| {
            let s = h.call_method1(method, (sql,))?;
            if s.is_none() { Ok(None) } else { import(&s).map(Some) }
        })
    }
}

impl Host for PyHost {
    fn source_query(&self, sql: &str) -> Result<Batches> {
        self.stream("source_query", sql)?.ok_or_else(|| Error::msg("source_query returned None"))
    }

    fn source_stream(&self, sql: &str, partition_on: Option<&str>) -> Result<Option<Batches>> {
        self.call(|_, h| {
            let s = h.call_method1("source_stream", (sql, partition_on))?;
            if s.is_none() { Ok(None) } else { import(&s).map(Some) }
        })
    }

    fn source_copy(&self, sql: &str, rel: &str, m: Mode, d: Drift) -> Result<()> {
        self.call(|py, h| h.call_method1("source_copy", (sql, rel, mode(m), drift_obj(py, d))).map(|_| ()))
    }

    fn table_exists(&self, rel: &str) -> Result<bool> {
        self.call(|_, h| h.call_method1("table_exists", (rel,))?.extract())
    }

    fn path_exists(&self, rel: &str) -> Result<bool> {
        self.call(|_, h| h.call_method1("path_exists", (rel,))?.extract())
    }

    fn version(&self, rel: &str) -> Result<i64> {
        #[cfg(feature = "kernel")]
        if let Some(path) = self.local(rel) {
            return crate::kernel::version(&path);
        }
        self.call(|_, h| h.call_method1("version", (rel,))?.extract())
    }

    fn read(&self, rel: &str, version: Option<i64>, columns: Option<&[String]>) -> Result<Batches> {
        #[cfg(feature = "kernel")]
        if let Some(path) = self.local(rel) {
            return crate::kernel::read(&path, version, columns);
        }
        self.call(|_, h| import(&h.call_method1("read", (rel, version, columns.map(|c| c.to_vec())))?))
    }

    fn column_names(&self, rel: &str) -> Result<Vec<String>> {
        #[cfg(feature = "kernel")]
        if let Some(path) = self.local(rel) {
            return crate::kernel::column_names(&path);
        }
        self.call(|_, h| h.call_method1("column_names", (rel,))?.extract())
    }

    fn write(&self, rel: &str, data: Batches, m: Mode, d: Drift) -> Result<()> {
        self.call(|py, h| {
            let stream = Py::new(py, ArrowStream::new(data))?;
            h.call_method1("write", (rel, stream, mode(m), drift_obj(py, d))).map(|_| ())
        })
    }

    fn restore(&self, rel: &str, version: i64) -> Result<()> {
        self.call(|_, h| h.call_method1("restore", (rel, version)).map(|_| ()))
    }

    fn remove(&self, rel: &str, recursive: bool) -> Result<()> {
        self.call(|_, h| h.call_method1("remove", (rel, recursive)).map(|_| ()))
    }

    fn vacuum(&self, rel: &str) -> Result<()> {
        self.call(|_, h| h.call_method1("vacuum", (rel,)).map(|_| ()))
    }

    fn mkdir(&self, rel: &str) -> Result<()> {
        self.call(|_, h| h.call_method1("mkdir", (rel,)).map(|_| ()))
    }

    fn upload_str(&self, rel: &str, content: &str) -> Result<()> {
        self.call(|_, h| h.call_method1("upload_str", (rel, content)).map(|_| ()))
    }

    fn file_age_seconds(&self, rel: &str) -> Result<Option<f64>> {
        self.call(|_, h| h.call_method1("file_age_seconds", (rel,))?.extract())
    }

    fn log(&self, level: Level, msg: &str, args: LogArgs<'_>) -> Result<()> {
        let level = match level {
            Level::Info => "info",
            Level::Warning => "warning",
            Level::Error => "error",
        };
        self.call(|py, h| {
            let kw = PyDict::new(py);
            if let Some(s) = args.sql {
                kw.set_item("sql", s)?;
            }
            if let Some(s) = args.load {
                kw.set_item("load", s)?;
            }
            if let Some(s) = args.sub_load {
                kw.set_item("sub_load", s)?;
            }
            h.call_method1("log", (level, msg, kw)).map(|_| ())
        })
    }

    fn fail_point(&self, name: &str) -> Result<Option<bool>> {
        self.call(|_, h| h.call_method1("fail_point", (name,))?.extract())
    }
}

fn spec(d: &Bound<'_, PyAny>) -> PyResult<LoadSpec> {
    let get = |k: &str| d.get_item(k);
    let cols: Vec<Bound<'_, PyAny>> = get("columns")?.extract()?;
    let drift = get("allow_schema_drift")?;
    let drift = if let Ok(s) = drift.extract::<String>() {
        if s == "new_only" {
            Drift::NewOnly
        } else {
            return Err(PyValueError::new_err(format!("allow_schema_drift {s}")));
        }
    } else if drift.is_truthy()? {
        Drift::Yes
    } else {
        Drift::No
    };
    let load_mode: String = get("load_mode")?.extract()?;
    Ok(LoadSpec {
        sql: SourceSql {
            dialect: sql::dialect(&get("dialect")?.extract::<String>()?)?,
            source: sql::source(&get("source")?)?,
            type_map: sql::type_map(get("type_map")?.extract()?),
            no_trim: get("no_trim")?.extract()?,
        },
        cols: cols.iter().map(sql::column).collect::<PyResult<_>>()?,
        pks: get("pks")?.extract()?,
        delta_col: get("delta_col")?.extract()?,
        load_mode: LoadMode::parse(&load_mode)
            .ok_or_else(|| PyValueError::new_err(format!("load_mode {load_mode}")))?,
        drift,
        no_complex_entries_load: get("no_complex_entries_load")?.extract()?,
        schema_fingerprint: get("schema_fingerprint")?.extract()?,
    })
}

fn scalar_py(py: Python<'_>, s: &Scalar) -> PyResult<Py<PyAny>> {
    Ok(match s {
        Scalar::Null => py.None(),
        Scalar::Bool(b) => b.into_pyobject(py)?.to_owned().into_any().unbind(),
        Scalar::Int(i) => i.into_pyobject(py)?.into_any().unbind(),
        Scalar::Float(f) => f.into_pyobject(py)?.into_any().unbind(),
        Scalar::Str(s) => s.into_pyobject(py)?.into_any().unbind(),
        Scalar::Bytes(b) => pyo3::types::PyBytes::new(py, b).into_any().unbind(),
        other => other.to_text().into_pyobject(py)?.into_any().unbind(),
    })
}

fn state_py(py: Python<'_>, s: &State) -> PyResult<Py<PyAny>> {
    Ok((scalar_py(py, &s.0)?, s.1).into_pyobject(py)?.into_any().unbind())
}

/// Runs one load; returns `{"type": "full"|"delta"|"none"|"append_only", ...}`.
#[pyfunction]
pub fn run_load(py: Python<'_>, host: Py<PyAny>, load_spec: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let spec = spec(load_spec)?;
    let host = PyHost::new(py, host)?;
    let result = py.detach(|| load::execute(&host, &spec)).map_err(to_py)?;
    let out = PyDict::new(py);
    match result {
        LoadResult::Full => out.set_item("type", "full")?,
        LoadResult::NoLoad => out.set_item("type", "none")?,
        LoadResult::AppendOnly => out.set_item("type", "append_only")?,
        LoadResult::Delta { starting_local_state, starting_source_state, end_source_state, dirty } => {
            out.set_item("type", "delta")?;
            out.set_item("starting_local_state", state_py(py, &starting_local_state)?)?;
            out.set_item("starting_source_state", state_py(py, &starting_source_state)?)?;
            if let Some(e) = end_source_state {
                out.set_item("end_source_state", state_py(py, &e)?)?;
            }
            out.set_item("dirty", dirty)?;
        }
    }
    Ok(out.into_any().unbind())
}

/// Rebuilds `latest_pk_version` from the Delta table; False if there was nothing to restore.
#[pyfunction]
pub fn restore_last_pk(py: Python<'_>, host: Py<PyAny>, load_spec: &Bound<'_, PyAny>) -> PyResult<bool> {
    let spec = spec(load_spec)?;
    let host = PyHost::new(py, host)?;
    py.detach(|| load::restore_latest_pk(&host, &spec)).map_err(to_py)
}
