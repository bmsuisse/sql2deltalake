//! Runs a load in Rust against a Python host object (`sql2deltalake._native.Host`), with the
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

/// A callback's exception; `BaseException`s that are not `Exception`s (`KeyboardInterrupt`,
/// `SystemExit`) end the load without any fallback.
fn external(py: Python<'_>, e: PyErr) -> Error {
    if e.is_instance_of::<pyo3::exceptions::PyException>(py) {
        Error::External(Box::new(PyFailure(e)))
    } else {
        Error::Interrupted(Box::new(PyFailure(e)))
    }
}

/// The original Python exception if the error came from a callback; a failed restore of the
/// load state is added to it as a note (`__notes__`).
pub fn to_py(e: Error) -> PyErr {
    match e {
        Error::External(b) | Error::Interrupted(b) => match b.downcast::<PyFailure>() {
            Ok(f) => f.0,
            Err(other) => PyRuntimeError::new_err(other.to_string()),
        },
        Error::NotRestored { load, restore } => {
            let err = to_py(*load);
            Python::attach(|py| {
                let note = format!("The load state could not be restored: {restore}");
                let value = err.value(py);
                let added = if value.hasattr("add_note").unwrap_or(false) {
                    value.call_method1("add_note", (note,)).map(|_| ())
                } else {
                    value.setattr("__notes__", vec![note])
                };
                if let Err(e) = added {
                    e.write_unraisable(py, None);
                }
            });
            err
        }
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

/// What the load asked about one Delta table, until the load changes something below it.
#[derive(Default, Clone)]
struct TableMeta {
    exists: Option<bool>,
    version: Option<i64>,
    columns: Option<Vec<String>>,
}

pub struct PyHost {
    obj: Py<PyAny>,
    /// Per load (one `PyHost` per `run_load`): existence, version and columns by relative path,
    /// dropped for a path when the load writes, restores, removes or vacuums at or below it.
    meta: std::sync::Mutex<std::collections::HashMap<String, TableMeta>>,
    /// Local path of the target folder: its Delta tables are read with delta-kernel-rs.
    kernel_root: Option<String>,
    /// The source, read with arrowsql's Rust core without entering Python.
    #[cfg(feature = "arrowsql-core")]
    source: Option<crate::source::SourceConn>,
}

impl PyHost {
    fn new(py: Python<'_>, obj: Py<PyAny>) -> PyResult<PyHost> {
        let kernel_root = if cfg!(feature = "kernel") && std::env::var("SQL2DELTA_KERNEL").map_or(true, |v| v != "0") {
            obj.bind(py).call_method0("kernel_root")?.extract()?
        } else {
            None
        };
        #[cfg(feature = "arrowsql-core")]
        let source = {
            let conn = obj.bind(py).call_method0("source_connection")?;
            if conn.is_none() {
                None
            } else {
                let (cs, utf8, partitions): (String, bool, usize) = conn.extract()?;
                Some(crate::source::SourceConn::new(cs, utf8, partitions, 65536))
            }
        };
        Ok(PyHost {
            obj,
            meta: Default::default(),
            kernel_root,
            #[cfg(feature = "arrowsql-core")]
            source,
        })
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
    fn cached<T>(
        &self,
        rel: &str,
        get: impl Fn(&TableMeta) -> Option<T>,
        set: impl Fn(&mut TableMeta, T),
        load: impl FnOnce() -> Result<T>,
    ) -> Result<T>
    where
        T: Clone,
    {
        if let Some(v) = self.meta.lock().unwrap().get(rel).and_then(&get) {
            return Ok(v);
        }
        let v = load()?;
        set(self.meta.lock().unwrap().entry(rel.to_string()).or_default(), v.clone());
        Ok(v)
    }

    /// Forgets what is known about `rel`, the tables below it and the folders above it.
    fn changed(&self, rel: &str) {
        let rel = rel.trim_end_matches('/');
        self.meta
            .lock()
            .unwrap()
            .retain(|k, _| !(k == rel || k.starts_with(&format!("{rel}/")) || rel.starts_with(&format!("{k}/"))));
    }

    fn call<T>(&self, f: impl FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<T>) -> Result<T> {
        Python::attach(|py| f(py, self.obj.bind(py)).map_err(|e| external(py, e)))
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
        #[cfg(feature = "arrowsql-core")]
        if let Some(src) = &self.source {
            return src.open(sql, false, None).map_err(|e| Error::External(Box::new(e)));
        }
        self.stream("source_query", sql)?.ok_or_else(|| Error::msg("source_query returned None"))
    }

    fn source_stream(&self, sql: &str, partition_on: Option<&str>) -> Result<Option<Batches>> {
        #[cfg(feature = "arrowsql-core")]
        if let Some(src) = &self.source {
            return src.open(sql, src.utf8_strings, partition_on).map(Some).map_err(|e| Error::External(Box::new(e)));
        }
        self.call(|_, h| {
            let s = h.call_method1("source_stream", (sql, partition_on))?;
            if s.is_none() { Ok(None) } else { import(&s).map(Some) }
        })
    }

    fn source_copy(&self, sql: &str, rel: &str, m: Mode, d: Drift) -> Result<()> {
        self.changed(rel);
        self.call(|py, h| h.call_method1("source_copy", (sql, rel, mode(m), drift_obj(py, d))).map(|_| ()))
    }

    fn table_exists(&self, rel: &str) -> Result<bool> {
        self.cached(
            rel,
            |m| m.exists,
            |m, v| m.exists = Some(v),
            || {
                #[cfg(feature = "kernel")]
                if let Some(path) = self.local(rel) {
                    return crate::kernel::table_exists(&path);
                }
                self.call(|_, h| h.call_method1("table_exists", (rel,))?.extract())
            },
        )
    }

    fn path_exists(&self, rel: &str) -> Result<bool> {
        #[cfg(feature = "kernel")]
        if let Some(path) = self.local(rel) {
            return Ok(std::path::Path::new(&path).exists());
        }
        self.call(|_, h| h.call_method1("path_exists", (rel,))?.extract())
    }

    fn version(&self, rel: &str) -> Result<i64> {
        self.cached(
            rel,
            |m| m.version,
            |m, v| m.version = Some(v),
            || {
                #[cfg(feature = "kernel")]
                if let Some(path) = self.local(rel) {
                    return crate::kernel::version(&path);
                }
                self.call(|_, h| h.call_method1("version", (rel,))?.extract())
            },
        )
    }

    fn read(&self, rel: &str, version: Option<i64>, columns: Option<&[String]>) -> Result<Batches> {
        #[cfg(feature = "kernel")]
        if let Some(path) = self.local(rel) {
            return crate::kernel::read(&path, version, columns);
        }
        self.call(|_, h| import(&h.call_method1("read", (rel, version, columns.map(|c| c.to_vec())))?))
    }

    fn column_names(&self, rel: &str) -> Result<Vec<String>> {
        self.cached(
            rel,
            |m| m.columns.clone(),
            |m, v| m.columns = Some(v),
            || {
                #[cfg(feature = "kernel")]
                if let Some(path) = self.local(rel) {
                    return crate::kernel::column_names(&path);
                }
                self.call(|_, h| h.call_method1("column_names", (rel,))?.extract())
            },
        )
    }

    fn write(&self, rel: &str, data: Batches, m: Mode, d: Drift) -> Result<()> {
        self.changed(rel);
        self.call(|py, h| {
            let stream = Py::new(py, ArrowStream::new(data))?;
            h.call_method1("write", (rel, stream, mode(m), drift_obj(py, d))).map(|_| ())
        })
    }

    fn restore(&self, rel: &str, version: i64) -> Result<()> {
        self.changed(rel);
        self.call(|_, h| h.call_method1("restore", (rel, version)).map(|_| ()))
    }

    fn remove(&self, rel: &str, recursive: bool) -> Result<()> {
        self.changed(rel);
        self.call(|_, h| h.call_method1("remove", (rel, recursive)).map(|_| ()))
    }

    fn vacuum(&self, rel: &str) -> Result<()> {
        self.changed(rel);
        self.call(|_, h| h.call_method1("vacuum", (rel,)).map(|_| ()))
    }

    fn mkdir(&self, rel: &str) -> Result<()> {
        self.changed(rel);
        self.call(|_, h| h.call_method1("mkdir", (rel,)).map(|_| ()))
    }

    fn upload_str(&self, rel: &str, content: &str) -> Result<()> {
        self.changed(rel);
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
        shortcut_allowed: get("shortcut_allowed")?.extract()?,
        watermark_sql: get("watermark_sql")?.extract()?,
        watermark_parts: get("watermark_parts")?.extract()?,
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
