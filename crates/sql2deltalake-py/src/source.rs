//! Source reads through arrowsql's Rust core: SQL Server over TDS straight into Arrow, without
//! arrowsql's Python package. The native load reads its source here without entering Python; a
//! Spark task reads its slice here and hands it to pyarrow as one C stream.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrowsql_core::{ConnectOptions, Connection, Partitioning, ReadOptions, Slice};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::arrow_ffi::{ArrowStream, Batches};

/// Where and how the source is read.
#[derive(Clone)]
pub struct SourceConn {
    pub options: ConnectOptions,
    pub utf8_strings: bool,
    pub partitions: usize,
    pub batch_size: usize,
}

impl SourceConn {
    pub fn new(connection_string: String, utf8_strings: bool, partitions: usize, batch_size: usize) -> SourceConn {
        SourceConn { options: ConnectOptions::new(connection_string), utf8_strings, partitions, batch_size }
    }

    /// The result of `sql` in arrowsql's types. With `partition_on` (an integer column of the
    /// result) and more than one partition, the read is split into ranges of it on parallel
    /// connections and the batches arrive unordered.
    pub fn open(
        &self,
        sql: &str,
        utf8_strings: bool,
        partition_on: Option<&str>,
    ) -> Result<Batches, arrowsql_core::Error> {
        let partitioning = match partition_on {
            Some(column) if self.partitions > 1 => {
                Partitioning::Range { column: column.to_string(), partitions: self.partitions }
            }
            _ if !utf8_strings => {
                // one statement on one connection: nothing to plan
                let slice = Slice { sql: sql.to_string(), params: vec![] };
                return Ok(Box::new(slice.open(&self.options, self.batch_size, 2)?));
            }
            _ => Partitioning::None,
        };
        let read = ReadOptions { batch_rows: self.batch_size, max_buffered: None, utf8_strings };
        Ok(Box::new(arrowsql_core::read_arrow_stream(&self.options, sql, partitioning, read)?))
    }

    /// The whole (small) result of `sql`, read within `timeout`: login, execution and fetch. A
    /// statement still running at the deadline is cancelled, and the error is HYT00 with the
    /// stage that took too long. For probes whose answer is optional: they must not stall a load.
    pub fn read_bounded(&self, sql: &str, timeout: Duration) -> Result<Batches, arrowsql_core::Error> {
        let started = Instant::now();
        let deadline = started + timeout;
        let mut options = self.options.clone();
        options.login_timeout = Some(timeout.as_secs().clamp(1, u64::from(u32::MAX)) as u32);
        let conn = Connection::connect(&options).map_err(|e| expired(e, "login", started, timeout))?;
        let done = Arc::new(AtomicBool::new(false));
        let watchdog = {
            let (done, cancel) = (done.clone(), conn.cancel_handle());
            std::thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    let now = Instant::now();
                    if now >= deadline {
                        // again until the read returns: the statement may not have started yet
                        cancel.cancel();
                    }
                    std::thread::sleep(deadline.saturating_duration_since(now).clamp(TICK, TICK * 4));
                }
            })
        };
        let read = (|| {
            let reader = conn.execute(sql, &[])?.into_reader(self.batch_size, 1)?;
            let schema = reader.schema();
            let batches = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| arrowsql_core::Error::driver("HY000", e.to_string()))?;
            Ok((schema, batches))
        })();
        done.store(true, Ordering::Release);
        let _ = watchdog.join();
        let (schema, batches) =
            read.map_err(|e| if Instant::now() >= deadline { expired(e, "query", started, timeout) } else { e })?;
        Ok(Box::new(RecordBatchIterator::new(batches.into_iter().map(Ok), schema)))
    }
}

const TICK: Duration = Duration::from_millis(25);

fn expired(e: arrowsql_core::Error, stage: &str, started: Instant, timeout: Duration) -> arrowsql_core::Error {
    if started.elapsed() < timeout && e.sqlstate != "HYT00" {
        return e;
    }
    // without the driver prefix ("[Microsoft][ODBC Driver 18 for SQL Server]") the new error adds
    let cause = e.message.rsplit(']').next().unwrap_or(&e.message);
    arrowsql_core::Error::driver(
        "HYT00",
        format!("{stage} did not finish within {} s ({cause})", timeout.as_secs_f32()),
    )
}

pub fn source_error(e: arrowsql_core::Error) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// `sql` read through arrowsql's Rust core as an Arrow stream in arrowsql's types (see
/// [`SourceConn::open`]). With `timeout` (seconds) the whole result is read within it, login
/// included (see [`SourceConn::read_bounded`]); not with `partition_on` or `utf8_strings`.
#[pyfunction]
#[pyo3(signature = (connection_string, sql, batch_size=65536, utf8_strings=false, partition_on=None, partitions=1, timeout=None))]
#[allow(clippy::too_many_arguments)]
pub fn read_sql(
    py: Python<'_>,
    connection_string: String,
    sql: String,
    batch_size: usize,
    utf8_strings: bool,
    partition_on: Option<String>,
    partitions: usize,
    timeout: Option<f64>,
) -> PyResult<ArrowStream> {
    let source = SourceConn::new(connection_string, utf8_strings, partitions, batch_size);
    let reader = match timeout {
        Some(t) => {
            if partition_on.is_some() || utf8_strings {
                return Err(PyValueError::new_err("timeout does not go with partition_on or utf8_strings"));
            }
            if !(t > 0.0 && t.is_finite()) {
                return Err(PyValueError::new_err("timeout must be a positive number of seconds"));
            }
            py.detach(|| source.read_bounded(&sql, Duration::from_secs_f64(t)))
        }
        None => py.detach(|| source.open(&sql, utf8_strings, partition_on.as_deref())),
    }
    .map_err(source_error)?;
    Ok(ArrowStream::new(reader))
}
