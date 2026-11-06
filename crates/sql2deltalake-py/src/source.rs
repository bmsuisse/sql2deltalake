//! Source reads through arrowsql's Rust core: SQL Server over TDS straight into Arrow, without
//! arrowsql's Python package. The native load reads its source here without entering Python; a
//! Spark task reads its slice here and hands it to pyarrow as one C stream.

use arrowsql_core::{ConnectOptions, Partitioning, ReadOptions, Slice};
use pyo3::exceptions::PyRuntimeError;
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
}

pub fn source_error(e: arrowsql_core::Error) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// `sql` read through arrowsql's Rust core as an Arrow stream in arrowsql's types (see
/// [`SourceConn::open`]).
#[pyfunction]
#[pyo3(signature = (connection_string, sql, batch_size=65536, utf8_strings=false, partition_on=None, partitions=1))]
pub fn read_sql(
    py: Python<'_>,
    connection_string: String,
    sql: String,
    batch_size: usize,
    utf8_strings: bool,
    partition_on: Option<String>,
    partitions: usize,
) -> PyResult<ArrowStream> {
    let source = SourceConn::new(connection_string, utf8_strings, partitions, batch_size);
    let reader = py.detach(|| source.open(&sql, utf8_strings, partition_on.as_deref())).map_err(source_error)?;
    Ok(ArrowStream::new(reader))
}
