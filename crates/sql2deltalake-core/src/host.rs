//! What the load needs from its environment: the source, the Delta tables of the target folder
//! and a log. The Python bindings implement it with the reader/destination objects; the load
//! itself never touches Python.

use arrow_array::RecordBatchReader;

use crate::Result;

pub type Batches = Box<dyn RecordBatchReader + Send>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Overwrite,
    Append,
}

/// `WriteConfig.allow_schema_drift`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Drift {
    No,
    Yes,
    NewOnly,
}

impl Drift {
    /// Python truthiness: `True` and `"new_only"`.
    pub fn allowed(self) -> bool {
        self != Drift::No
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Info,
    Warning,
    Error,
}

/// Optional fields of a log message.
#[derive(Default, Clone, Copy)]
pub struct LogArgs<'a> {
    pub sql: Option<&'a str>,
    pub load: Option<&'a str>,
    pub sub_load: Option<&'a str>,
}

/// Paths (`rel`) are relative to the target folder: `delta`, `delta_load/latest_pk_version`, `meta/lock.txt`.
pub trait Host: Sync {
    /// Runs a (small) query on the source; batches as the source delivers them.
    fn source_query(&self, sql: &str) -> Result<Batches>;
    /// Streams a data read from the source; `None` if the host copies source reads itself
    /// ([`Host::source_copy`]), for example a reader that splits queries into slices.
    /// `partition_on`: an integer key column of the result the host may split the read on
    /// (parallel range reads); the batches may then arrive in any order.
    fn source_stream(&self, sql: &str, partition_on: Option<&str>) -> Result<Option<Batches>>;
    /// Writes the result of `sql` to the Delta table `rel` (used when `source_stream` gives `None`).
    fn source_copy(&self, sql: &str, rel: &str, mode: Mode, drift: Drift) -> Result<()>;

    fn table_exists(&self, rel: &str) -> Result<bool>;
    fn path_exists(&self, rel: &str) -> Result<bool>;
    fn version(&self, rel: &str) -> Result<i64>;
    /// The rows of a Delta table (at `version`), only `columns` if given.
    fn read(&self, rel: &str, version: Option<i64>, columns: Option<&[String]>) -> Result<Batches>;
    fn column_names(&self, rel: &str) -> Result<Vec<String>>;
    /// Writes `data` with the schema-drift policy `drift`; an empty overwrite still creates the table.
    fn write(&self, rel: &str, data: Batches, mode: Mode, drift: Drift) -> Result<()>;
    fn restore(&self, rel: &str, version: i64) -> Result<()>;
    /// Removes a file, or a folder with `recursive`.
    fn remove(&self, rel: &str, recursive: bool) -> Result<()>;
    fn vacuum(&self, rel: &str) -> Result<()>;
    fn mkdir(&self, rel: &str) -> Result<()>;
    fn upload_str(&self, rel: &str, content: &str) -> Result<()>;
    /// Seconds since the last modification, `None` if the file does not exist.
    fn file_age_seconds(&self, rel: &str) -> Result<Option<f64>>;

    fn log(&self, level: Level, msg: &str, args: LogArgs<'_>) -> Result<()>;
    /// A test hook at named steps of the load: may raise, or return `Some(false)` to skip a step.
    fn fail_point(&self, name: &str) -> Result<Option<bool>>;
}
