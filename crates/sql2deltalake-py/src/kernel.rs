//! Reads of the load's Delta tables with delta-kernel-rs when the target is a local path:
//! no Python, no GIL, no pyarrow for the PK snapshots and the state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::{RecordBatch, RecordBatchIterator};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_default_engine::storage::store_from_url_opts;
use sql2deltalake_core::host::Batches;
use sql2deltalake_core::{Error, Result};

fn kernel_error(e: impl std::fmt::Display) -> Error {
    Error::msg(format!("delta-kernel: {e}"))
}

/// One engine per store root (scheme and authority), shared by all loads of the process.
fn engine(url: &url::Url) -> Result<Arc<dyn Engine>> {
    static ENGINES: OnceLock<Mutex<HashMap<String, Arc<dyn Engine>>>> = OnceLock::new();
    let key = format!("{}://{}", url.scheme(), url.authority());
    let mut engines = ENGINES.get_or_init(Default::default).lock().unwrap();
    if let Some(e) = engines.get(&key) {
        return Ok(e.clone());
    }
    let store = store_from_url_opts(url, Vec::<(String, String)>::new()).map_err(kernel_error)?;
    let engine: Arc<dyn Engine> = Arc::new(DefaultEngineBuilder::new(store).build());
    engines.insert(key, engine.clone());
    Ok(engine)
}

fn rejected_protocol(e: &delta_kernel::KernelError) -> bool {
    matches!(e.without_backtrace(), delta_kernel::KernelError::InvalidProtocol(_))
}

type Snap = (delta_kernel::SnapshotRef, Arc<dyn Engine>);

/// The snapshot of `path`, or `None` if delta-kernel rejects the table's protocol action: tables
/// restored by delta-rs (RESTORE writes `readerFeatures: []` with reader version 1) are valid for
/// delta-rs and Spark, so the caller reads them through the Python host instead.
fn snapshot(path: &str, version: Option<i64>) -> Result<Option<Snap>> {
    let url = delta_kernel::try_parse_uri(path).map_err(kernel_error)?;
    let engine = engine(&url)?;
    let mut builder = Snapshot::builder_for(url.as_str());
    if let Some(v) = version {
        builder = builder.at_version(v as u64);
    }
    match builder.build(engine.as_ref()) {
        Ok(snap) => Ok(Some((snap, engine))),
        Err(e) if rejected_protocol(&e) => {
            sql2deltalake_core::load::trace(&format!("kernel: {path}: {e}; reading it in Python"));
            Ok(None)
        }
        Err(e) => Err(kernel_error(e)),
    }
}

/// Whether `path` is a Delta table: a `_delta_log` with a commit or a checkpoint, as deltalake's
/// `TableNotFoundError` decides. Only a log directory that is not there means no table; one that
/// cannot be listed (permissions, I/O) or read is an error, as in Python. `None`: ask the Python
/// host (see [`snapshot`]); the same for the other functions here.
pub fn table_exists(path: &str) -> Result<Option<bool>> {
    let log = std::path::Path::new(path.strip_prefix("file://").unwrap_or(path)).join("_delta_log");
    let cannot_list = |e: std::io::Error| Error::msg(format!("cannot list {}: {e}", log.display()));
    let entries = match std::fs::read_dir(&log) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Some(false)),
        Err(e) => return Err(cannot_list(e)),
    };
    let mut has_commit = false;
    for entry in entries {
        let name = entry.map_err(cannot_list)?.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".json") || name.contains(".checkpoint.") {
            has_commit = true;
            break;
        }
    }
    if !has_commit {
        return Ok(Some(false));
    }
    Ok(snapshot(path, None)?.map(|_| true))
}

pub fn version(path: &str) -> Result<Option<i64>> {
    Ok(snapshot(path, None)?.map(|(s, _)| s.version() as i64))
}

pub fn column_names(path: &str) -> Result<Option<Vec<String>>> {
    Ok(snapshot(path, None)?.map(|(s, _)| s.schema().fields().map(|f| f.name().clone()).collect()))
}

/// The rows of the table (at `version`), only `columns` (matched ignoring case) if given.
pub fn read(path: &str, version: Option<i64>, columns: Option<&[String]>) -> Result<Option<Batches>> {
    let Some((snap, engine)) = snapshot(path, version)? else { return Ok(None) };
    let mut scan = snap.clone().scan_builder();
    if let Some(cols) = columns {
        let names: Vec<String> = snap.schema().fields().map(|f| f.name().clone()).collect();
        let resolved: Vec<String> = cols
            .iter()
            .map(|c| {
                names.iter().find(|n| *n == c).or_else(|| names.iter().find(|n| n.eq_ignore_ascii_case(c))).cloned()
            })
            .collect::<Option<_>>()
            .ok_or_else(|| Error::msg(format!("columns {cols:?} not all in {path}: {names:?}")))?;
        scan = scan.with_schema(snap.schema().project_as_struct(&resolved).map_err(kernel_error)?.into());
    }
    let scan = scan.build().map_err(kernel_error)?;
    let batches: Vec<RecordBatch> = scan
        .execute(engine)
        .map_err(kernel_error)?
        .map(|d| d.and_then(EngineDataArrowExt::try_into_record_batch))
        .collect::<Result<_, _>>()
        .map_err(kernel_error)?;
    let schema = match batches.first() {
        Some(b) => b.schema(),
        None => {
            let schema: delta_kernel::arrow::datatypes::Schema = match columns {
                Some(_) => scan.logical_schema().as_ref().try_into_arrow().map_err(kernel_error)?,
                None => snap.schema().as_ref().try_into_arrow().map_err(kernel_error)?,
            };
            Arc::new(schema)
        }
    };
    sql2deltalake_core::load::trace(&format!("kernel read {path}"));
    Ok(Some(Box::new(RecordBatchIterator::new(batches.into_iter().map(Ok), schema))))
}
