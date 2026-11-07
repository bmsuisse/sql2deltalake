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

fn snapshot(path: &str, version: Option<i64>) -> Result<(delta_kernel::SnapshotRef, Arc<dyn Engine>)> {
    let url = delta_kernel::try_parse_uri(path).map_err(kernel_error)?;
    let engine = engine(&url)?;
    let mut builder = Snapshot::builder_for(url.as_str());
    if let Some(v) = version {
        builder = builder.at_version(v as u64);
    }
    Ok((builder.build(engine.as_ref()).map_err(kernel_error)?, engine))
}

/// Whether `path` is a Delta table: a `_delta_log` with a commit or a checkpoint, as deltalake's
/// `TableNotFoundError` decides; a log that is there but cannot be read is an error, as in Python.
pub fn table_exists(path: &str) -> Result<bool> {
    let log = std::path::Path::new(path.strip_prefix("file://").unwrap_or(path)).join("_delta_log");
    let Ok(entries) = std::fs::read_dir(&log) else { return Ok(false) };
    let has_commit = entries.filter_map(|e| e.ok()).any(|e| {
        let name = e.file_name();
        let name = name.to_string_lossy();
        name.ends_with(".json") || name.contains(".checkpoint.")
    });
    if !has_commit {
        return Ok(false);
    }
    snapshot(path, None)?;
    Ok(true)
}

pub fn version(path: &str) -> Result<i64> {
    Ok(snapshot(path, None)?.0.version() as i64)
}

pub fn column_names(path: &str) -> Result<Vec<String>> {
    Ok(snapshot(path, None)?.0.schema().fields().map(|f| f.name().clone()).collect())
}

/// The rows of the table (at `version`), only `columns` (matched ignoring case) if given.
pub fn read(path: &str, version: Option<i64>, columns: Option<&[String]>) -> Result<Batches> {
    let (snap, engine) = snapshot(path, version)?;
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
    Ok(Box::new(RecordBatchIterator::new(batches.into_iter().map(Ok), schema)))
}
