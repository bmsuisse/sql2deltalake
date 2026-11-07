//! The load: full load, delta load (PK snapshot diff, strange updates, deletes), simple delta,
//! append-only inserts, the no-updates shortcut and the restore of the state after a failure.
//! Same steps, files and log messages as the Python implementation it replaces.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StringArray, new_null_array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::host::{Batches, Drift, Host, Level, LogArgs, Mode};
use crate::keys::{self, KeyEncoder};
use crate::normalize::{NormalizedReader, has_time_columns};
use crate::scalar::{Scalar, scalar_at};
use crate::sqlgen::{
    self, Column, Dialect, IS_DELETED_COL_NAME, IS_FULL_LOAD_COL_NAME, SourceSql, VALID_FROM_COL_NAME, ValidFrom,
};
use crate::table::{self, collect, project};
use crate::{Error, Result};

pub const DELTA: &str = "delta";
pub const DELTA_1: &str = "delta_load/delta_1";
pub const DELTA_2: &str = "delta_load/delta_2";
pub const PRIMARY_KEYS_TS: &str = "delta_load/primary_keys_ts";
pub const LATEST_PK_VERSION: &str = "delta_load/latest_pk_version";
pub const LAST_LOAD_STATE: &str = "delta_load/last_load_state";
const LOCK_FILE: &str = "meta/lock.txt";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadMode {
    Overwrite,
    Append,
    ForceFull,
    AppendInserts,
    SimpleDelta,
    SimpleDeltaCheck,
}

impl LoadMode {
    pub fn parse(s: &str) -> Option<LoadMode> {
        Some(match s {
            "overwrite" => LoadMode::Overwrite,
            "append" => LoadMode::Append,
            "force_full" => LoadMode::ForceFull,
            "append_inserts" => LoadMode::AppendInserts,
            "simple_delta" => LoadMode::SimpleDelta,
            "simple_delta_check" => LoadMode::SimpleDeltaCheck,
            _ => return None,
        })
    }
}

/// Everything about one table load, resolved by the caller (columns, keys, names, types).
pub struct LoadSpec {
    pub sql: SourceSql,
    pub cols: Vec<Column>,
    /// Indices into `cols`.
    pub pks: Vec<usize>,
    pub delta_col: Option<usize>,
    pub load_mode: LoadMode,
    pub drift: Drift,
    pub no_complex_entries_load: bool,
    /// Fingerprint of the source schema, stored with the load state.
    pub schema_fingerprint: String,
    /// Whether MAX/COUNT of the delta column can tell that nothing changed (not for Postgres
    /// `xmin` nor an automatically picked AS ROW START column: both can commit late).
    pub shortcut_allowed: bool,
    /// The query of `MIN_ACTIVE_ROWVERSION()` as `mar`, in the table's database; `None` if it
    /// cannot be read (no rowversion delta column, a query over several databases).
    pub watermark_sql: Option<String>,
    /// T-SQL: the `MIN_ACTIVE_ROWVERSION()` expression and the quoted database to read it in,
    /// for the one query that reads the clock, the watermark and MAX/COUNT of a delta load.
    pub watermark_parts: Option<(String, Option<String>)>,
}

/// The `__timestamp` of a pass from the source clock: strictly later than `after`, the previous
/// pass of the same load, even if the clock stands still or goes back.
pub fn pass_micros(clock: i64, after: Option<i64>) -> i64 {
    match after {
        Some(prev) if clock <= prev => prev + 1,
        _ => clock,
    }
}

/// `(MAX(delta value), COUNT(*))`.
pub type State = (Scalar, i64);

#[derive(Clone, Debug, PartialEq)]
pub enum LoadResult {
    Full,
    Delta {
        starting_local_state: State,
        starting_source_state: State,
        end_source_state: Option<State>,
        dirty: Option<bool>,
    },
    NoLoad,
    AppendOnly,
}

/// The `__timestamp` of the current pass.
#[derive(Clone, Debug)]
struct Pass {
    valid_from: ValidFrom,
    micros: Option<i64>,
}

/// What this load wrote, for the load state: the delta-value maximum and row count of
/// `latest_pk_version` and the delta-value maximum of `primary_keys_ts`.
#[derive(Default)]
struct Written {
    latest_pk: Option<(Scalar, i64)>,
    pk_snapshot_max: Option<Scalar>,
}

struct Load<'a, H: Host> {
    host: &'a H,
    spec: &'a LoadSpec,
    pass: Pass,
    delta_col: Option<usize>,
    /// Shared by all passes of the load.
    written: Arc<Mutex<Written>>,
    /// MAX/COUNT of the source read with the clock (and the query that read it), for the first
    /// `source_ts_cnt` of the pass.
    prefetched: Mutex<Option<(String, State)>>,
}

/// Step timings on stderr with `SQL2DELTA_TRACE=1`.
pub fn trace(step: &str) {
    use std::sync::OnceLock;
    use std::time::Instant;
    static ON: OnceLock<bool> = OnceLock::new();
    static LAST: Mutex<Option<Instant>> = Mutex::new(None);
    if !*ON.get_or_init(|| std::env::var("SQL2DELTA_TRACE").is_ok_and(|v| v == "1")) {
        return;
    }
    let now = Instant::now();
    let mut last = LAST.lock().unwrap();
    if let Some(prev) = *last {
        eprintln!("[sql2deltalake] {:>8.1} ms  {step}", (now - prev).as_secs_f64() * 1000.0);
    }
    *last = Some(now);
}

fn now_micros() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}

/// `r`, or what `fallback` makes of a recoverable error; interrupts are passed on unhandled.
fn recover<T>(r: Result<T>, fallback: impl FnOnce(Error) -> Result<T>) -> Result<T> {
    match r {
        Err(e) if e.is_recoverable() => fallback(e),
        other => other,
    }
}

/// Delta values as the shortcut compares them: a rowversion read from the source as bytes
/// equals the local `CAST(rowversion AS BIGINT)`; timestamps compare by instant.
fn comparable(v: &Scalar) -> Scalar {
    match v {
        Scalar::Bytes(_) => v.as_i64().map_or(v.clone(), Scalar::Int),
        Scalar::Timestamp { micros, .. } => Scalar::Timestamp { micros: *micros, utc: false },
        other => other.clone(),
    }
}

fn less(a: &Scalar, b: &Scalar) -> bool {
    match (a, b) {
        (Scalar::Int(a), Scalar::Int(b)) => a < b,
        _ => false,
    }
}

/// Tees the key columns of a stream that is being written, so they need no re-read.
struct Tee {
    inner: Batches,
    keep: Option<Vec<usize>>,
    out: Arc<Mutex<Vec<RecordBatch>>>,
}

impl Iterator for Tee {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next();
        if let Some(Ok(b)) = &next {
            let kept = match &self.keep {
                None => Ok(b.clone()),
                Some(idx) => b.project(idx),
            };
            match kept {
                Ok(k) => self.out.lock().unwrap().push(k),
                Err(e) => return Some(Err(e)),
            }
        }
        next
    }
}

impl RecordBatchReader for Tee {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// Makes every field nullable on the way to the writer (source data is written all-nullable).
struct Nullable {
    inner: Batches,
    schema: SchemaRef,
}

impl Iterator for Nullable {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                // writers choke on empty batches (deltalake's schema cast); the schema says it all
                Ok(b) if b.num_rows() == 0 => continue,
                Ok(b) => return Some(b.with_schema(self.schema.clone())),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl RecordBatchReader for Nullable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// The stream as the writer gets it: all fields nullable, no empty batches.
fn nullable(inner: Batches) -> Batches {
    let schema = table::nullable_schema(&inner.schema());
    Box::new(Nullable { inner, schema })
}

impl<'a, H: Host> Load<'a, H> {
    fn info(&self, msg: &str, args: LogArgs<'_>) -> Result<()> {
        self.host.log(Level::Info, msg, args)
    }

    fn warn(&self, msg: &str) -> Result<()> {
        self.host.log(Level::Warning, msg, LogArgs::default())
    }

    fn dialect(&self) -> Dialect {
        self.spec.sql.dialect
    }

    fn col(&self, i: usize) -> &Column {
        &self.spec.cols[i]
    }

    fn all_cols(&self) -> Vec<&Column> {
        self.spec.cols.iter().collect()
    }

    fn delta_col(&self) -> Result<&Column> {
        self.delta_col.map(|i| self.col(i)).ok_or_else(|| Error::msg("Must have a delta_col for delta loads"))
    }

    fn pk_targets(&self) -> Vec<String> {
        self.spec.pks.iter().map(|i| self.col(*i).target.clone()).collect()
    }

    /// PK target names plus the delta column's.
    fn pk_ts_targets(&self) -> Vec<String> {
        let mut t = self.pk_targets();
        if let Some(d) = self.delta_col {
            t.push(self.col(d).target.clone());
        }
        t
    }

    /// The first primary key if it is an integer: data reads may be split into ranges of it.
    fn partition_column(&self) -> Option<&str> {
        let c = self.col(*self.spec.pks.first()?);
        matches!(c.type_key.as_str(), "int" | "bigint" | "smallint" | "tinyint" | "integer")
            .then_some(c.target.as_str())
    }

    fn is_rowversion(&self) -> bool {
        self.dialect() == Dialect::Tsql
            && self
                .delta_col
                .is_some_and(|d| matches!(self.col(d).orig_type.to_lowercase().as_str(), "timestamp" | "rowversion"))
    }

    // ---- source ----------------------------------------------------------------------------

    fn query_rows(&self, sql: &str) -> Result<RecordBatch> {
        collect(self.host.source_query(sql)?)
    }

    fn query_scalar(&self, sql: &str, col: &str) -> Result<Scalar> {
        let b = self.query_rows(sql)?;
        if b.num_rows() == 0 {
            return Err(Error::msg(format!("no rows from {sql}")));
        }
        scalar_at(b.column(table::index_of(&b.schema(), col)?), 0)
    }

    /// Scale of the `time` columns of a query by position (Arrow's time64 does not carry it).
    fn time_scales(&self, sql: &str) -> Result<HashMap<usize, u8>> {
        if self.dialect() != Dialect::Tsql {
            return Ok(HashMap::new());
        }
        let describe = format!(
            "SELECT d.column_ordinal, d.scale FROM sys.dm_exec_describe_first_result_set(N{}, NULL, 0) AS d WHERE d.is_hidden = 0 AND d.system_type_id = 41",
            sqlgen::string_literal(sql)
        );
        let read = || -> Result<HashMap<usize, u8>> {
            let b = self.query_rows(&describe)?;
            let mut out = HashMap::new();
            for i in 0..b.num_rows() {
                let ord = scalar_at(b.column(0), i)?.as_i64().unwrap_or(1);
                let scale = scalar_at(b.column(1), i)?.as_i64().unwrap_or(7);
                out.insert((ord - 1).max(0) as usize, scale as u8);
            }
            Ok(out)
        };
        recover(read(), |e| {
            let _ = self.warn(&format!("Could not read the scale of time columns, using 7: {e}"));
            Ok(HashMap::new())
        })
    }

    /// Writes the result of `sql` to `rel`; returns the columns `keep` (all if `None`) of the
    /// written rows, teed from the stream, or `None` if the host copied the rows itself.
    fn source_to_delta_teed(
        &self,
        sql: &str,
        rel: &str,
        mode: Mode,
        drift: Drift,
        keep: Option<&[String]>,
    ) -> Result<Option<RecordBatch>> {
        let Some(stream) = self.host.source_stream(sql, self.partition_column())? else {
            self.host.source_copy(sql, rel, mode, drift)?;
            return Ok(None);
        };
        let scales = if has_time_columns(&stream.schema()) { self.time_scales(sql)? } else { HashMap::new() };
        let stream = NormalizedReader::wrap(stream, scales);
        let schema = stream.schema();
        let keep_idx =
            keep.map(|k| k.iter().map(|n| table::index_of(&schema, n)).collect::<Result<Vec<_>>>()).transpose()?;
        let kept_schema = match &keep_idx {
            None => schema.clone(),
            Some(idx) => Arc::new(schema.project(idx)?),
        };
        let out = Arc::new(Mutex::new(Vec::new()));
        let tee = Tee { inner: stream, keep: keep_idx, out: out.clone() };
        self.host.write(rel, nullable(Box::new(tee)), mode, drift)?;
        let parts = std::mem::take(&mut *out.lock().unwrap());
        Ok(Some(arrow_select::concat::concat_batches(&kept_schema, parts.iter())?))
    }

    /// Writes the result of `sql` to the staging table `rel` (overwrite) and returns its rows.
    fn source_to_staging(&self, sql: &str, rel: &str, mode: Mode, drift: Drift) -> Result<RecordBatch> {
        match self.source_to_delta_teed(sql, rel, mode, drift, None)? {
            Some(rows) => Ok(rows),
            None => collect(self.host.read(rel, None, None)?),
        }
    }

    // ---- load timestamp --------------------------------------------------------------------

    /// Reads the source clock once, as the `__timestamp` literal of all reads of one pass;
    /// later than `after` (an earlier pass of the same load).
    fn fetch_pass(&self, after: Option<i64>) -> Result<Pass> {
        let v = self.query_scalar(sqlgen::load_timestamp_query(self.dialect()), "ts")?;
        let Scalar::Timestamp { micros, .. } = v else {
            return Err(Error::msg(format!("unexpected clock value {v:?}")));
        };
        Ok(self.pass_at(pass_micros(micros, after)))
    }

    fn pass_at(&self, micros: i64) -> Pass {
        Pass {
            valid_from: ValidFrom::Literal(sqlgen::load_timestamp_literal(self.dialect(), micros)),
            micros: Some(micros),
        }
    }

    fn first_pass(&self) -> Result<Pass> {
        recover(self.fetch_pass(None), |e| {
            self.warn(&format!(
                "Could not read the load timestamp from the source, the source clock is used per query: {e}"
            ))?;
            Ok(Pass { valid_from: ValidFrom::SourceClock, micros: None })
        })
    }

    /// For a further source read of the same load that can return rows an earlier pass already
    /// appended: a later `__timestamp`, so the newer version of a row sorts last.
    fn next_pass(&self) -> Result<Pass> {
        let Some(prev) = self.pass.micros else { return Ok(self.pass.clone()) };
        recover(self.fetch_pass(Some(prev)), |e| {
            self.warn(&format!(
                "Could not read the load timestamp from the source, using the previous one plus 1 microsecond: {e}"
            ))?;
            Ok(self.pass_at(prev + 1))
        })
    }

    fn with_pass(&self, pass: Pass) -> Load<'a, H> {
        Load {
            host: self.host,
            spec: self.spec,
            pass,
            delta_col: self.delta_col,
            written: self.written.clone(),
            prefetched: Mutex::new(None),
        }
    }

    /// `MIN_ACTIVE_ROWVERSION()` before the load reads data: every lower rowversion is committed
    /// and visible to the reads of this load.
    fn fetch_watermark(&self) -> Result<Option<i64>> {
        let Some(sql) = self.spec.watermark_sql.as_deref().filter(|_| self.is_rowversion()) else {
            return Ok(None);
        };
        recover(self.query_scalar(sql, "mar").map(|v| v.as_i64()), |e| {
            self.warn(&format!("Could not read MIN_ACTIVE_ROWVERSION(): {e}"))?;
            Ok(None)
        })
    }

    /// For a delta load on T-SQL: the clock, `MIN_ACTIVE_ROWVERSION()` and MAX/COUNT of the
    /// source in one round trip. Sets the pass and keeps MAX/COUNT for `source_ts_cnt`; returns
    /// the watermark, or `None` if the query failed (then they are read one by one).
    fn prefetch_source_state(&mut self) -> Result<Option<Option<i64>>> {
        let watermark = self.spec.watermark_parts.as_ref().filter(|_| self.is_rowversion());
        let sql = self.spec.sql.source_state(
            self.delta_col.map(|d| self.col(d)),
            watermark.map(|(expr, db)| (expr.as_str(), db.as_deref())),
        );
        let read = || -> Result<(i64, State, Option<i64>)> {
            let b = self.query_rows(&sql)?;
            if b.num_rows() == 0 {
                return Err(Error::msg(format!("no rows from {sql}")));
            }
            let at = |name: &str| -> Result<Scalar> { scalar_at(b.column(table::index_of(&b.schema(), name)?), 0) };
            let Scalar::Timestamp { micros, .. } = at("ts")? else {
                return Err(Error::msg("unexpected clock value"));
            };
            let cnt = at("cnt")?.as_i64().unwrap_or(0);
            let mar = if watermark.is_some() { at("mar")?.as_i64() } else { None };
            Ok((micros, (at("max_ts")?, cnt), mar))
        };
        match read() {
            Ok((micros, state, mar)) => {
                self.pass = self.pass_at(pass_micros(micros, None));
                *self.prefetched.lock().unwrap() = Some((sql, state));
                Ok(Some(mar))
            }
            Err(e) if !e.is_recoverable() => Err(e),
            Err(e) => {
                self.warn(&format!("Could not read the source state in one query, reading it step by step: {e}"))?;
                Ok(None)
            }
        }
    }

    // ---- local state -------------------------------------------------------------------------

    fn delta_max(&self, batch: &RecordBatch) -> Result<Scalar> {
        match self.delta_col {
            Some(d) => max_of(batch.column(table::index_of(&batch.schema(), &self.col(d).target)?)),
            None => Ok(Scalar::Null),
        }
    }

    /// Overwrites `latest_pk_version` and remembers its delta-value maximum and row count.
    fn write_latest_pk(&self, lpk: RecordBatch) -> Result<()> {
        let stats = (self.delta_max(&lpk)?, lpk.num_rows() as i64);
        self.host.write(LATEST_PK_VERSION, nullable(table::single(lpk)), Mode::Overwrite, Drift::Yes)?;
        self.written.lock().unwrap().latest_pk = Some(stats);
        Ok(())
    }

    /// Max and count of the delta column, from the last PK snapshot if there is one.
    fn local_delta_value_and_count(&self) -> Result<State> {
        let rel = if self.host.path_exists(PRIMARY_KEYS_TS)? { PRIMARY_KEYS_TS } else { DELTA };
        let Some(d) = self.delta_col else {
            let b = collect(self.host.read(rel, None, None)?)?;
            return Ok((Scalar::Null, b.num_rows() as i64));
        };
        let target = self.col(d).target.clone();
        let b = collect(self.host.read(rel, None, Some(std::slice::from_ref(&target)))?)?;
        Ok((max_of(b.column(0))?, b.num_rows() as i64))
    }

    fn source_ts_cnt(&self) -> Result<State> {
        if let Some((sql, state)) = self.prefetched.lock().unwrap().take() {
            self.info("Retrieve source delta value / count", LogArgs { sql: Some(&sql), ..Default::default() })?;
            return Ok(state);
        }
        let sql = self.spec.sql.source_ts_cnt(self.delta_col.map(|d| self.col(d)));
        self.info("Retrieve source delta value / count", LogArgs { sql: Some(&sql), ..Default::default() })?;
        let b = self.query_rows(&sql)?;
        let max = scalar_at(b.column(table::index_of(&b.schema(), "max_ts")?), 0)?;
        let cnt = scalar_at(b.column(table::index_of(&b.schema(), "cnt")?), 0)?.as_i64().unwrap_or(0);
        Ok((max, cnt))
    }

    fn write_last_load_state(&self, watermark: Option<i64>) -> Result<()> {
        if !self.host.table_exists(LATEST_PK_VERSION)? {
            if self.host.path_exists(LAST_LOAD_STATE)? {
                self.host.remove(LAST_LOAD_STATE, true)?;
            }
            return Ok(());
        }
        let version = self.host.version(LATEST_PK_VERSION)?;
        // what the next load's no-updates check needs, so it reads no table data
        let written = self.written.lock().unwrap();
        let pk_snapshot_version =
            if self.host.path_exists(PRIMARY_KEYS_TS)? { self.host.version(PRIMARY_KEYS_TS)? } else { -1 };
        let lpk = written.latest_pk.as_ref().map(|(m, c)| (comparable(m), *c));
        let local_value_present = if pk_snapshot_version >= 0 {
            written.pk_snapshot_max.as_ref().map(|m| !m.is_null())
        } else {
            // the next load takes the delta value from the data, which holds every latest PK row
            lpk.as_ref().filter(|(m, _)| !m.is_null()).map(|_| true)
        };
        let max = lpk.as_ref().map(|(m, _)| m).filter(|m| !m.is_null());
        let schema = Arc::new(Schema::new(vec![
            Field::new("latest_pk_version", DataType::Int64, true),
            Field::new("source_schema", DataType::Utf8, true),
            Field::new("min_active_rowversion", DataType::Int64, true),
            Field::new("latest_pk_max_int", DataType::Int64, true),
            Field::new("latest_pk_max_text", DataType::Utf8, true),
            Field::new("latest_pk_count", DataType::Int64, true),
            Field::new("primary_keys_ts_version", DataType::Int64, true),
            Field::new("local_value_present", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![version])),
                Arc::new(StringArray::from(vec![self.spec.schema_fingerprint.as_str()])),
                Arc::new(Int64Array::from(vec![watermark])),
                Arc::new(Int64Array::from(vec![max.and_then(|m| m.as_i64())])),
                Arc::new(StringArray::from(vec![max.map(|m| m.to_text())])),
                Arc::new(Int64Array::from(vec![lpk.as_ref().map(|(_, c)| *c)])),
                Arc::new(Int64Array::from(vec![Some(pk_snapshot_version)])),
                Arc::new(BooleanArray::from(vec![local_value_present])),
            ],
        )?;
        drop(written);
        self.host.write(LAST_LOAD_STATE, nullable(table::single(batch)), Mode::Overwrite, Drift::Yes)
    }

    /// The no-updates check from the load state alone (no table data read); `None` if the state
    /// does not have what it needs (written by an older version, or a load that did not write
    /// `latest_pk_version`) or `primary_keys_ts` changed since.
    fn up_to_date_from_state(&self, source_delta: &Scalar, source_count: i64) -> Result<Option<bool>> {
        if source_delta.is_null() || !self.host.table_exists(LAST_LOAD_STATE)? {
            return Ok(None);
        }
        let state = collect(self.host.read(LAST_LOAD_STATE, None, None)?)?;
        if state.num_rows() != 1 {
            return Ok(None);
        }
        let get = |n: &str| -> Result<Scalar> {
            match table::index_of(&state.schema(), n) {
                Ok(i) => scalar_at(state.column(i), 0),
                Err(_) => Ok(Scalar::Null),
            }
        };
        let (count, local_present, pkts_version) =
            (get("latest_pk_count")?, get("local_value_present")?, get("primary_keys_ts_version")?);
        if count.is_null() || local_present != Scalar::Bool(true) || pkts_version.is_null() {
            return Ok(None);
        }
        let pkts_now = if self.host.path_exists(PRIMARY_KEYS_TS)? { self.host.version(PRIMARY_KEYS_TS)? } else { -1 };
        if pkts_version.as_i64() != Some(pkts_now) || !self.host.table_exists(LATEST_PK_VERSION)? {
            return Ok(None);
        }
        if get("latest_pk_version")?.as_i64() != Some(self.host.version(LATEST_PK_VERSION)?)
            || get("source_schema")? != Scalar::Str(self.spec.schema_fingerprint.clone())
        {
            return Ok(Some(false));
        }
        let source = comparable(source_delta);
        let max_matches = match (&source, get("latest_pk_max_int")?, get("latest_pk_max_text")?) {
            (Scalar::Int(s), Scalar::Int(l), _) => *s == l,
            (s, _, Scalar::Str(l)) => s.to_text() == l,
            _ => false,
        };
        if !max_matches || count.as_i64() != Some(source_count) {
            return Ok(Some(false));
        }
        if self.is_rowversion() {
            let (local, watermark) = (get("latest_pk_max_int")?, get("min_active_rowversion")?);
            return Ok(Some(!watermark.is_null() && less(&local, &watermark)));
        }
        Ok(Some(true))
    }

    /// True if the PKs and delta values written by the last successful load match the source.
    fn target_is_up_to_date(&self, source_delta: &Scalar, source_count: i64) -> Result<bool> {
        let d = self.delta_col()?;
        if source_delta.is_null()
            || !self.host.table_exists(LATEST_PK_VERSION)?
            || !self.host.table_exists(LAST_LOAD_STATE)?
        {
            return Ok(false);
        }
        let state = collect(self.host.read(LAST_LOAD_STATE, None, None)?)?;
        if state.num_rows() != 1 {
            return Ok(false);
        }
        let get = |n: &str| -> Result<Scalar> { scalar_at(state.column(table::index_of(&state.schema(), n)?), 0) };
        if get("latest_pk_version")?.as_i64() != Some(self.host.version(LATEST_PK_VERSION)?)
            || get("source_schema")? != Scalar::Str(self.spec.schema_fingerprint.clone())
        {
            return Ok(false);
        }
        let lpk = collect(self.host.read(LATEST_PK_VERSION, None, Some(std::slice::from_ref(&d.target)))?)?;
        let local_max = comparable(&max_of(lpk.column(0))?);
        if local_max.is_null() || (local_max.clone(), lpk.num_rows() as i64) != (comparable(source_delta), source_count)
        {
            return Ok(false);
        }
        if self.is_rowversion() {
            let watermark = get("min_active_rowversion")?;
            return Ok(!watermark.is_null() && less(&local_max, &watermark));
        }
        Ok(true)
    }

    // ---- loads -------------------------------------------------------------------------------

    fn full_load(&self, mode: Mode) -> Result<LoadResult> {
        self.info("Start Full Load", LogArgs::default())?;
        let sql = self.spec.sql.full_load(&self.all_cols(), &self.pass.valid_from);
        self.info("executing sql", LogArgs { sql: Some(&sql), load: Some("full"), ..Default::default() })?;
        let keep = if self.delta_col.is_some() { self.pk_ts_targets() } else { vec![] };
        trace("full load: start");
        let teed = self.source_to_delta_teed(&sql, DELTA, mode, self.spec.drift, Some(&keep))?;
        trace("full load: source to delta");
        if self.delta_col.is_none() {
            self.info("Full Load done", LogArgs::default())?;
            return Ok(LoadResult::Full);
        }
        self.info(" Full Load done, write meta for delta load", LogArgs::default())?;
        self.host.mkdir("delta_load")?;
        let lpk = match teed {
            Some(t) => t,
            None => self.last_full_load_keys()?,
        };
        self.write_latest_pk(lpk)?;
        trace("full load: latest pks written");
        Ok(LoadResult::Full)
    }

    /// PKs and delta values of the rows of the latest full load in the Delta table.
    fn last_full_load_keys(&self) -> Result<RecordBatch> {
        let mut cols = self.pk_ts_targets();
        cols.push(VALID_FROM_COL_NAME.into());
        cols.push(IS_FULL_LOAD_COL_NAME.into());
        let all = collect(self.host.read(DELTA, None, Some(&cols))?)?;
        let n = cols.len();
        let full = table::filter(&all, &table::true_mask(all.column(n - 1))?)?;
        let keep = self.pk_ts_targets();
        let Some(i) = keys::argmax(full.column(n - 2))? else { return project(&full.slice(0, 0), &keep) };
        let max = full.column(n - 2).slice(i, 1);
        let mask = arrow_ord::cmp::eq(full.column(n - 2), &arrow_array::Scalar::new(max))?;
        project(&table::filter(&full, &mask)?, &keep)
    }

    fn retrieve_primary_key_data(&self) -> Result<RecordBatch> {
        let mut cols: Vec<&Column> = self.spec.pks.iter().map(|i| self.col(*i)).collect();
        cols.push(self.delta_col()?);
        let sql = self.spec.sql.pk_ts(&cols);
        self.info("Retrieve all PK/TS", LogArgs { sql: Some(&sql), ..Default::default() })?;
        let pkts = self.source_to_staging(&sql, PRIMARY_KEYS_TS, Mode::Overwrite, Drift::Yes)?;
        self.written.lock().unwrap().pk_snapshot_max = Some(self.delta_max(&pkts)?);
        Ok(pkts)
    }

    /// Reads `sql` into the staging table `delta_load/delta_1` and appends it to the data.
    fn load_updates_to_delta(&self, sql: &str) -> Result<RecordBatch> {
        self.host.fail_point("load_updates")?;
        self.info("Executing sql", LogArgs { sql: Some(sql), load: Some("delta"), sub_load: Some("delta_1") })?;
        let d1 = self.source_to_staging(sql, DELTA_1, Mode::Overwrite, Drift::Yes)?;
        if d1.num_rows() > 0 {
            self.host.write(DELTA, nullable(table::single(d1.clone())), Mode::Append, self.spec.drift)?;
        }
        Ok(d1)
    }

    fn criterion(&self, op: &str, value: &Scalar) -> Result<String> {
        Ok(self.spec.sql.criterion(self.delta_col()?, op, value))
    }

    fn append_inserts(&self) -> Result<LoadResult> {
        self.info(&format!("Start Append Only Load with Delta Column {}", self.delta_col()?.name), LogArgs::default())?;
        let (dlv, _) = self.local_delta_value_and_count()?;
        let criteria = if dlv.is_null() { vec![] } else { vec![self.criterion(">", &dlv)?] };
        self.info(" Start delta step 2, load updates by timestamp", LogArgs::default())?;
        let sql = self.spec.sql.updates(&self.all_cols(), &criteria, &self.pass.valid_from);
        self.load_updates_to_delta(&sql)?;
        self.info("Done Append only load", LogArgs::default())?;
        Ok(LoadResult::AppendOnly)
    }

    fn delta_load(&self, simple: bool, simple_check: bool) -> Result<LoadResult> {
        let dc = self.delta_col()?;
        let existing: Vec<String> = self.host.column_names(DELTA)?.iter().map(|c| c.to_lowercase()).collect();
        let missing: Vec<&str> = self
            .spec
            .cols
            .iter()
            .filter(|c| !existing.contains(&c.target.to_lowercase()))
            .map(|c| c.target.as_str())
            .collect();
        if !missing.is_empty() && self.spec.drift.allowed() {
            self.warn(&format!("New columns from source: {}. Do a full load", py_list(&missing)))?;
            return self.full_load(Mode::Append);
        }
        let pk_names: Vec<&str> = self.spec.pks.iter().map(|i| self.col(*i).name.as_str()).collect();
        self.info(
            &format!(
                "Start {}Delta Load with Delta Column {} and pks: {}",
                if simple { "SIMPLE " } else { "" },
                dc.name,
                pk_names.join(", ")
            ),
            LogArgs::default(),
        )?;
        if !simple {
            if !self.host.table_exists(LATEST_PK_VERSION)? {
                self.warn("Primary keys missing, try to restore")?;
                let restored = recover(restore_last_pk(self.host, self), |e| {
                    let _ = self.warn(&format!("Could not restore primary keys: {e}"));
                    Ok(false)
                })?;
                if !restored {
                    self.warn("No primary keys found, do a full load")?;
                    return self.full_load(Mode::Append);
                }
            } else {
                let cols: Vec<String> =
                    self.host.column_names(LATEST_PK_VERSION)?.iter().map(|c| c.to_lowercase()).collect();
                let pk_set: Vec<String> = self.pk_targets().iter().map(|c| c.to_lowercase()).collect();
                if !pk_set.iter().all(|p| cols.contains(p)) {
                    self.warn(&format!(
                        "Primary keys do not match. Expected: {}, Found: {}. Do a full load",
                        pk_set.join(", "),
                        cols.join(", ")
                    ))?;
                    return self.full_load(Mode::Append);
                }
            }
        }
        let old_pk_version = if simple { None } else { Some(self.host.version(LATEST_PK_VERSION)?) };
        trace("delta load: checks");
        let source_state = self.source_ts_cnt()?;
        let from_state = if self.spec.shortcut_allowed {
            recover(self.up_to_date_from_state(&source_state.0, source_state.1), |e| {
                self.warn(&format!("Could not check for updates, loading: {e}"))?;
                Ok::<_, Error>(Some(false))
            })?
        } else {
            Some(false)
        };
        trace("no-updates check from the state");
        if from_state == Some(true) {
            self.info("No updates, done", LogArgs::default())?;
            return Ok(LoadResult::NoLoad);
        }
        let (dlv, current_count) = match self.local_delta_value_and_count() {
            Ok(v) => v,
            Err(e) if !e.is_recoverable() => return Err(e),
            Err(e) => {
                self.warn(&format!("Could not get delta value: {e}"))?;
                return self.full_load(Mode::Append);
            }
        };
        trace("local max/count");
        let starting_local_state = (dlv.clone(), current_count);
        let (source_delta, source_count) = source_state;
        let starting_source_state = (source_delta.clone(), source_count);
        if !dlv.is_null()
            && from_state.is_none()
            && recover(self.target_is_up_to_date(&source_delta, source_count), |e| {
                self.warn(&format!("Could not check for updates, loading: {e}"))?;
                Ok::<_, Error>(false)
            })?
        {
            self.info("No updates, done", LogArgs::default())?;
            return Ok(LoadResult::NoLoad);
        }
        trace("shortcut check");
        if dlv.is_null() {
            self.warn("No delta load value, do a full load")?;
            return self.full_load(Mode::Append);
        }
        let pkts = if simple {
            self.info(
                &format!("Start delta step 1, MAX({}): {}. Total RowCount: {source_count}", dc.name, dlv.to_text()),
                LogArgs::default(),
            )?;
            None
        } else {
            self.info(
                &format!("Start delta step 1, get primary keys and timestamps. MAX({}): {}", dc.name, dlv.to_text()),
                LogArgs::default(),
            )?;
            Some(self.retrieve_primary_key_data()?)
        };
        trace("pk snapshot");
        let criterion = self.criterion(">", &dlv)?;
        self.info(
            &format!("Start delta step 2, load updates by {}", dc.name),
            LogArgs { load: Some("delta"), sub_load: Some("delta_1"), ..Default::default() },
        )?;
        let upds_sql = self.spec.sql.updates(&self.all_cols(), &[criterion], &self.pass.valid_from);
        let mut d1 = self.load_updates_to_delta(&upds_sql)?;
        let mut delta_load_value = dlv;
        trace("delta_1");
        let lpk = if let Some(pkts) = pkts {
            let lpk_old = collect(self.host.read(LATEST_PK_VERSION, old_pk_version, Some(&self.pk_ts_targets()))?)?;
            trace("read old pks");
            let (d2, kept) = match self.handle_additional_updates(&pkts, &lpk_old, &mut d1)? {
                Additional::FullLoad(r) => return Ok(r),
                Additional::Reloaded(min_ts, kept) => {
                    if !min_ts.is_null() {
                        delta_load_value = min_ts;
                    }
                    (None, kept)
                }
                Additional::Rows(d2, kept) => (Some(d2), kept),
            };
            trace("additional updates");
            self.info("Start delta step 3.5, write deletes", LogArgs::default())?;
            self.write_deletes(&lpk_old, &kept, &d1, d2.as_ref())?;
            trace("deletes");
            self.info("Start delta step 4, write meta for next delta load", LogArgs::default())?;
            let lpk = self.latest_pks(Some(&pkts), &d1, d2.as_ref(), Some(&delta_load_value))?;
            trace("latest pks");
            self.write_latest_pk(lpk.clone())?;
            trace("write latest pks");
            self.info("Done delta load, do some last checks", LogArgs::default())?;
            lpk
        } else {
            self.write_empty_delta_2(&d1)?;
            if self.host.path_exists(PRIMARY_KEYS_TS)? {
                self.host.remove(PRIMARY_KEYS_TS, true)?;
            }
            let src = self.latest_pks(None, &d1, None, None)?;
            let lpk = if self.host.path_exists(LATEST_PK_VERSION)? {
                // upsert of the changed keys into the last PK version
                let current = collect(self.host.read(LATEST_PK_VERSION, None, Some(&self.pk_ts_targets()))?)?;
                let enc = KeyEncoder::new(&src, &self.pk_targets())?;
                let kept = table::filter(&current, &keys::not_in(&enc, &current, &src)?)?;
                table::union_all(&[src, kept])?
            } else {
                src
            };
            self.write_latest_pk(lpk.clone())?;
            lpk
        };
        self.host.fail_point("local_pk_count")?;
        let target_count = lpk.num_rows() as i64;
        let dirty = source_count != target_count;
        if !dirty {
            self.info(&format!("Source and target count match: {source_count}"), LogArgs::default())?;
            return Ok(LoadResult::Delta {
                starting_local_state,
                starting_source_state,
                end_source_state: None,
                dirty: Some(false),
            });
        }
        self.warn(&format!(
            "Source and target count do not match. Source: {source_count}, Target: {target_count}. {}",
            if simple_check { "Do a normal delta" } else { "" }
        ))?;
        if simple && simple_check {
            return self.with_pass(self.next_pass()?).delta_load(false, false);
        }
        let end = self.source_ts_cnt()?;
        if (comparable(&end.0), end.1) != (comparable(&starting_source_state.0), starting_source_state.1) {
            self.warn(&format!(
                "Source state changed during load: {} -> {}",
                py_state(&starting_source_state),
                py_state(&end)
            ))?;
        }
        Ok(LoadResult::Delta {
            starting_local_state,
            starting_source_state,
            end_source_state: Some(end),
            dirty: Some(true),
        })
    }

    /// The PKs and delta values after this load: the rows read by key (delta_2), the rows read by
    /// delta value (delta_1) and the PK snapshot (up to `delta_load_value`) for all others.
    fn latest_pks(
        &self,
        pkts: Option<&RecordBatch>,
        d1: &RecordBatch,
        d2: Option<&RecordBatch>,
        delta_load_value: Option<&Scalar>,
    ) -> Result<RecordBatch> {
        let names = self.pk_ts_targets();
        let pks = self.pk_targets();
        let d1p = project(d1, &names)?;
        let pkts = pkts.map(|p| project(p, &names)).transpose()?;
        let d2p = d2.map(|d| project(d, &names)).transpose()?;
        let enc = KeyEncoder::new(pkts.as_ref().unwrap_or(&d1p), &pks)?;
        let mut parts = Vec::new();
        match &d2p {
            Some(d2p) => {
                parts.push(d2p.clone());
                parts.push(table::filter(&d1p, &keys::not_in(&enc, &d1p, d2p)?)?);
            }
            None => parts.push(d1p.clone()),
        }
        if let Some(pk) = pkts {
            let mut mask = keys::not_in(&enc, &pk, &d1p)?;
            if let Some(d2p) = &d2p {
                mask = table::and(&mask, &keys::not_in(&enc, &pk, d2p)?)?;
            }
            if let Some(v) = delta_load_value.filter(|v| !v.is_null()) {
                let ts = pk.column(names.len() - 1);
                let lit = scalar_array(v, ts.data_type())?;
                let le = arrow_ord::cmp::lt_eq(ts, &arrow_array::Scalar::new(lit))?;
                let le: BooleanArray = le.iter().map(|b| Some(b == Some(true))).collect();
                mask = table::and(&mask, &le)?;
            }
            parts.push(table::filter(&pk, &mask)?);
        }
        table::union_all(&parts)
    }

    /// Marks the keys of the last load that are neither in the snapshot (`kept`) nor read again
    /// (delta_1, delta_2) as deleted.
    fn write_deletes(
        &self,
        lpk_old: &RecordBatch,
        kept: &[bool],
        d1: &RecordBatch,
        d2: Option<&RecordBatch>,
    ) -> Result<()> {
        let pks = self.pk_targets();
        let mut gone: BooleanArray = kept.iter().map(|k| Some(!k)).collect();
        if gone.true_count() > 0 {
            let enc = KeyEncoder::new(lpk_old, &pks)?;
            gone = table::and(&gone, &keys::not_in(&enc, lpk_old, d1)?)?;
            if let Some(d2) = d2 {
                gone = table::and(&gone, &keys::not_in(&enc, lpk_old, d2)?)?;
            }
        }
        let deleted = table::filter(&project(lpk_old, &pks)?, &gone)?;
        if deleted.num_rows() == 0 {
            return Ok(());
        }
        let markers = delete_markers(&d1.schema(), &deleted, &pks, self.pass.micros)?;
        self.host.write(DELTA, nullable(table::single(markers)), Mode::Append, self.spec.drift)
    }

    fn handle_additional_updates(
        &self,
        pkts: &RecordBatch,
        lpk_old: &RecordBatch,
        d1: &mut RecordBatch,
    ) -> Result<Additional> {
        let names = self.pk_ts_targets();
        let compute = || -> Result<(RecordBatch, Vec<bool>)> {
            self.host.fail_point("additional_updates")?;
            let pk = project(pkts, &names)?;
            let diff = keys::diff_snapshot(&pk, lpk_old, &self.pk_targets(), &self.delta_col()?.target)?;
            Ok((table::filter(&pk, &diff.changed)?, diff.kept))
        };
        let (additional, kept) = match compute() {
            Ok(a) => a,
            Err(e) if !e.is_recoverable() => return Err(e),
            Err(e) => {
                self.warn(&format!("Could not create view for additional updates: {e}"))?;
                let restored = recover(restore_last_pk(self.host, self), |e| {
                    let _ = self.warn(&format!("Could not restore primary keys: {e}"));
                    Ok(false)
                })?;
                if !restored {
                    self.warn("No primary keys found, do a full load")?;
                    return Ok(Additional::FullLoad(self.with_pass(self.next_pass()?).full_load(Mode::Append)?));
                }
                compute()?
            }
        };
        let pks = self.pk_targets();
        let enc = KeyEncoder::new(&additional, &pks)?;
        let real = project(&table::filter(&additional, &keys::except(&enc, &additional, d1)?)?, &pks)?;
        let update_count = real.num_rows();
        if update_count == 0 {
            self.write_empty_delta_2(d1)?;
            return Ok(Additional::Rows(empty_like(d1), kept));
        }
        if update_count > 1000 || self.spec.no_complex_entries_load {
            self.write_empty_delta_2(d1)?;
            self.warn(&format!("Start delta step 3, load {update_count} strange updates via normal delta load"))?;
            let dc_target = self.delta_col()?.target.clone();
            let ts = additional.column(table::index_of(&additional.schema(), &dc_target)?);
            let min_ts = match keys::argmin(ts)? {
                Some(i) => scalar_at(ts, i)?,
                None => Scalar::Null,
            };
            let criterion = self.criterion(">=", &min_ts)?;
            self.info("Start delta step 2, load updates by timestamp", LogArgs::default())?;
            let pass = self.next_pass()?; // re-reads rows of delta_1
            let sql = self.spec.sql.updates(&self.all_cols(), &[criterion], &pass.valid_from);
            self.info(
                "execute sql",
                LogArgs { sql: Some(&sql), load: Some("delta"), sub_load: Some("delta_1_additional") },
            )?;
            *d1 = self.load_updates_to_delta(&sql)?;
            return Ok(Additional::Reloaded(min_ts, kept));
        }
        // keys in batches, so a statement stays below 8000 characters (Spark)
        let char_size: usize =
            self.spec.pks.iter().map(|i| 5 + if self.col(*i).is_numeric { 10 } else { 40 }).sum::<usize>().max(1);
        let batch_size = (7000 / char_size).max(10);
        self.warn(&format!(
            "Start delta step 3, load {update_count} strange updates via batches of size {batch_size}"
        ))?;
        let rows: Vec<String> = (0..update_count)
            .map(|r| {
                let fields = (0..pks.len())
                    .map(|c| Ok(format!("\"p{c}\": {}", scalar_at(real.column(c), r)?.to_json())))
                    .collect::<Result<Vec<_>>>()?;
                Ok(format!("{{{}}}", fields.join(", ")))
            })
            .collect::<Result<_>>()?;
        let mut d2_parts = Vec::new();
        for (i, chunk) in rows.chunks(batch_size).enumerate() {
            let mode = if i == 0 { Mode::Overwrite } else { Mode::Append };
            d2_parts.extend(self.write_delta_2(chunk, mode)?);
        }
        let d2 = if d2_parts.iter().any(Option::is_none) {
            // copied by the host chunk by chunk: the staging table holds all of them
            collect(self.host.read(DELTA_2, None, None)?)?
        } else if d2_parts.is_empty() {
            empty_like(d1)
        } else {
            table::union_all(&d2_parts.into_iter().flatten().collect::<Vec<_>>())?
        };
        if d2.num_rows() > 0 {
            self.host.write(DELTA, nullable(table::single(d2.clone())), Mode::Append, Drift::Yes)?;
        }
        Ok(Additional::Rows(d2, kept))
    }

    fn rows_by_pk_sql(&self, json: &str) -> String {
        let pks: Vec<&Column> = self.spec.pks.iter().map(|i| self.col(*i)).collect();
        self.spec.sql.rows_by_pk_json(&self.all_cols(), &pks, json, &self.pass.valid_from)
    }

    /// Reads the rows of the given keys (JSON objects) into `delta_load/delta_2`; the rows read,
    /// or `None` if the host copied them itself (then only the staging table has them).
    fn write_delta_2(&self, rows: &[String], mode: Mode) -> Result<Vec<Option<RecordBatch>>> {
        self.host.fail_point("write_delta2")?;
        if mode == Mode::Overwrite {
            let sql = self.rows_by_pk_sql(&format!("[/* {} entries */]", rows.len()));
            self.info(
                "execute sql",
                LogArgs { sql: Some(&sql), load: Some("delta"), sub_load: Some("delta_additional") },
            )?;
        }
        let sql = self.rows_by_pk_sql(&format!("[{}]", rows.join(", ")));
        if sql.len() > 7000 && rows.len() > 1 {
            let (a, b) = rows.split_at(rows.len() / 2);
            let first = self.source_to_delta_teed(
                &self.rows_by_pk_sql(&format!("[{}]", a.join(", "))),
                DELTA_2,
                mode,
                Drift::Yes,
                None,
            )?;
            let second = self.source_to_delta_teed(
                &self.rows_by_pk_sql(&format!("[{}]", b.join(", "))),
                DELTA_2,
                Mode::Append,
                self.spec.drift,
                None,
            )?;
            return Ok(vec![first, second]);
        }
        Ok(vec![self.source_to_delta_teed(&sql, DELTA_2, mode, Drift::Yes, None)?])
    }

    /// `delta_load/delta_2` without rows (it has the columns of `delta_1`).
    fn write_empty_delta_2(&self, d1: &RecordBatch) -> Result<()> {
        self.host.fail_point("write_delta2")?;
        let sql = self.rows_by_pk_sql("[/* 0 entries */]");
        self.info("execute sql", LogArgs { sql: Some(&sql), load: Some("delta"), sub_load: Some("delta_additional") })?;
        self.host.write(
            DELTA_2,
            nullable(table::empty(table::nullable_schema(&d1.schema()))),
            Mode::Overwrite,
            Drift::Yes,
        )
    }
}

enum Additional {
    /// Fell back to a full load, which wrote the PKs.
    FullLoad(LoadResult),
    /// Many strange updates: delta_1 was re-read from this delta value on. With the
    /// `kept` marks of the last load's keys (see `keys::SnapshotDiff`).
    Reloaded(Scalar, Vec<bool>),
    /// The rows read by key (delta_2), with the `kept` marks.
    Rows(RecordBatch, Vec<bool>),
}

fn empty_like(b: &RecordBatch) -> RecordBatch {
    b.slice(0, 0)
}

fn py_list(items: &[&str]) -> String {
    format!("[{}]", items.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(", "))
}

fn py_state(s: &State) -> String {
    format!("({}, {})", s.0.to_text(), s.1)
}

fn max_of(col: &ArrayRef) -> Result<Scalar> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    // integers and timestamps: a vectorized max instead of the row-format scan
    let as_i64 = |c: &ArrayRef| -> Result<Option<i64>> {
        let c = arrow_cast::cast(c, &DataType::Int64)?;
        Ok(arrow_arith::aggregate::max(c.as_primitive::<Int64Type>()))
    };
    match col.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => {
            return Ok(as_i64(col)?.map_or(Scalar::Null, Scalar::Int));
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            return Ok(as_i64(col)?.map_or(Scalar::Null, |micros| Scalar::Timestamp { micros, utc: tz.is_some() }));
        }
        _ => {}
    }
    match keys::argmax(col)? {
        Some(i) => scalar_at(col, i),
        None => Ok(Scalar::Null),
    }
}

/// A one-element array of `value` with type `t` (for comparisons with a column).
fn scalar_array(value: &Scalar, t: &DataType) -> Result<ArrayRef> {
    let arr: ArrayRef = match value {
        Scalar::Int(v) => Arc::new(Int64Array::from(vec![*v])),
        Scalar::Str(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        Scalar::Bool(b) => Arc::new(BooleanArray::from(vec![*b])),
        Scalar::Float(f) => Arc::new(arrow_array::Float64Array::from(vec![*f])),
        Scalar::Timestamp { micros, utc } => {
            let a = arrow_array::TimestampMicrosecondArray::from(vec![*micros]);
            Arc::new(if *utc { a.with_timezone("UTC") } else { a })
        }
        Scalar::Date(d) => Arc::new(arrow_array::Date32Array::from(vec![*d])),
        Scalar::Decimal { value, scale } => Arc::new(
            arrow_array::Decimal128Array::from(vec![*value])
                .with_precision_and_scale(38, *scale)
                .map_err(Error::from)?,
        ),
        Scalar::Bytes(b) => Arc::new(arrow_array::BinaryArray::from(vec![b.as_slice()])),
        Scalar::Null => new_null_array(t, 1),
    };
    Ok(if arr.data_type() == t { arr } else { crate::normalize::strict_cast(&arr, t)? })
}

/// Rows marking deleted keys: the key columns, NULL in all others, `__timestamp` the pass's
/// source timestamp (`micros`, UTC; the local clock without one), `__is_deleted` true,
/// `__is_full_load` false; with the columns of the staging table `schema`.
fn delete_markers(
    schema: &SchemaRef,
    deleted: &RecordBatch,
    pks: &[String],
    micros: Option<i64>,
) -> Result<RecordBatch> {
    let n = deleted.num_rows();
    let now = micros.unwrap_or_else(now_micros);
    let schema = table::nullable_schema(schema);
    let cols = schema
        .fields()
        .iter()
        .map(|f| -> Result<ArrayRef> {
            let name = f.name();
            if let Some(p) = pks.iter().position(|p| p.eq_ignore_ascii_case(name)) {
                let c = deleted.column(table::index_of(&deleted.schema(), &pks[p])?);
                return Ok(if c.data_type() == f.data_type() {
                    c.clone()
                } else {
                    crate::normalize::strict_cast(c, f.data_type())?
                });
            }
            if name == VALID_FROM_COL_NAME {
                let tz = match f.data_type() {
                    DataType::Timestamp(_, tz) => tz.clone(),
                    _ => None,
                };
                let ts: ArrayRef =
                    Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![now; n]).with_timezone_opt(tz.clone()));
                let target = match f.data_type() {
                    DataType::Timestamp(..) => f.data_type().clone(),
                    _ => DataType::Timestamp(TimeUnit::Microsecond, None),
                };
                return Ok(arrow_cast::cast(&ts, &target)?);
            }
            if name == IS_DELETED_COL_NAME || name == IS_FULL_LOAD_COL_NAME {
                let b: ArrayRef = Arc::new(BooleanArray::from(vec![name == IS_DELETED_COL_NAME; n]));
                return Ok(if f.data_type() == &DataType::Boolean { b } else { arrow_cast::cast(&b, f.data_type())? });
            }
            Ok(new_null_array(f.data_type(), n))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, cols)?)
}

/// Rebuilds `latest_pk_version` from the Delta table: the last full load plus the newest row
/// per key written after it, without deleted keys. False if there is nothing to restore.
fn restore_last_pk<H: Host>(host: &H, load: &Load<'_, H>) -> Result<bool> {
    if host.fail_point("restore_last_pk")? == Some(false) {
        return Ok(false);
    }
    let rows = last_pk_rows(host, &load.pk_targets(), &load.delta_col()?.target)?;
    load.info("Restoring last pk version", LogArgs::default())?;
    let Some(rows) = rows.filter(|r| r.num_rows() > 0) else { return Ok(false) };
    load.write_latest_pk(rows)?;
    Ok(true)
}

/// The PKs and delta values `latest_pk_version` should hold according to the Delta table.
pub fn last_pk_rows<H: Host>(host: &H, pks: &[String], delta_target: &str) -> Result<Option<RecordBatch>> {
    let mut names: Vec<String> = pks.to_vec();
    names.push(delta_target.to_string());
    let mut cols = names.clone();
    cols.extend([VALID_FROM_COL_NAME.into(), IS_DELETED_COL_NAME.into(), IS_FULL_LOAD_COL_NAME.into()]);
    let all = collect(host.read(DELTA, None, Some(&cols))?)?;
    let n = names.len();
    let (ts, full) = (all.column(n).clone(), all.column(n + 2).clone());
    let full_mask = table::true_mask(&full)?;
    let full_rows = table::filter(&all, &full_mask)?;
    let Some(i) = keys::argmax(full_rows.column(n))? else { return Ok(None) };
    let latest_full = arrow_array::Scalar::new(full_rows.column(n).slice(i, 1));
    let in_last_full = table::and(&full_mask, &arrow_ord::cmp::eq(&ts, &latest_full)?)?;
    let last_full = project(&table::filter(&all, &in_last_full)?, &names)?;
    let after = table::filter(&all, &arrow_ord::cmp::gt(&ts, &latest_full)?)?;
    let enc = KeyEncoder::new(&last_full, pks)?;
    let newest = keys::latest_per_key(&enc, &after, &[after.column(n).clone(), after.column(n - 1).clone()])?;
    let newest = arrow_select::take::take_record_batch(
        &after,
        &arrow_array::UInt64Array::from(newest.iter().map(|i| *i as u64).collect::<Vec<_>>()),
    )?;
    let alive = project(&table::filter(&newest, &negate(&table::true_mask(newest.column(n + 1))?))?, &names)?;
    let untouched = table::filter(&last_full, &keys::not_in(&enc, &last_full, &newest)?)?;
    Ok(Some(table::union_all(&[alive, untouched])?))
}

fn negate(m: &BooleanArray) -> BooleanArray {
    m.iter().map(|v| Some(v != Some(true))).collect()
}

/// Runs one load of `spec` against `host`.
pub fn execute<H: Host>(host: &H, spec: &LoadSpec) -> Result<LoadResult> {
    // before the versions to restore on failure are read: another load may still change them
    let token = host.acquire_lock(LOCK_FILE)?;
    let result = execute_locked(host, spec);
    let released = host.release_lock(LOCK_FILE, &token);
    let result = result?;
    released?;
    Ok(result)
}

fn execute_locked<H: Host>(host: &H, spec: &LoadSpec) -> Result<LoadResult> {
    let mut load = Load {
        host,
        spec,
        pass: Pass { valid_from: ValidFrom::SourceClock, micros: None },
        delta_col: spec.delta_col,
        written: Default::default(),
        prefetched: Mutex::new(None),
    };
    let last_version_pk = if host.table_exists(LATEST_PK_VERSION)? {
        match host.version(LATEST_PK_VERSION) {
            Ok(v) => Some(v),
            Err(e) if !e.is_recoverable() => return Err(e),
            Err(e) => {
                load.warn(&format!("Could not get last version: {e}"))?;
                None
            }
        }
    } else {
        None
    };
    let last_version_pk_ts =
        match host.table_exists(PRIMARY_KEYS_TS).and_then(|e| e.then(|| host.version(PRIMARY_KEYS_TS)).transpose()) {
            Ok(v) => v,
            Err(e) if !e.is_recoverable() => return Err(e),
            Err(e) => {
                load.warn(&format!("Could not get version of {PRIMARY_KEYS_TS}: {e}"))?;
                None
            }
        };
    let run = |load: &mut Load<'_, H>| -> Result<LoadResult> {
        trace("load: start");
        let delta_load_ahead = spec.sql.dialect == Dialect::Tsql
            && matches!(spec.load_mode, LoadMode::Append | LoadMode::SimpleDelta | LoadMode::SimpleDeltaCheck)
            && load.delta_col.is_some()
            && !spec.pks.is_empty()
            && host.table_exists(DELTA)?;
        let prefetched = if delta_load_ahead { load.prefetch_source_state()? } else { None };
        let watermark = match prefetched {
            Some(watermark) => watermark,
            None => {
                load.pass = load.first_pass()?;
                load.fetch_watermark()?
            }
        };
        trace("load: clock and watermark");
        let result = if !host.table_exists(DELTA)? || spec.load_mode == LoadMode::Overwrite {
            host.mkdir(DELTA)?;
            load.full_load(Mode::Overwrite)?
        } else if spec.load_mode == LoadMode::AppendInserts {
            if load.delta_col.is_none() && spec.pks.len() == 1 && spec.cols[spec.pks[0]].is_identity {
                load.delta_col = Some(spec.pks[0]); // identity columns are usually increasing
            }
            if load.delta_col.is_none() {
                return Err(Error::msg("Must provide delta column for append_inserts load"));
            }
            load.append_inserts()?
        } else if load.delta_col.is_none() || spec.pks.is_empty() || spec.load_mode == LoadMode::ForceFull {
            load.full_load(Mode::Append)?
        } else {
            load.delta_load(
                matches!(spec.load_mode, LoadMode::SimpleDelta | LoadMode::SimpleDeltaCheck),
                spec.load_mode == LoadMode::SimpleDeltaCheck,
            )?
        };
        trace("load: done");
        if result != LoadResult::NoLoad {
            load.write_last_load_state(watermark)?;
        }
        trace("load: state written");
        for rel in [LATEST_PK_VERSION, DELTA_1, DELTA_2, PRIMARY_KEYS_TS, LAST_LOAD_STATE] {
            if host.table_exists(rel)? {
                host.vacuum(rel)?;
            }
        }
        trace("load: vacuum");
        Ok(result)
    };
    let mut result = run(&mut load);
    if let Err(e) = &result {
        // restore the state files to the last successful load
        let mut not_restored = Vec::new();
        if let Some(v) = last_version_pk {
            let restored = host
                .version(LATEST_PK_VERSION)
                .and_then(|cur| if cur > v { host.restore(LATEST_PK_VERSION, v) } else { Ok(()) });
            if let Err(re) = restored {
                not_restored.push(format!("{LATEST_PK_VERSION} to version {v}: {re}"));
            }
        }
        // primary_keys_ts holds the source snapshot read by this failed load
        let restore_pk_ts = || -> Result<()> {
            match last_version_pk_ts {
                Some(v) => {
                    if host.version(PRIMARY_KEYS_TS)? > v {
                        host.restore(PRIMARY_KEYS_TS, v)?;
                    }
                }
                None => {
                    if host.path_exists(PRIMARY_KEYS_TS)? {
                        host.remove(PRIMARY_KEYS_TS, true)?;
                    }
                }
            }
            Ok(())
        };
        if let Err(restore_error) = restore_pk_ts() {
            let _ = load.warn(&format!("Could not restore {PRIMARY_KEYS_TS}, removing it: {restore_error}"));
            let removed = host
                .path_exists(PRIMARY_KEYS_TS)
                .and_then(|exists| if exists { host.remove(PRIMARY_KEYS_TS, true) } else { Ok(()) });
            if let Err(re) = removed {
                not_restored.push(format!("{PRIMARY_KEYS_TS} (restore: {restore_error}; removal: {re})"));
            }
        }
        let _ = host.log(Level::Error, &format!("Error during load: {e}"), LogArgs::default());
        if !not_restored.is_empty() {
            let restore = not_restored.join("; ");
            let _ = host.log(Level::Error, &format!("Could not restore the load state: {restore}"), LogArgs::default());
            if let Err(e) = result {
                result = Err(Error::NotRestored { load: Box::new(e), restore });
            }
        }
    }
    result
}

/// `latest_pk_version` rebuilt from the Delta table (public for `restore_last_pk` callers).
pub fn restore_latest_pk<H: Host>(host: &H, spec: &LoadSpec) -> Result<bool> {
    let load = Load {
        host,
        spec,
        pass: Pass { valid_from: ValidFrom::SourceClock, micros: None },
        delta_col: spec.delta_col,
        written: Default::default(),
        prefetched: Mutex::new(None),
    };
    restore_last_pk(host, &load)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{BooleanArray, Int64Array, RecordBatch, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn pass_timestamps_increase_with_frozen_or_backward_clocks() {
        assert_eq!(pass_micros(100, None), 100);
        assert_eq!(pass_micros(100, Some(50)), 100);
        assert_eq!(pass_micros(100, Some(100)), 101); // frozen clock
        assert_eq!(pass_micros(90, Some(100)), 101); // clock went back
    }

    /// The Delta tables of a target folder in memory (each write is a version); no source.
    #[derive(Default)]
    struct MemLake {
        tables: Mutex<HashMap<String, Vec<RecordBatch>>>,
    }

    impl MemLake {
        fn put(&self, rel: &str, b: RecordBatch) {
            self.tables.lock().unwrap().entry(rel.into()).or_default().push(b);
        }
    }

    impl Host for MemLake {
        fn source_query(&self, _: &str) -> Result<Batches> {
            Err(Error::msg("no source"))
        }
        fn source_stream(&self, _: &str, _: Option<&str>) -> Result<Option<Batches>> {
            Err(Error::msg("no source"))
        }
        fn source_copy(&self, _: &str, _: &str, _: Mode, _: Drift) -> Result<()> {
            Err(Error::msg("no source"))
        }
        fn table_exists(&self, rel: &str) -> Result<bool> {
            Ok(self.tables.lock().unwrap().contains_key(rel))
        }
        fn path_exists(&self, rel: &str) -> Result<bool> {
            self.table_exists(rel)
        }
        fn version(&self, rel: &str) -> Result<i64> {
            Ok(self.tables.lock().unwrap()[rel].len() as i64 - 1)
        }
        fn read(&self, rel: &str, version: Option<i64>, columns: Option<&[String]>) -> Result<Batches> {
            let t = self.tables.lock().unwrap();
            let versions = &t[rel];
            let b = versions[version.map_or(versions.len() - 1, |v| v as usize)].clone();
            Ok(table::single(match columns {
                Some(c) => project(&b, c)?,
                None => b,
            }))
        }
        fn column_names(&self, rel: &str) -> Result<Vec<String>> {
            Ok(self.tables.lock().unwrap()[rel]
                .last()
                .unwrap()
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect())
        }
        fn write(&self, rel: &str, data: Batches, mode: Mode, _: Drift) -> Result<()> {
            let new = collect(data)?;
            let b = match (mode, self.tables.lock().unwrap().get(rel).and_then(|v| v.last().cloned())) {
                (Mode::Append, Some(old)) => table::union_all(&[old, new])?,
                _ => new,
            };
            self.put(rel, b);
            Ok(())
        }
        fn restore(&self, rel: &str, version: i64) -> Result<()> {
            let b = self.tables.lock().unwrap()[rel][version as usize].clone();
            self.put(rel, b);
            Ok(())
        }
        fn remove(&self, rel: &str, _: bool) -> Result<()> {
            self.tables.lock().unwrap().remove(rel);
            Ok(())
        }
        fn vacuum(&self, _: &str) -> Result<()> {
            Ok(())
        }
        fn mkdir(&self, _: &str) -> Result<()> {
            Ok(())
        }
        fn acquire_lock(&self, _: &str) -> Result<String> {
            Ok("token".into())
        }
        fn release_lock(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        fn log(&self, _: Level, _: &str, _: LogArgs<'_>) -> Result<()> {
            Ok(())
        }
        fn fail_point(&self, _: &str) -> Result<Option<bool>> {
            Ok(None)
        }
    }

    fn history(rows: &[(i64, i64, i64, bool, bool)]) -> RecordBatch {
        // (id, rv, __timestamp, __is_deleted, __is_full_load)
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("rv", DataType::Int64, true),
                Field::new(VALID_FROM_COL_NAME, DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Field::new(IS_DELETED_COL_NAME, DataType::Boolean, true),
                Field::new(IS_FULL_LOAD_COL_NAME, DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
                Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
                Arc::new(BooleanArray::from(rows.iter().map(|r| r.3).collect::<Vec<_>>())),
                Arc::new(BooleanArray::from(rows.iter().map(|r| r.4).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    }

    fn pairs(b: &RecordBatch) -> Vec<(i64, i64)> {
        let mut v: Vec<(i64, i64)> = (0..b.num_rows())
            .map(|i| {
                (b.column(0).as_primitive::<Int64Type>().value(i), b.column(1).as_primitive::<Int64Type>().value(i))
            })
            .collect();
        v.sort();
        v
    }

    #[test]
    fn last_pk_rows_takes_last_full_load_and_newer_rows() {
        let lake = MemLake::default();
        lake.put(
            DELTA,
            history(&[
                (1, 10, 100, false, true), // an older full load
                (9, 90, 100, false, true),
                (1, 11, 200, false, true), // the last full load: 1, 2, 3
                (2, 20, 200, false, true),
                (3, 30, 200, false, true),
                (2, 21, 300, false, false), // updated
                (3, 30, 300, true, false),  // deleted
                (4, 40, 300, false, false), // inserted
                (2, 22, 400, false, false), // updated again
            ]),
        );
        let rows = last_pk_rows(&lake, &["id".into()], "rv").unwrap().unwrap();
        assert_eq!(pairs(&rows), vec![(1, 11), (2, 22), (4, 40)]);
    }

    #[test]
    fn last_pk_rows_without_full_load_is_none() {
        let lake = MemLake::default();
        lake.put(DELTA, history(&[(1, 10, 100, false, false)]));
        assert!(last_pk_rows(&lake, &["id".into()], "rv").unwrap().is_none());
    }

    #[test]
    fn delete_markers_have_the_staging_columns() {
        let staging = history(&[]).schema();
        let deleted = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
            vec![Arc::new(arrow_array::Int32Array::from(vec![5, 6]))],
        )
        .unwrap();
        let m = delete_markers(&staging, &deleted, &["id".into()], Some(1_700_000_000_000_000)).unwrap();
        assert_eq!(m.num_rows(), 2);
        // the pass's source timestamp, not the clock of this machine
        let ts = arrow_cast::cast(m.column(2), &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        assert!(
            ts.as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                .iter()
                .all(|v| v == Some(1_700_000_000_000_000))
        );
        assert_eq!(m.column(0).as_primitive::<Int64Type>().values().to_vec(), vec![5, 6]);
        assert_eq!(m.column(1).null_count(), 2);
        assert!(m.column(3).as_boolean().iter().all(|v| v == Some(true)));
        assert!(m.column(4).as_boolean().iter().all(|v| v == Some(false)));
        assert_eq!(m.column(2).null_count(), 0);
    }

    #[test]
    fn max_of_and_comparable() {
        let c: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(7)]));
        assert_eq!(max_of(&c).unwrap(), Scalar::Int(7));
        let c: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![Some("a"), Some("c"), None]));
        assert_eq!(max_of(&c).unwrap(), Scalar::Str("c".into()));
        assert_eq!(comparable(&Scalar::Bytes(vec![0, 0, 0, 0, 0, 0, 1, 0])), Scalar::Int(256));
    }
}
