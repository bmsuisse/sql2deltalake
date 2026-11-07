//! The source SQL builder for Python (parity tests, readers).

use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use sql2deltalake_core::sqlgen::{self, Column, Dialect, MappedType, SourceRef, SourceSql, TypeMap, ValidFrom};

use crate::scalar::to_scalar;

pub fn dialect(name: &str) -> PyResult<Dialect> {
    Dialect::parse(name).ok_or_else(|| PyValueError::new_err(format!("dialect {name} has no native load path")))
}

/// A column from the dict of `sql2deltalake._native.column_spec`.
pub fn column(d: &Bound<'_, PyAny>) -> PyResult<Column> {
    let s = |k: &str| -> PyResult<String> { d.get_item(k)?.extract() };
    let b = |k: &str| -> PyResult<bool> { d.get_item(k)?.extract() };
    Ok(Column {
        name: s("name")?,
        target: s("target")?,
        type_key: s("type_key")?,
        type_sql: s("type_sql")?,
        orig_type: s("orig_type")?,
        is_string: b("is_string")?,
        is_numeric: b("is_numeric")?,
        is_identity: b("is_identity")?,
    })
}

pub fn source(v: &Bound<'_, PyAny>) -> PyResult<SourceRef> {
    if let Ok(q) = v.extract::<String>() {
        return Ok(SourceRef::Query(q));
    }
    Ok(SourceRef::Table(v.extract()?))
}

pub fn type_map(m: HashMap<String, (String, bool)>) -> TypeMap {
    m.into_iter().map(|(k, (sql, is_string))| (k, MappedType { sql, is_string })).collect()
}

fn valid_from(v: Option<String>) -> ValidFrom {
    v.map_or(ValidFrom::SourceClock, ValidFrom::Literal)
}

#[pyclass(module = "sql2deltalake._core", frozen)]
pub struct SqlBuilder {
    sql: SourceSql,
    cols: Vec<Column>,
}

impl SqlBuilder {
    fn pick(&self, idx: &[usize]) -> PyResult<Vec<&Column>> {
        idx.iter()
            .map(|i| self.cols.get(*i).ok_or_else(|| PyValueError::new_err("column index out of range")))
            .collect()
    }

    fn all(&self) -> Vec<&Column> {
        self.cols.iter().collect()
    }
}

#[pymethods]
impl SqlBuilder {
    #[new]
    fn new(
        dialect_name: &str,
        source_ref: &Bound<'_, PyAny>,
        type_map_spec: HashMap<String, (String, bool)>,
        no_trim: bool,
        columns: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        Ok(SqlBuilder {
            sql: SourceSql {
                dialect: dialect(dialect_name)?,
                source: source(source_ref)?,
                type_map: type_map(type_map_spec),
                no_trim,
            },
            cols: columns.iter().map(column).collect::<PyResult<_>>()?,
        })
    }

    /// `valid_from`: a literal from `load_timestamp_literal`, None for the source clock.
    #[pyo3(signature = (valid_from=None))]
    fn full_load(&self, valid_from: Option<String>) -> String {
        self.sql.full_load(&self.all(), &self::valid_from(valid_from))
    }

    fn pk_ts(&self, columns: Vec<usize>) -> PyResult<String> {
        Ok(self.sql.pk_ts(&self.pick(&columns)?))
    }

    #[pyo3(signature = (criteria, valid_from=None))]
    fn updates(&self, criteria: Vec<String>, valid_from: Option<String>) -> String {
        self.sql.updates(&self.all(), &criteria, &self::valid_from(valid_from))
    }

    fn criterion(&self, column: usize, op: &str, value: &Bound<'_, PyAny>) -> PyResult<String> {
        Ok(self.sql.criterion(self.pick(&[column])?[0], op, &to_scalar(value)?))
    }

    #[pyo3(signature = (column=None))]
    fn source_ts_cnt(&self, column: Option<usize>) -> PyResult<String> {
        let c = column.map(|c| self.pick(&[c])).transpose()?;
        Ok(self.sql.source_ts_cnt(c.as_ref().map(|c| c[0])))
    }

    #[pyo3(signature = (pks, json, valid_from=None))]
    fn rows_by_pk_json(&self, pks: Vec<usize>, json: &str, valid_from: Option<String>) -> PyResult<String> {
        Ok(self.sql.rows_by_pk_json(&self.all(), &self.pick(&pks)?, json, &self::valid_from(valid_from)))
    }
}

#[pyfunction]
pub fn load_timestamp_literal(dialect_name: &str, micros: i64) -> PyResult<String> {
    Ok(sqlgen::load_timestamp_literal(dialect(dialect_name)?, micros))
}

#[pyfunction]
pub fn compat_name(name: &str) -> String {
    sqlgen::compat_name(name)
}
