//! The SQL sent to the source: column selects with type conversion and trimming, the system
//! columns, delta criteria, the PK/delta-value snapshot and the `OPENJSON` read of single rows.
//!
//! The text matches what the Python implementation produced with sqlglot for T-SQL and Postgres
//! (golden tests in `tests/sqlgen_golden.rs`), so loads keep sending the same statements.

use std::collections::HashMap;
use std::fmt::Write;

use crate::scalar::Scalar;

pub const VALID_FROM_COL_NAME: &str = "__timestamp";
pub const IS_DELETED_COL_NAME: &str = "__is_deleted";
pub const IS_FULL_LOAD_COL_NAME: &str = "__is_full_load";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Tsql,
    Postgres,
}

impl Dialect {
    /// sqlglot dialect names; `None` for dialects without a native load path.
    pub fn parse(name: &str) -> Option<Dialect> {
        match name.to_ascii_lowercase().as_str() {
            "tsql" | "mssql" => Some(Dialect::Tsql),
            "postgres" | "postgresql" => Some(Dialect::Postgres),
            _ => None,
        }
    }

    pub fn quote(self, name: &str) -> String {
        match self {
            Dialect::Tsql => format!("[{}]", name.replace(']', "]]")),
            Dialect::Postgres => format!("\"{}\"", name.replace('"', "\"\"")),
        }
    }
}

/// A source column as the load sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    /// Name in the source.
    pub name: String,
    /// Name in the Delta table (`WriteConfig.get_target_name`).
    pub target: String,
    /// Lower-case base type in the source dialect, the key of the type map (`datetime2`, `int`).
    pub type_key: String,
    /// The full type in the source dialect (`VARCHAR(100)`), for `OPENJSON ... WITH`.
    pub type_sql: String,
    /// The type as the source catalog reports it (`data_type_str`): decides trimming and `xid`.
    pub orig_type: String,
    pub is_string: bool,
    /// Numeric or boolean: estimates the JSON size of a key.
    pub is_numeric: bool,
    pub is_identity: bool,
}

/// The target of a type-map entry, rendered in the source dialect.
#[derive(Clone, Debug, PartialEq)]
pub struct MappedType {
    pub sql: String,
    pub is_string: bool,
}

pub type TypeMap = HashMap<String, MappedType>;

/// What the load reads: a table (1 to 3 name parts) or a query rendered in the source dialect.
#[derive(Clone, Debug, PartialEq)]
pub enum SourceRef {
    Table(Vec<String>),
    Query(String),
}

/// The `__timestamp` column of a source read.
#[derive(Clone, Debug, PartialEq)]
pub enum ValidFrom {
    /// The source clock, evaluated per query (`GETUTCDATE()`).
    SourceClock,
    /// A timestamp read once per load pass, see [`load_timestamp_literal`].
    Literal(String),
}

/// What all source selects of one load have in common.
#[derive(Clone, Debug)]
pub struct SourceSql {
    pub dialect: Dialect,
    pub source: SourceRef,
    pub type_map: TypeMap,
    pub no_trim: bool,
}

const NO_TRIM_TYPES: [&str; 5] = ["uuid", "uniqueidentifier", "guid", "oid", "xid"];

impl SourceSql {
    pub fn column_ref(&self, name: &str, table_alias: Option<&str>) -> String {
        match table_alias {
            Some(alias) => format!("{}.{}", self.dialect.quote(alias), self.dialect.quote(name)),
            None => self.dialect.quote(name),
        }
    }

    fn text_type(&self) -> &'static str {
        match self.dialect {
            Dialect::Tsql => "VARCHAR(MAX)",
            Dialect::Postgres => "TEXT",
        }
    }

    /// The source expression of a column: type map cast, `xid` to bigint, `TRIM` of strings.
    pub fn convert(&self, col: &Column, table_alias: Option<&str>) -> String {
        let mut expr = self.column_ref(&col.name, table_alias);
        let mapped = self.type_map.get(&col.type_key);
        if mapped.is_none() && (col.orig_type == "xid" || col.type_key == "xid") {
            return format!("CAST(CAST({expr} AS {}) AS BIGINT)", self.text_type());
        }
        if let Some(mapped) = mapped {
            expr = format!("CAST({expr} AS {})", mapped.sql);
        }
        let is_string = mapped.map_or(col.is_string, |m| m.is_string);
        if !col.orig_type.is_empty()
            && !NO_TRIM_TYPES.contains(&col.orig_type.to_lowercase().as_str())
            && is_string
            && !self.no_trim
        {
            expr = format!("TRIM({expr})");
        }
        expr
    }

    fn bool_literal(&self, value: bool) -> &'static str {
        match (self.dialect, value) {
            (Dialect::Tsql, false) => "CAST(0 AS BIT)",
            (Dialect::Tsql, true) => "CAST(1 AS BIT)",
            (Dialect::Postgres, false) => "FALSE",
            (Dialect::Postgres, true) => "TRUE",
        }
    }

    /// The select list of a source read: converted columns aliased to their target names,
    /// then `__timestamp`, `__is_deleted` and `__is_full_load` if asked for.
    pub fn select_list(
        &self,
        cols: &[&Column],
        table_alias: Option<&str>,
        valid_from: Option<&ValidFrom>,
        is_deleted: Option<bool>,
        is_full: Option<bool>,
    ) -> Vec<String> {
        let q = |s: &str| self.dialect.quote(s);
        let mut out: Vec<String> =
            cols.iter().map(|c| format!("{} AS {}", self.convert(c, table_alias), q(&c.target))).collect();
        match valid_from {
            None => {}
            Some(ValidFrom::Literal(lit)) => out.push(format!("{lit} AS {}", q(VALID_FROM_COL_NAME))),
            Some(ValidFrom::SourceClock) => {
                let clock = match self.dialect {
                    Dialect::Tsql => "CAST(GETUTCDATE() AS datetime2(6))",
                    Dialect::Postgres => "STATEMENT_TIMESTAMP()",
                };
                out.push(format!("{clock} AS {}", q(VALID_FROM_COL_NAME)));
            }
        }
        if let Some(v) = is_deleted {
            out.push(format!("{} AS {}", self.bool_literal(v), q(IS_DELETED_COL_NAME)));
        }
        if let Some(v) = is_full {
            out.push(format!("{} AS {}", self.bool_literal(v), q(IS_FULL_LOAD_COL_NAME)));
        }
        out
    }

    pub fn from_clause(&self, alias: &str) -> String {
        match &self.source {
            SourceRef::Table(parts) => {
                let name: Vec<String> = parts.iter().map(|p| self.dialect.quote(p)).collect();
                format!("FROM {} AS {alias}", name.join("."))
            }
            SourceRef::Query(sql) => format!("FROM ({sql}) AS {alias}"),
        }
    }

    fn select(&self, list: &[String], alias: &str, criteria: &[String]) -> String {
        let mut sql = format!("SELECT {} {}", list.join(", "), self.from_clause(alias));
        if !criteria.is_empty() {
            let _ = write!(sql, " WHERE {}", criteria.join(" AND "));
        }
        sql
    }

    /// The full load: all columns, `__is_deleted = 0`, `__is_full_load = 1`.
    pub fn full_load(&self, cols: &[&Column], valid_from: &ValidFrom) -> String {
        self.select(&self.select_list(cols, None, Some(valid_from), Some(false), Some(true)), "t", &[])
    }

    /// The primary keys and delta values of all source rows.
    pub fn pk_ts(&self, cols: &[&Column]) -> String {
        self.select(&self.select_list(cols, None, None, None, None), "t", &[])
    }

    /// Changed rows: all columns of the rows matching `criteria`, as non-full, non-deleted rows.
    pub fn updates(&self, cols: &[&Column], criteria: &[String], valid_from: &ValidFrom) -> String {
        self.select(&self.select_list(cols, Some("t"), Some(valid_from), Some(false), Some(false)), "t", criteria)
    }

    /// `<delta col> <op> <value>`, comparing the converted source column with a local value.
    pub fn criterion(&self, delta_col: &Column, op: &str, value: &Scalar) -> String {
        format!("{} {op} {}", self.convert(delta_col, Some("t")), literal(self.dialect, value))
    }

    /// `MAX(<delta col>)`, converted like the loaded values, and `COUNT(*)` of the source.
    pub fn source_ts_cnt(&self, delta_col: Option<&Column>) -> String {
        let max = match delta_col {
            None => "NULL AS max_ts".to_string(),
            Some(c) => {
                if c.orig_type == "xid" {
                    let col = self.dialect.quote(&c.name);
                    format!("MAX(CAST(CAST({col} AS {}) AS BIGINT)) AS max_ts", self.text_type())
                } else {
                    format!("MAX({}) AS max_ts", self.convert(c, Some("t")))
                }
            }
        };
        format!("SELECT {max}, COUNT(*) AS cnt {}", self.from_clause("t"))
    }

    /// Reads the rows of the given primary keys: the select of [`SourceSql::updates`] joined to
    /// the keys, passed as a JSON array of `{"p0": ..., "p1": ...}` objects.
    pub fn rows_by_pk_json(&self, cols: &[&Column], pks: &[&Column], json: &str, valid_from: &ValidFrom) -> String {
        let sql = self.select(&self.select_list(cols, Some("t"), Some(valid_from), Some(false), Some(false)), "t", &[]);
        let col_defs: Vec<String> = pks.iter().enumerate().map(|(i, p)| format!("p{i} {}", p.type_sql)).collect();
        let pk_map: Vec<String> =
            pks.iter().enumerate().map(|(i, p)| format!("p{i} as {}", ansi_quote(&p.target))).collect();
        let on: Vec<String> = pks
            .iter()
            .map(|p| {
                let collate =
                    if p.is_string && self.dialect == Dialect::Tsql { "COLLATE Latin1_General_100_BIN " } else { "" };
                format!("t.{} {collate} = ttt.{}", ansi_quote(&p.name), ansi_quote(&p.target))
            })
            .collect();
        let rows = match self.dialect {
            Dialect::Tsql => {
                format!(
                    "SELECT {} FROM OPENJSON({}) with ({}) ",
                    pk_map.join(", "),
                    string_literal(json),
                    col_defs.join(", ")
                )
            }
            Dialect::Postgres => format!(
                "SELECT {} FROM JSON_TABLE({}, '$[*]' COLUMNS ({}))",
                pk_map.join(", "),
                string_literal(json),
                col_defs.join(", ")
            ),
        };
        format!("{sql}\n            inner join ({rows}) ttt\n                on {}\n            ", on.join(" AND "))
    }
}

/// `"name"`, the identifier quoting the T-SQL `OPENJSON` join always used; `"` doubled, which
/// T-SQL (`QUOTED_IDENTIFIER ON`) and Postgres both read as one `"` inside the identifier.
fn ansi_quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A value from the local Delta state as a source literal.
pub fn literal(dialect: Dialect, value: &Scalar) -> String {
    match value {
        Scalar::Null => "NULL".into(),
        Scalar::Bool(v) => match (dialect, v) {
            (Dialect::Tsql, true) => "(1 = 1)".into(),
            (Dialect::Tsql, false) => "(1 = 0)".into(),
            (Dialect::Postgres, true) => "TRUE".into(),
            (Dialect::Postgres, false) => "FALSE".into(),
        },
        Scalar::Int(v) => v.to_string(),
        Scalar::Float(v) => crate::scalar::python_float_repr(*v),
        Scalar::Decimal { .. } => value.to_text(),
        Scalar::Str(s) => string_literal(s),
        Scalar::Bytes(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02X}")).collect();
            match dialect {
                Dialect::Tsql => format!("0x{hex}"),
                Dialect::Postgres => format!("x'{hex}'"),
            }
        }
        Scalar::Date(_) => format!("CAST({} AS DATE)", string_literal(&value.to_text())),
        Scalar::Timestamp { utc, .. } => {
            let text = string_literal(&value.to_text());
            match (dialect, utc) {
                (Dialect::Tsql, false) => format!("CAST({text} AS DATETIME2)"),
                (Dialect::Tsql, true) => format!("CAST({text} AS DATETIMEOFFSET) AT TIME ZONE 'UTC'"),
                (Dialect::Postgres, false) => format!("CAST({text} AS TIMESTAMP)"),
                (Dialect::Postgres, true) => format!("CAST({text} AS TIMESTAMPTZ)"),
            }
        }
    }
}

/// The query that reads the source clock for `__timestamp`.
pub fn load_timestamp_query(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Tsql => "SELECT CAST(GETUTCDATE() AS datetime2(6)) AS ts",
        Dialect::Postgres => "SELECT STATEMENT_TIMESTAMP() AS ts",
    }
}

/// `__timestamp` of one load pass as a literal (without the alias). `micros` is UTC.
pub fn load_timestamp_literal(dialect: Dialect, micros: i64) -> String {
    let ts = Scalar::Timestamp { micros, utc: dialect == Dialect::Postgres };
    match dialect {
        Dialect::Tsql => format!("CAST({} AS datetime2(6))", string_literal(&ts.to_text_fixed_micros())),
        Dialect::Postgres => format!("CAST({} AS TIMESTAMPTZ)", string_literal(&ts.to_text())),
    }
}

/// One slice of `sql` by ranges of an integer `column`: `lower <= column < upper`, open where
/// `None`; the last slice (`upper` = `None`) also takes the NULLs when `with_nulls`.
pub fn partition_sql(
    dialect: Dialect,
    sql: &str,
    column: &str,
    lower: Option<i64>,
    upper: Option<i64>,
    with_nulls: bool,
) -> String {
    let col = format!("_p.{}", dialect.quote(column));
    let mut conds = Vec::new();
    if let Some(l) = lower {
        conds.push(format!("{col} >= {l}"));
    }
    if let Some(u) = upper {
        conds.push(format!("{col} < {u}"));
    }
    let mut cond = if conds.is_empty() { "1 = 1".to_string() } else { conds.join(" AND ") };
    if with_nulls && upper.is_none() {
        cond = format!("({cond}) OR {col} IS NULL");
    }
    format!("SELECT * FROM ({sql}) AS _p WHERE {cond}")
}

/// `n` slices of `min..=max` as `(lower, upper)` pairs: the first open below, the last open above.
pub fn partition_bounds(min: i64, max: i64, n: usize) -> Vec<(Option<i64>, Option<i64>)> {
    let n = n.max(1) as i128;
    let span = (max as i128 - min as i128 + 1).max(1);
    let step = (span + n - 1) / n;
    let cuts: Vec<i64> = (1..n).map(|i| (min as i128 + i * step).min(max as i128 + 1) as i64).collect();
    let mut out = Vec::new();
    let mut lower = None;
    for c in cuts {
        if lower.is_some_and(|l| l >= c) {
            continue;
        }
        out.push((lower, Some(c)));
        lower = Some(c);
    }
    out.push((lower, None));
    out
}

/// The default target name: characters Spark rejects in column names become `_`.
pub fn compat_name(name: &str) -> String {
    name.chars().map(|c| if " ,;{}()\n\t=".contains(c) { '_' } else { c }).collect()
}

/// sqlglot's default type map for a dialect (T-SQL only): `(key, type sql, is_string)`.
pub fn default_type_map(dialect: Dialect) -> TypeMap {
    let entries: &[(&str, &str)] = match dialect {
        Dialect::Tsql => &[
            ("datetime", "datetime2(6)"),
            ("datetime2", "datetime2(6)"),
            ("rowversion", "BIGINT"),
            ("timestamp", "BIGINT"),
            ("tinyint", "SMALLINT"),
        ],
        Dialect::Postgres => &[],
    };
    entries.iter().map(|(k, v)| (k.to_string(), MappedType { sql: v.to_string(), is_string: false })).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, key: &str, orig: &str, is_string: bool) -> Column {
        Column {
            name: name.into(),
            target: compat_name(name),
            type_key: key.into(),
            type_sql: key.to_uppercase(),
            orig_type: orig.into(),
            is_string,
            is_numeric: !is_string,
            is_identity: false,
        }
    }

    #[test]
    fn quotes_and_escapes() {
        assert_eq!(Dialect::Tsql.quote("a]b"), "[a]]b]");
        assert_eq!(Dialect::Postgres.quote("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn convert_trims_strings_but_not_guids() {
        let s = SourceSql {
            dialect: Dialect::Tsql,
            source: SourceRef::Table(vec!["dbo".into(), "t".into()]),
            type_map: default_type_map(Dialect::Tsql),
            no_trim: false,
        };
        assert_eq!(s.convert(&col("a", "varchar", "varchar(10)", true), Some("t")), "TRIM([t].[a])");
        assert_eq!(s.convert(&col("g", "uniqueidentifier", "uniqueidentifier", true), None), "[g]");
        assert_eq!(s.convert(&col("rv", "timestamp", "timestamp", false), None), "CAST([rv] AS BIGINT)");
    }

    #[test]
    fn partitions_cover_the_range_once() {
        assert_eq!(
            partition_bounds(1, 100, 4),
            vec![(None, Some(26)), (Some(26), Some(51)), (Some(51), Some(76)), (Some(76), None)]
        );
        assert_eq!(partition_bounds(5, 5, 3), vec![(None, Some(6)), (Some(6), None)]);
        assert_eq!(
            partition_sql(Dialect::Tsql, "SELECT 1 AS id", "id", Some(3), None, true),
            "SELECT * FROM (SELECT 1 AS id) AS _p WHERE (_p.[id] >= 3) OR _p.[id] IS NULL"
        );
    }

    #[test]
    fn compat_name_replaces_spark_unfriendly_chars() {
        assert_eq!(compat_name("User - iD"), "User_-_iD");
        assert_eq!(compat_name("a,b;c{d}(e)\nf\tg=h"), "a_b_c_d__e__f_g_h");
    }

    #[test]
    fn rows_by_pk_escapes_hostile_names() {
        let s = SourceSql {
            dialect: Dialect::Tsql,
            source: SourceRef::Table(vec!["dbo".into(), "t]x".into()]),
            type_map: default_type_map(Dialect::Tsql),
            no_trim: false,
        };
        let evil = col("x\" = ttt.\"id\" OR 1=1 --", "int", "int", false);
        let bracket = col("a]b", "int", "int", false);
        let vf = ValidFrom::SourceClock;
        let sql = s.rows_by_pk_json(&[&evil, &bracket], &[&evil, &bracket], "[]", &vf);
        assert!(sql.contains("FROM [dbo].[t]]x] AS t"), "{sql}");
        assert!(sql.contains("[x\" = ttt.\"id\" OR 1=1 --]"), "{sql}");
        assert!(sql.contains("[a]]b]"), "{sql}");
        let ansi = |n: &str| format!("\"{}\"", n.replace('"', "\"\""));
        assert!(sql.contains(&format!("p0 as {}", ansi(&evil.target))), "{sql}");
        assert!(sql.contains(&format!("t.{}  = ttt.{}", ansi(&evil.name), ansi(&evil.target))), "{sql}");
        assert!(sql.contains("t.\"a]b\"  = ttt.\"a]b\""), "{sql}");
        // every double quote of the ON clause pairs up: no identifier ends early
        let on = &sql[sql.find("on ").unwrap()..];
        assert_eq!(on.matches('"').count() % 2, 0);
        let pg = SourceSql { dialect: Dialect::Postgres, ..s };
        assert!(pg.rows_by_pk_json(&[&evil], &[&evil], "[]", &vf).contains(&ansi(&evil.name)));
    }
}
