"""The source SQL the Python (sqlglot) implementation generates for a set of column/source/config
combinations. The Rust query builder must produce the same text: `cargo test` checks the snapshot
in crates/sql2deltalake-core/tests/golden/sqlgen.json, tests/test_30_native_sqlgen.py compares live.

Rewrite the snapshot (with the Python implementation in place): .venv/bin/python -m tests.sql_golden
"""

import datetime as dt
import decimal
import json
from pathlib import Path

import sqlglot as sg
import sqlglot.expressions as ex

from odbc2deltalake._native import column_spec, source_spec, type_map_spec
from odbc2deltalake.db_to_delta import (
    _fetch_load_timestamp,
    _get_cols_select,
    _get_type_map,
    _get_update_sql,
    _source_convert,
)
from odbc2deltalake.load_infos import retrieve_source_ts_cnt
from odbc2deltalake.metadata import InformationSchemaColInfo as Col
from odbc2deltalake.query import sql_quote_name, sql_quote_value
from odbc2deltalake.sql_schema import is_string_type
from odbc2deltalake.write_init import WriteConfig, WriteConfigAndInfos

OUT = Path(__file__).parent.parent / "crates/sql2deltalake-core/tests/golden/sqlgen.json"


def col(name, type_str, dialect, **kw):
    return Col(
        column_name=name,
        data_type=ex.DataType.build(type_str, dialect=dialect),
        data_type_str=type_str,
        **kw,
    )


def columns(dialect):
    if dialect == "tsql":
        return [
            col("User - iD", "bigint", dialect, is_identity=True),
            col("FirstName", "varchar(100)", dialect),
            col("LastName", "nvarchar(MAX)", dialect),
            col("Age", "decimal(15,3)", dialect),
            col("companyid", "varchar(10)", dialect),
            col("dt", "datetime", dialect),
            col("d2", "datetime2(7)", dialect),
            col("uid", "uniqueidentifier", dialect),
            col("nbr", "tinyint", dialect),
            col("x]y", "int", dialect),
            col("time stamp", "timestamp", dialect),
        ]
    return [
        col("User - iD", "bigint", dialect),
        col("FirstName", "varchar(100)", dialect),
        col("LastName", "text", dialect),
        col("Age", "numeric(15,3)", dialect),
        col("companyid", "character varying(10)", dialect),
        col("dt", "timestamp", dialect),
        col("uid", "uuid", dialect),
        col("nbr", "smallint", dialect),
        col('x"y', "integer", dialect),
        Col(
            column_name="xmin",
            data_type=ex.DataType.build("oid", dialect="postgres"),
            data_type_str="xid",
        ),
    ]


class _Reader:
    """Enough of a DataSourceReader to build queries; no connection."""

    def __init__(self, dialect, now):
        self.source_dialect = dialect
        self.query_dialect = "duckdb"
        self.now = now

    def source_sql_to_py(self, sql):
        return [{"ts": self.now}]


class _Logger:
    def info(self, *a, **k):
        pass

    warning = info


def literal_spec(v):
    if isinstance(v, bool):
        return {"bool": v}
    if isinstance(v, int):
        return {"int": v}
    if isinstance(v, float):
        return {"float": v}
    if isinstance(v, decimal.Decimal):
        sign, digits, exp = v.as_tuple()
        value = int("".join(map(str, digits))) * (-1 if sign else 1)
        return {"decimal": [str(value), -exp]}
    if isinstance(v, str):
        return {"str": v}
    if isinstance(v, bytes):
        return {"bytes": v.hex()}
    if isinstance(v, dt.datetime):
        utc = v.tzinfo is not None
        naive = v.replace(tzinfo=None)
        micros = (naive - dt.datetime(1970, 1, 1)) // dt.timedelta(microseconds=1)
        return {"timestamp": [micros, utc]}
    if isinstance(v, dt.date):
        return {"date": (v - dt.date(1970, 1, 1)).days}
    raise TypeError(v)


LITERALS = [
    12345,
    -3,
    "abc'd",
    dt.datetime(2020, 1, 2, 3, 4, 5, 123456),
    dt.datetime(2020, 1, 2, 3, 4, 5),
    dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc),
    dt.date(2020, 1, 2),
    decimal.Decimal("12.500"),
    1.5,
    True,
    b"\x00\x01",
]

SOURCES = [
    ("dbo", "user"),
    ("long schema", "long table name"),
    ("db", "dbo", "x"),
    "plain",
    "query",
]


def cases():
    out = []
    for dialect in ["tsql", "postgres"]:
        cols = columns(dialect)
        pk_sets = [[cols[0]], [cols[0], cols[4]]]
        delta_col = cols[-1]
        for src in SOURCES:
            toq = (
                sg.parse_one(
                    "SELECT a, b FROM dbo.x WHERE y > 1 AND z = 'q'", dialect=dialect
                )
                if src == "query"
                else src
            )
            for cfg_i, cfg in enumerate([
                WriteConfig(dialect=dialect),
                WriteConfig(dialect=dialect, no_trim=True),
                WriteConfig(
                    dialect=dialect,
                    data_type_map={
                        "decimal" if dialect == "tsql" else "decimal": ex.DataType.build(
                            "float", dialect=dialect
                        ),
                        "varchar": ex.DataType.build("varchar(200)", dialect=dialect),
                    },
                ),
            ]):
                full_matrix = src == SOURCES[0] and cfg_i == 0
                now = dt.datetime(2024, 5, 6, 7, 8, 9, 101112)
                reader = _Reader(dialect, now)
                infos = WriteConfigAndInfos(
                    col_infos=cols,
                    pk_cols=pk_sets[0],
                    delta_col=delta_col,
                    write_config=cfg,
                    destination=None,  # type: ignore
                    source=reader,  # type: ignore
                    table_or_query=toq,
                    logger=_Logger(),  # type: ignore
                )
                load_ts = _fetch_load_timestamp(reader, dialect)  # type: ignore
                case = {
                    "dialect": dialect,
                    "source": source_spec(infos),
                    "type_map": type_map_spec(cfg),
                    "no_trim": cfg.no_trim,
                    "columns": [column_spec(c, cfg) for c in cols],
                    "now_micros": literal_spec(now)["timestamp"][0],
                    "expected": {},
                }
                exp = case["expected"]
                exp["load_timestamp"] = load_ts.this.sql(dialect)
                for name, vf in [("clock", None), ("literal", load_ts)]:
                    exp["full_load_" + name] = (
                        infos.from_("t")
                        .select(
                            *_get_cols_select(
                                is_deleted=False,
                                is_full=True,
                                cols=cols,
                                with_valid_from=True,
                                data_type_map=_get_type_map(cfg),
                                system="source",
                                get_target_name=cfg.get_target_name,
                                no_trim=cfg.no_trim,
                                source_dialect=dialect,
                                valid_from=vf,
                            )
                        )
                        .sql(dialect)
                    )
                exp["pk_ts"] = (
                    infos.from_("t")
                    .select(
                        *_get_cols_select(
                            cols=pk_sets[1] + [delta_col],
                            data_type_map=_get_type_map(cfg),
                            system="source",
                            get_target_name=cfg.get_target_name,
                            no_trim=cfg.no_trim,
                            source_dialect=dialect,
                        )
                    )
                    .sql(dialect)
                )
                exp["source_ts_cnt"] = (
                    infos.from_("t")
                    .select(
                        ex.func(
                            "MAX",
                            ex.cast(
                                ex.cast(
                                    ex.column(delta_col.column_name, quoted=True),
                                    ex.DataType.Type.TEXT,
                                ),
                                ex.DataType.Type.BIGINT,
                            )
                            if delta_col.data_type_str == "xid"
                            else ex.column(delta_col.column_name, quoted=True),
                        ).as_("max_ts"),
                        ex.Count(this=ex.Star()).as_("cnt"),
                    )
                    .sql(dialect)
                )
                crit_cols = [delta_col, cols[5], cols[1]]
                exp["updates"] = []
                for c in crit_cols:
                    for v in LITERALS if full_matrix else LITERALS[:1]:
                        for op in [">", ">="] if full_matrix else [">"]:
                            crit = _source_convert(
                                c.column_name,
                                c.data_type,
                                c.data_type_str,
                                table_alias="t",
                                type_map=_get_type_map(cfg),
                                no_trim=cfg.no_trim,
                                dialect=dialect,
                            )
                            crit = crit > ex.convert(v) if op == ">" else crit >= ex.convert(v)
                            exp["updates"].append(
                                {
                                    "column": cols.index(c),
                                    "op": op,
                                    "value": literal_spec(v),
                                    "valid_from": "literal",
                                    "sql": _get_update_sql(
                                        cols=cols,
                                        criterion=crit,
                                        query=infos.from_("t"),
                                        write_config=cfg,
                                        valid_from=load_ts,
                                    ),
                                }
                            )
                exp["updates_no_criterion"] = _get_update_sql(
                    cols=cols, criterion=None, query=infos.from_("t"), write_config=cfg
                )
                exp["rows_by_pk"] = []
                for pks in pk_sets:
                    data = [
                        {f"p{i}": v for i, v in enumerate(vals)}
                        for vals in [(1, "c1"), (2, "it's"), (3, "é\n")][:2]
                    ]
                    data = [{k: v for k, v in d.items() if int(k[1:]) < len(pks)} for d in data]
                    js = json.dumps(data, default=str)
                    exp["rows_by_pk"].append(
                        {
                            "pks": [cols.index(p) for p in pks],
                            "json": js,
                            "sql": _rows_by_pk(infos, pks, js, load_ts),
                        }
                    )
                out.append(case)
    return out


def _rows_by_pk(infos, pks, js, load_ts):
    """The query of db_to_delta._write_delta2 (an inner function there)."""
    write_config = infos.write_config

    def _collate(c):
        if is_string_type(c.data_type) and write_config.dialect == "tsql":
            return "COLLATE Latin1_General_100_BIN "
        return ""

    col_defs = ", ".join(
        [f"p{i} {p.data_type.sql(write_config.dialect)}" for i, p in enumerate(pks)]
    )
    selects = list(
        _get_cols_select(
            infos.col_infos,
            is_full=False,
            is_deleted=False,
            with_valid_from=True,
            table_alias="t",
            data_type_map=_get_type_map(write_config),
            system="source",
            get_target_name=write_config.get_target_name,
            no_trim=write_config.no_trim,
            source_dialect=write_config.dialect,
            valid_from=load_ts,
        )
    )
    sql = infos.from_("t").select(*selects).sql(write_config.dialect)
    pk_map = ", ".join(
        [
            "p" + str(i) + " as " + sql_quote_name(write_config.get_target_name(c))
            for i, c in enumerate(pks)
        ]
    )
    on = " AND ".join(
        [
            f"t.{sql_quote_name(c.column_name)} {_collate(c)} = ttt.{sql_quote_name(write_config.get_target_name(c))}"
            for c in pks
        ]
    )
    if write_config.dialect == "tsql":
        return f"""{sql}
            inner join (SELECT {pk_map} FROM OPENJSON({sql_quote_value(js)}) with ({col_defs}) ) ttt
                on {on}
            """
    return f"""{sql}
            inner join (SELECT {pk_map} FROM JSON_TABLE({sql_quote_value(js)}, '$[*]' COLUMNS ({col_defs}))) ttt
                on {on}
            """


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(cases(), indent=0, ensure_ascii=False) + "\n", encoding="utf-8")
    print("wrote", OUT)
