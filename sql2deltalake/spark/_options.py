"""Options of the Spark data source and of ``prepare``. Names are case-insensitive and may be
written camelCase or snake_case (``deltaColumn`` = ``delta_column``)."""

import re
from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Tuple, Union

LOAD_MODES = (
    "append",
    "force_full",
    "append_inserts",
    "simple_delta",
    "simple_delta_check",
)

_ALIASES = {
    "url": "url",
    "connectionstring": "url",
    "table": "table",
    "dbtable": "table",
    "query": "query",
    "keys": "keys",
    "primarykeys": "keys",
    "deltacolumn": "delta_column",
    "target": "target",
    "state": "state",
    "loadmode": "load_mode",
    "numpartitions": "num_partitions",
    "partitioncolumn": "partition_column",
    "batchsize": "batch_size",
    "utf8strings": "utf8_strings",
    "notrim": "no_trim",
    "nocomplexentriesload": "no_complex_entries_load",
    "maxinlinedeletes": "max_inline_deletes",
    "plan": "plan",
    "path": "path",  # Spark passes load("x") as option path
}
_TRUE = {"true", "1", "yes"}
_FALSE = {"false", "0", "no"}
_NAME_PART = re.compile(r"\[(?:[^\]]|\]\])+\]|[^.\[\]]+")


def normalize(raw: Mapping[str, Any]) -> dict:
    out: dict = {}
    for key, value in dict(raw).items():
        if value is None:
            continue
        name = _ALIASES.get(str(key).lower().replace("_", ""))
        if name is None:
            raise ValueError(
                f"unknown option {key!r}; known options: "
                + ", ".join(sorted(set(_ALIASES.values()) - {"path"}))
            )
        if name in out and out[name] != value:
            raise ValueError(f"option {name!r} is set twice")
        out[name] = value
    return out


def _bool(opts: dict, name: str, default: bool) -> bool:
    value = opts.get(name)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text not in _TRUE | _FALSE:
        raise ValueError(f"{name} must be true or false, got {value!r}")
    return text in _TRUE


def _int(opts: dict, name: str, default: int, minimum: int) -> int:
    value = opts.get(name)
    if value is None:
        return default
    try:
        out = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None
    if out < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {out}")
    return out


def _list(value: Union[str, List[str], Tuple[str, ...], None]) -> Optional[List[str]]:
    if value is None:
        return None
    names = (
        [n.strip() for n in value.split(",")] if isinstance(value, str) else list(value)
    )
    if not names or not all(names):
        raise ValueError("keys must be a comma separated list of column names")
    return names


def parse_table(value: str) -> Tuple[str, ...]:
    """``schema.table`` / ``[schema].[table]`` -> unquoted name parts."""
    parts, pos, value = [], 0, value.strip()
    while True:
        match = _NAME_PART.match(value, pos)
        if not match:
            raise ValueError(
                f"table must be a name such as schema.table, got {value!r}"
            )
        part = match.group()
        parts.append(
            part[1:-1].replace("]]", "]") if part.startswith("[") else part.strip()
        )
        pos = match.end()
        if pos == len(value):
            break
        if value[pos] != "." or len(parts) == 3:
            raise ValueError(
                f"table must be a name such as schema.table, got {value!r}"
            )
        pos += 1
    return tuple(parts)


@dataclass(frozen=True)
class LoadOptions:
    url: str
    table: Optional[Tuple[str, ...]]
    query: Optional[str]
    keys: Optional[List[str]]
    delta_column: Optional[str]
    target: Optional[str]
    state: Optional[str]
    load_mode: str
    num_partitions: int
    partition_column: Optional[str]
    batch_size: int
    utf8_strings: bool
    no_trim: bool
    no_complex_entries_load: bool
    max_inline_deletes: int

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "LoadOptions":
        opts = normalize(raw)
        url = opts.get("url")
        if not url:
            raise ValueError(
                "option 'url' (the SQL Server connection string) is required"
            )
        table, query = opts.get("table") or opts.get("path"), opts.get("query")
        if bool(table) == bool(query):
            raise ValueError("set exactly one of the options 'table' and 'query'")
        load_mode = str(opts.get("load_mode", "append")).lower()
        if load_mode not in LOAD_MODES:
            raise ValueError(
                f"load_mode must be one of {', '.join(LOAD_MODES)}, got {load_mode!r}"
            )
        return cls(
            url=str(url),
            table=parse_table(str(table)) if table else None,
            query=str(query) if query else None,
            keys=_list(opts.get("keys")),
            delta_column=opts.get("delta_column"),
            target=opts.get("target"),
            state=opts.get("state"),
            load_mode=load_mode,
            num_partitions=_int(opts, "num_partitions", 1, 1),
            partition_column=opts.get("partition_column"),
            batch_size=_int(opts, "batch_size", 65536, 1),
            utf8_strings=_bool(opts, "utf8_strings", False),
            no_trim=_bool(opts, "no_trim", False),
            no_complex_entries_load=_bool(opts, "no_complex_entries_load", False),
            max_inline_deletes=_int(opts, "max_inline_deletes", 100_000, 0),
        )

    def table_or_query(self) -> Any:
        if self.query:
            import sqlglot as sg

            return sg.parse_one(self.query, dialect="tsql")
        assert self.table is not None
        return self.table if len(self.table) > 1 else self.table[0]
