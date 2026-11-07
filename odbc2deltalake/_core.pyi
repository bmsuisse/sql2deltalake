"""Type stubs of the Rust extension module (crates/sql2deltalake-py)."""

from typing import Any, Mapping, Optional, Sequence, Union

__version__: str

class ArrowStream:
    """An Arrow C stream (`__arrow_c_stream__`), consumable once."""

    def __arrow_c_stream__(self, requested_schema: Optional[object] = None) -> object: ...
    def __arrow_c_schema__(self) -> object: ...

class SqlBuilder:
    def __init__(
        self,
        dialect_name: str,
        source_ref: Union[Sequence[str], str],
        type_map_spec: Mapping[str, tuple[str, bool]],
        no_trim: bool,
        columns: Sequence[Mapping[str, Any]],
    ) -> None: ...
    def full_load(self, valid_from: Optional[str] = None) -> str: ...
    def pk_ts(self, columns: Sequence[int]) -> str: ...
    def updates(self, criteria: Sequence[str], valid_from: Optional[str] = None) -> str: ...
    def criterion(self, column: int, op: str, value: Any) -> str: ...
    def source_ts_cnt(self, column: Optional[int] = None) -> str: ...
    def rows_by_pk_json(
        self, pks: Sequence[int], json: str, valid_from: Optional[str] = None
    ) -> str: ...

def load_timestamp_literal(dialect_name: str, micros: int) -> str: ...
def compat_name(name: str) -> str: ...
def normalize(
    source: object,
    time_scales: Optional[Mapping[int, int]] = None,
    target: Optional[object] = None,
) -> ArrowStream: ...
def read_sql(
    connection_string: str,
    sql: str,
    batch_size: int = 65536,
    utf8_strings: bool = False,
    partition_on: Optional[str] = None,
    partitions: int = 1,
) -> ArrowStream:
    """Only in builds with the cargo feature `arrowsql-core` (the default)."""
    ...
def pk_diff(
    snapshot: object,
    previous: object,
    keys: Sequence[str],
    delta: str,
    watermark: object,
) -> dict[str, Any]: ...
def count_new_keys(rows: object, previous: object, keys: Sequence[str]) -> int: ...
def merge_latest(
    previous: Optional[object],
    loaded: object,
    keys: Sequence[str],
    delta: str,
    order: str = "__timestamp",
    deleted: str = "__is_deleted",
) -> dict[str, Any]: ...
def run_load(host: object, load_spec: Mapping[str, Any]) -> dict[str, Any]: ...
def restore_last_pk(host: object, load_spec: Mapping[str, Any]) -> bool: ...
