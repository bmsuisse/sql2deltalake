"""The boundary to the Rust core (`odbc2deltalake._core`): sqlglot objects and user callables are
turned into plain values here, once per load, so the core needs no Python while it runs."""

from typing import TYPE_CHECKING, Any, Mapping, Union

import sqlglot.expressions as ex

from .sql_schema import is_string_type

if TYPE_CHECKING:
    from .metadata import InformationSchemaColInfo
    from .write_init import WriteConfig, WriteConfigAndInfos


def column_spec(
    c: "InformationSchemaColInfo", write_config: "WriteConfig"
) -> dict[str, Any]:
    dialect = write_config.dialect
    return {
        "name": c.column_name,
        "target": write_config.get_target_name(c),
        "type_key": ex.DataType(this=c.data_type.this).sql(dialect).lower(),
        "type_sql": c.data_type.sql(dialect),
        "orig_type": c.data_type_str or "",
        "is_string": bool(is_string_type(c.data_type)),
        "is_numeric": c.data_type.this
        in [ex.DataType.Type.BOOLEAN, *ex.DataType.NUMERIC_TYPES],
        "is_identity": bool(c.is_identity),
    }


def type_map_spec(write_config: "WriteConfig") -> dict[str, tuple[str, bool]]:
    from .write_init import DEFAULT_DATA_TYPE_MAP

    tm: Mapping[str, Union[ex.DataType, ex.DataType.Type]] = (
        DEFAULT_DATA_TYPE_MAP.get(write_config.dialect, {})
        if write_config.data_type_map == "default"
        else write_config.data_type_map
    )
    res: dict[str, tuple[str, bool]] = {}
    for key, mapped in tm.items():
        to = ex.cast(ex.column("x"), mapped).to  # what sqlglot renders for CAST(x AS mapped)
        res[key] = (to.sql(write_config.dialect), bool(is_string_type(mapped)))
    return res


def source_spec(infos: "WriteConfigAndInfos") -> Union[list[str], str]:
    """The table name parts, or the query rendered in the source dialect."""
    toq = infos.table_or_query
    if isinstance(toq, ex.Query):
        return toq.sql(infos.write_config.dialect)
    if isinstance(toq, str):
        return [toq]
    return list(toq)
