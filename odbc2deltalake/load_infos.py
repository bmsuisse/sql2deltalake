from typing import Any, TYPE_CHECKING
import sqlglot as sg
import sqlglot.expressions as ex

from odbc2deltalake.metadata import InformationSchemaColInfo

if TYPE_CHECKING:
    from .write_init import WriteConfigAndInfos


def get_local_delta_value_and_count(
    infos: "WriteConfigAndInfos",
) -> tuple[Any, int]:
    from odbc2deltalake.write_init import DBDeltaPathConfigs

    if (infos.destination / "delta_load" / DBDeltaPathConfigs.PRIMARY_KEYS_TS).exists():
        delta_path = (
            infos.destination / "delta_load" / DBDeltaPathConfigs.PRIMARY_KEYS_TS
        )
    else:
        delta_path = infos.destination / "delta"
    tmp_view_name = "temp_" + str(abs(hash(str(delta_path))))
    infos.source.local_register_update_view(delta_path, tmp_view_name)

    row = infos.source.local_execute_sql_to_py(
        sg.from_(ex.to_identifier(tmp_view_name)).select(
            (
                ex.func(
                    "MAX",
                    ex.column(infos.write_config.get_target_name(infos.delta_col)),
                ).as_("max_ts")
                if infos.delta_col
                else ex.convert(None).as_("max_ts")
            ),
            ex.Count(this=ex.Star()).as_("cnt"),
        )
    )[0]
    mt, cnt = row["max_ts"], row["cnt"]
    if isinstance(mt, bytearray):
        return bytes(mt), cnt
    return mt, cnt


def source_ts_cnt_query(infos: "WriteConfigAndInfos") -> ex.Select:
    """MAX of the delta column, converted like the loaded values (a datetime2(7) rounds to the
    microseconds Delta stores, a rowversion becomes a BIGINT), and COUNT(*) of the source."""
    from odbc2deltalake.db_to_delta import _get_type_map, _source_convert

    def _delta_col(col: InformationSchemaColInfo):
        if col.data_type_str == "xid":
            col_expr = ex.column(col.column_name, quoted=True)
            return ex.cast(
                ex.cast(col_expr, ex.DataType.Type.TEXT), ex.DataType.Type.BIGINT
            )
        wc = infos.write_config
        return _source_convert(
            col.column_name,
            col.data_type,
            col.data_type_str,
            table_alias="t",
            type_map=_get_type_map(wc),
            no_trim=wc.no_trim,
            dialect=wc.dialect,
        )

    return infos.from_("t").select(
        (
            ex.func("MAX", _delta_col(infos.delta_col)).as_("max_ts")
            if infos.delta_col
            else ex.convert(None).as_("max_ts")
        ),
        ex.Count(this=ex.Star()).as_("cnt"),
    )


def retrieve_source_ts_cnt(infos: "WriteConfigAndInfos"):
    pk_ts_col_select = source_ts_cnt_query(infos)
    infos.logger.info(
        "Retrieve source delta value / count",
        sql=pk_ts_col_select.sql(infos.write_config.dialect),
    )
    row = infos.source.source_sql_to_py(sql=pk_ts_col_select)
    return row[0]["max_ts"], row[0]["cnt"]
