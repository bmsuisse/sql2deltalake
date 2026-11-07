from sql2deltalake.destination.destination import Destination
from sql2deltalake.read_utils.delta_rs import _build_type, _get_type
from .local_reader import LocalDeltaReader
from typing import TYPE_CHECKING, Literal, Optional, Union
import sqlglot.expressions as ex

if TYPE_CHECKING:
    import pyarrow as pa
    from sql2deltalake.metadata import InformationSchemaColInfo
    from adbc_driver_manager.dbapi import Connection


# ADBC returns postgres "numeric" as an opaque string extension type without
# precision/scale, so we read it as a wide decimal


def _is_opaque_numeric(tp: "pa.DataType") -> bool:
    import pyarrow as pa

    return (
        isinstance(tp, pa.BaseExtensionType)
        and getattr(tp, "type_name", None) == "numeric"
    )


def _decimal_schema(schema: "pa.Schema") -> "pa.Schema":
    import pyarrow as pa

    return pa.schema(
        [
            f.with_type(pa.decimal128(38, 18)) if _is_opaque_numeric(f.type) else f
            for f in schema
        ],
        metadata=schema.metadata,
    )


def _decimal_batch(batch: "pa.RecordBatch", schema: "pa.Schema") -> "pa.RecordBatch":
    import pyarrow as pa

    cols = [
        c.storage.cast(f.type) if _is_opaque_numeric(c.type) else c
        for c, f in zip(batch.columns, schema)
    ]
    return pa.RecordBatch.from_arrays(cols, schema=schema)


class ADBCReader(LocalDeltaReader):
    """Reads the source through an ADBC connection (used for Postgres)."""

    def __init__(
        self,
        connection: "Connection",
        local_db: str = ":memory:",
        source_dialect: str = "postgres",
    ) -> None:
        super().__init__(local_db=local_db, source_dialect=source_dialect)
        self.connection = connection

    def _local_schema(self, schema: "pa.Schema") -> "Optional[pa.Schema]":
        """Postgres timestamps come back from DuckDB without a time zone; they are UTC."""
        import pyarrow as pa

        if self.source_dialect != "postgres" or not any(
            pa.types.is_timestamp(f.type) and f.type.tz is None for f in schema
        ):
            return None
        return pa.schema(
            [
                pa.field(f.name, pa.timestamp(f.type.unit, tz="UTC"), f.nullable, f.metadata)
                if pa.types.is_timestamp(f.type) and f.type.tz is None
                else f
                for f in schema
            ]
        )

    def source_schema_limit_one(
        self, sql: ex.Query
    ) -> "list[InformationSchemaColInfo]":
        from ..metadata import InformationSchemaColInfo

        limit_sql = sql.limit(0).sql(self.source_dialect)
        with self.connection.cursor() as cursor:
            cursor.execute(limit_sql)
            sc = _decimal_schema(cursor.fetch_arrow_table().schema)
        return [
            InformationSchemaColInfo(
                column_name=n,
                data_type_str=str(sc.field(n).type),
                data_type=_build_type(_get_type(sc.field(n).type)),
                is_nullable=sc.field(n).nullable,
            )
            for n in sc.names
        ]

    def source_sql_to_py(self, sql: Union[str, ex.Query]) -> list[dict]:
        if isinstance(sql, ex.Query):
            sql = sql.sql(self.source_dialect)
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            assert cursor.description is not None
            col_names = [desc[0] for desc in cursor.description]
            return [dict(zip(col_names, row)) for row in cursor.fetchall()]

    def source_write_sql_to_delta(
        self,
        sql: str,
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        *,
        allow_schema_drift: Union[bool, Literal["new_only"]],
    ):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            reader = cursor.fetch_record_batch()
            if any(_is_opaque_numeric(f.type) for f in reader.schema):
                import pyarrow as pa

                dec_schema = _decimal_schema(reader.schema)
                reader = pa.RecordBatchReader.from_batches(
                    dec_schema, (_decimal_batch(b, dec_schema) for b in reader)
                )
            self._write_reader_to_delta(
                reader, delta_path, mode, allow_schema_drift, all_nullable=True
            )
