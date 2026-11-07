from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional, Sequence, Union

import sqlglot as sg
import sqlglot.expressions as ex

from odbc2deltalake.destination.destination import Destination
from odbc2deltalake.read_utils.delta_rs import DeltaRSDeltaOps, _all_nullable
from odbc2deltalake.reader.reader import DataSourceReader, DeltaOps

if TYPE_CHECKING:
    import pyarrow as pa


def _delta_table(delta_path: Destination):
    dt = delta_path.as_delta_table()
    if isinstance(dt, Path):
        from deltalake import DeltaTable

        dt = DeltaTable(dt)
    return dt


class LocalDeltaReader(DataSourceReader):
    """The local half of a reader: Delta tables written with delta-rs, queried with DuckDB.
    Subclasses implement the source reads (`source_*`)."""

    def __init__(self, local_db: str = ":memory:", source_dialect: str = "tsql") -> None:
        from deltalake import WriterProperties

        self.writer_properties = WriterProperties(compression="ZSTD")
        self.duck_con = None
        self.local_db = local_db
        self.source_dialect = source_dialect

    @property
    def supports_proc_exec(self):
        return True

    @property
    def query_dialect(self) -> str:
        return "duckdb"

    def _duck(self):
        import duckdb

        self.duck_con = self.duck_con or duckdb.connect(self.local_db)
        return self.duck_con

    def local_register_update_view(
        self,
        delta_path: Destination,
        view_name: str,
        *,
        version: Union[int, None] = None,
    ):
        from deltalake2db import duckdb_create_view_for_delta

        dt = delta_path.as_delta_table()
        if version is not None:
            dt = _delta_table(delta_path)
            dt.load_as_version(version)
        duckdb_create_view_for_delta(self._duck(), dt, view_name)

    def local_execute_sql_to_py(self, sql: ex.Query) -> list[dict]:
        with self._duck().cursor() as cursor:
            cursor.execute(sql.sql(self.query_dialect))
            assert cursor.description is not None
            col_names = [desc[0] for desc in cursor.description]
            return [dict(zip(col_names, row)) for row in cursor.fetchall()]

    def local_register_view(self, sql: ex.Query, view_name: str):
        self._duck().sql(
            f"CREATE OR REPLACE VIEW {view_name} AS {sql.sql(self.query_dialect)}"
        )

    def local_pylist_to_delta(
        self,
        pylist: list[dict],
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        dummy_record: Union[dict, None] = None,
    ):
        from deltalake import write_deltalake
        import pyarrow as pa

        schema = (
            pa.RecordBatch.from_pylist([dummy_record]).schema if dummy_record else None
        )
        batch = pa.RecordBatch.from_pylist(pylist, schema=schema)
        dp, do = delta_path.as_path_options("object_store")

        write_deltalake(
            dp,
            [batch],
            storage_options=do,
            schema=batch.schema,
            mode=mode,
            schema_mode="overwrite" if mode == "overwrite" else "merge",
            writer_properties=self.writer_properties,
            engine="rust",
        )

    def local_delta_table_exists(self, delta_path: Destination) -> bool:
        if not delta_path.exists():
            return False
        from deltalake import DeltaTable
        from deltalake.exceptions import TableNotFoundError

        try:
            dt = _delta_table(delta_path)
            DeltaTable.version(dt)
            return True
        except TableNotFoundError:
            return False

    def _handle_schema_drift(
        self,
        delta_path: Destination,
        allow_schema_drift: Union[bool, Literal["new_only"]],
        mode: Literal["overwrite", "append"],
        input_schema,
    ):
        from deltalake.exceptions import TableNotFoundError

        if allow_schema_drift == "new_only":
            try:
                dt = _delta_table(delta_path)
                existing_field_names = [f.name for f in dt.schema().fields]
                new_schema = _all_nullable(input_schema)
                if any(n not in existing_field_names for n in new_schema.names):
                    import pyarrow as pa

                    pas_schema = dt.schema().to_pyarrow()
                    return pa.schema(
                        [
                            pas_schema.field(n)
                            if n in existing_field_names
                            else input_schema.field(n)
                            for n in input_schema.names
                        ]
                    ), "merge"
            except TableNotFoundError:
                pass
            return None, None
        if allow_schema_drift:
            return None, "overwrite" if mode == "overwrite" else "merge"
        return None, None

    def _write_reader_to_delta(
        self,
        reader: "pa.RecordBatchReader",
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        allow_schema_drift: Union[bool, Literal["new_only"]],
        *,
        all_nullable: bool,
    ):
        """Writes a stream with the schema drift policy; an empty stream still creates the table on overwrite."""
        import pyarrow as pa
        from deltalake import write_deltalake
        from deltalake.exceptions import DeltaError

        dp, do = delta_path.as_path_options(flavor="object_store")
        cast_schema, schema_mode = self._handle_schema_drift(
            delta_path, allow_schema_drift, mode, reader.schema
        )
        if cast_schema:
            reader = pa.RecordBatchReader.from_batches(
                cast_schema, (b.cast(cast_schema) for b in reader)
            )
        try:
            write_deltalake(
                dp,
                reader,
                schema=_all_nullable(reader.schema) if all_nullable else None,
                mode=mode,
                writer_properties=self.writer_properties,
                schema_mode=schema_mode,
                engine="rust",
                storage_options=do,
            )
        except DeltaError as e:
            if "No data source supplied to write command" not in str(e):
                raise e
            if mode == "overwrite":
                self._write_empty_delta_table(reader.schema, dp, do)

    def _local_schema(self, schema: "pa.Schema") -> "Optional[pa.Schema]":
        """A schema to cast local query results to before writing them, None to keep it."""
        return None

    def local_execute_sql_to_delta(
        self,
        sql: ex.Query,
        delta_path: Destination,
        mode: Literal["overwrite", "append"],
        *,
        allow_schema_drift: Union[bool, Literal["new_only"]],
    ):
        import pyarrow as pa
        from odbc2deltalake.read_utils.duckdb_utils import duckdb_arrow_reader

        with self._duck().cursor() as cur:
            batch_reader = duckdb_arrow_reader(cur, sql.sql(self.query_dialect))
            fixed = self._local_schema(batch_reader.schema)
            if fixed is not None:
                batch_reader = pa.RecordBatchReader.from_batches(
                    fixed, (b.cast(fixed) for b in batch_reader)
                )
            self._write_reader_to_delta(
                batch_reader, delta_path, mode, allow_schema_drift, all_nullable=False
            )

    def _write_empty_delta_table(
        self,
        schema: "pa.Schema",
        path: Union[str, Path],
        storage_options: Optional[dict[str, str]],
    ):
        from deltalake import write_deltalake

        write_deltalake(
            path,
            [],
            schema=_all_nullable(schema),
            mode="overwrite",
            schema_mode="overwrite",
            engine="pyarrow",
            storage_options=storage_options,
        )

    def get_local_delta_ops(self, delta_path: Destination) -> DeltaOps:
        return DeltaRSDeltaOps(_delta_table(delta_path))

    def local_upsert_into(
        self,
        local_sql_source: ex.Query,
        target_delta: Destination,
        merge_cols: Sequence[str],
    ):
        from odbc2deltalake.read_utils.duckdb_utils import duckdb_arrow_reader

        with self._duck().cursor() as cursor:
            src_reader = duckdb_arrow_reader(
                cursor, local_sql_source.sql(self.query_dialect)
            )
            (
                _delta_table(target_delta)
                .merge(
                    src_reader,
                    sg.and_(
                        *[
                            sg.column(mc, "tgt", quoted=True).eq(
                                sg.column(mc, "src", quoted=True)
                            )
                            for mc in merge_cols
                        ]
                    ).sql(self.query_dialect),
                    "src",
                    "tgt",
                    writer_properties=self.writer_properties,
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
