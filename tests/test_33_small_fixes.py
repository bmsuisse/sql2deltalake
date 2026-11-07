"""Unit tests without a database for small fixes."""

from typing import Any, List, Tuple


class _RecordingSource:
    """Collects what DeltaStorageBackend.flush writes instead of writing Delta."""

    def __init__(self) -> None:
        self.writes: List[Tuple[Any, List[str]]] = []

    def local_pylist_to_delta(self, rows, path, mode, dummy_record=None):
        self.writes.append((path, [r["message"] for r in rows]))


def test_loggers_keep_their_own_pending_messages():
    from odbc2deltalake.delta_logger import DeltaLogger

    source = _RecordingSource()
    a = DeltaLogger("log_a", source)  # type: ignore[arg-type]
    b = DeltaLogger("log_b", source)  # type: ignore[arg-type]
    for i in range(3):
        a.info(f"a{i}")
        b.info(f"b{i}")
    a.storage_backend.flush()  # type: ignore[union-attr]
    assert source.writes == [("log_a", ["a0", "a1", "a2"])]
    b.storage_backend.flush()  # type: ignore[union-attr]
    assert source.writes[1] == ("log_b", ["b0", "b1", "b2"])
