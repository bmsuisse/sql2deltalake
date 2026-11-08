"""Connection string helpers for the executors of the Spark data source: no imports beyond the
standard library and ``_core``, so a fresh Python worker starts quickly."""


def _conn_str_segments(connection_string: str) -> "list[str]":
    """Splits at the `;` between `key=value` pairs, not at one inside a `{...}` value (where `}}` is a `}`)."""
    cs = connection_string
    n = len(cs)
    segments: "list[str]" = []
    i = 0
    while True:
        eq = cs.find("=", i)
        semi = cs.find(";", i)
        j = i if eq == -1 or (semi != -1 and semi < eq) else eq + 1
        while j < n and cs[j] in " \t":
            j += 1
        if j > i and j < n and cs[j] == "{":
            j += 1
            while j < n:
                if cs[j] == "}":
                    if cs[j + 1 : j + 2] == "}":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
        end = cs.find(";", j)
        if end == -1:
            segments.append(cs[i:])
            return segments
        segments.append(cs[i:end])
        i = end + 1


def strip_odbc_driver(connection_string: str) -> str:
    """arrowsql speaks TDS itself: a `Driver=` keyword left over from an ODBC connection string is dropped."""
    return ";".join(
        s
        for s in _conn_str_segments(connection_string)
        if s.split("=", 1)[0].strip().lower() != "driver" or "=" not in s
    )


def core_reads(connection_string: str) -> bool:
    """Whether the source is read through arrowsql's Rust core (built into `_core`) instead of
    arrowsql's Python package: by default, unless `SQL2DELTA_ARROWSQL_CORE=0` or the connection
    string logs in with `Authentication=` (Entra ID tokens come from arrowsql's Python side)."""
    import os

    from sql2deltalake import _core

    if not hasattr(_core, "read_sql") or os.getenv("SQL2DELTA_ARROWSQL_CORE") == "0":
        return False
    return not any(
        s.split("=", 1)[0].strip().lower() == "authentication"
        for s in _conn_str_segments(connection_string)
    )
