from pkgutil import get_data
import sqlite3


_rollups_sql = get_data("src", "sql/views-rollups.sql")


def create_views(cursor: sqlite3.Cursor) -> None:
    if _rollups_sql is None:
        raise RuntimeError("Failed to load SQL")

    cursor.executescript(_rollups_sql.decode())
    return
