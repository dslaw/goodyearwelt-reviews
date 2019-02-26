import pytest
import sqlite3


with open("src/sql/schema.sql") as fh:
    setup_sql = fh.read()

@pytest.fixture
def cursor():
    conn = sqlite3.connect(":memory:")
    conn.executescript(setup_sql)
    yield conn.cursor()
    conn.close()
