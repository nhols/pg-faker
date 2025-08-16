import pathlib

import pytest
from psycopg import Connection, sql

from pg_faker import run
from pg_faker.strategies import int_strategy


def get_postgres_version(conn: Connection) -> int:
    """Get the major PostgreSQL version number."""
    result = conn.execute("SELECT version()").fetchone()
    version_str = result[0]  # type: ignore
    # Extract major version from string like "PostgreSQL 12.x ..."
    version_parts = version_str.split()[1].split(".")
    return int(version_parts[0])


SCHEMA_DIR = pathlib.Path(__file__).parent / "schemas"


def read_schema(filename: str) -> sql.SQL:
    with open(SCHEMA_DIR / filename, "r") as f:
        return sql.SQL(f.read())  # type: ignore


NOT_SUPPORTED = {"circular_fk", "array"}
SCHEMA_PARAMS = [
    pytest.param(f.read_text(), id=f.stem)
    for f in SCHEMA_DIR.glob("*.sql")
    if f.is_file() and f.stem not in NOT_SUPPORTED
]


@pytest.mark.parametrize("schema", SCHEMA_PARAMS)
def test_run(conn: Connection, schema: sql.SQL):
    conn.execute(schema)
    run(conn)


def test_circular_fk(conn: Connection):
    schema = read_schema("circular_fk.sql")
    conn.execute(schema)
    with pytest.raises(ValueError, match="Cycle detected in foreign key constraints"):
        run(conn)


def test_array(conn: Connection):
    schema = read_schema("array.sql")
    conn.execute(schema)
    with pytest.raises(ValueError, match="Unsupported pgtype:"):
        run(conn)


@pytest.mark.parametrize(
    "data_type",
    [
        "bigint",
        "bigserial",
        "bit(3)",
        "bit varying(5)",
        "boolean",
        "character(10)",
        "character varying(50)",
        "date",
        "double precision",
        "integer",
        "json",
        "jsonb",
        "numeric(10, 2)",
        "real",
        "smallint",
        "smallserial",
        "serial",
        "text",
        "time without time zone",
        "time with time zone",
        "timestamp without time zone",
        "timestamp with time zone",
        "uuid",
        "xml",
    ],
)
def test_run_all_types(conn: Connection, data_type: str):
    query = f"CREATE TABLE test (mycol {data_type})"
    conn.execute(query)  # type: ignore
    run(conn)


@pytest.mark.parametrize(
    "data_type",
    [
        "bytea",
        "cidr",
        "inet",
        "interval",
        "macaddr",
        "macaddr8",
        "money",
        "pg_lsn",
        "pg_snapshot",
        "tsquery",
        "tsvector",
        "txid_snapshot",
        "box",
        "circle",
        "line",
        "lseg",
        "path",
        "point",
        "polygon",
        "tsrange",
        "int[]",
    ],
)
def test_run_all_types_xfail(conn: Connection, data_type: str):
    query = f"CREATE TABLE test (mycol {data_type})"
    conn.execute(query)  # type: ignore

    # Special handling for pg_snapshot in PostgreSQL 12
    if data_type == "pg_snapshot" and get_postgres_version(conn) == 12:
        pytest.xfail("pg_snapshot is expected to fail in PostgreSQL 12")

    with pytest.raises(ValueError, match="Unsupported pgtype:"):
        run(conn)


def test_run_readme_eg_check_constraint(conn: Connection):
    query = "CREATE TABLE person (id SERIAL PRIMARY KEY, name TEXT NOT NULL, age INTEGER NOT NULL CHECK (age >= 18));"
    conn.execute(query)
    override_strategies = {
        "public.person": {
            "age": int_strategy(min_value=18),
        }
    }

    run(
        conn,
        tbl_override_strategies=override_strategies,
    )
