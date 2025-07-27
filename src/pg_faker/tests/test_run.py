import pathlib
from datetime import date

import pytest
from psycopg import Connection, sql

from pg_faker import run

SCHEMA_DIR = pathlib.Path(__file__).parent / "schemas"


def read_schema(filename: str) -> sql.SQL:
    with open(SCHEMA_DIR / filename, "r") as f:
        return sql.SQL(f.read())  # type: ignore


SCHEMA_PARAMS = [
    pytest.param(read_schema(fname), id=fname.rsplit(".", 1)[0])
    for fname in (
        "simple.sql",
        "fk.sql",
        "multi_fk.sql",
        "readme.sql",
        "keyword.sql",
        "all_nullable.sql",
        "composite_pk.sql",
    )
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


@pytest.mark.parametrize(
    "data_type",
    [
        "bigint",
        "bigserial",
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
        "bit(4)",
        "bit varying(8)",
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
    ],
)
def test_run_all_types_xfail(conn: Connection, data_type: str):
    pytest.xfail(f"{data_type} is expected to fail or is not yet supported.")
    query = f"CREATE TABLE test (mycol {data_type})"
    conn.execute(query)  # type: ignore
    run(conn)
