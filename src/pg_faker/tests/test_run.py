import pytest
from psycopg import Connection, sql

from pg_faker import run

SIMPLE_SCHEMA = """
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
"""

FK_SCHEMA = """CREATE TABLE parent (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);
CREATE TABLE child (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER REFERENCES parent(id),
    name VARCHAR(100)
);
"""

MULTI_FK_SCHEMA = """CREATE TABLE parent (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);
CREATE TABLE child (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER REFERENCES parent(id),
    name VARCHAR(100)
);
CREATE TABLE grandchild (
    id SERIAL PRIMARY KEY,
    child_id INTEGER REFERENCES child(id),
    name VARCHAR(100)
);
"""


@pytest.mark.parametrize(
    "schema",
    [
        SIMPLE_SCHEMA,
        FK_SCHEMA,
        MULTI_FK_SCHEMA,
    ],
)
def test_run(conn: Connection, schema: sql.SQL):
    conn.execute(schema)
    run(conn)


@pytest.mark.parametrize(
    "data_type",
    [
        "bigint",
        "bigserial",
        # "bit(4)",
        # "bit varying(8)",
        "boolean",
        # "bytea",
        "character(10)",
        "character varying(50)",
        # "cidr",
        "date",
        "double precision",
        # "inet",
        "integer",
        # "interval",
        "json",
        "jsonb",
        # "macaddr",
        # "macaddr8",
        # "money",
        "numeric(10, 2)",
        # "pg_lsn",
        # "pg_snapshot",
        "real",
        "smallint",
        "smallserial",
        "serial",
        "text",
        "time without time zone",
        "time with time zone",
        "timestamp without time zone",
        "timestamp with time zone",
        # "tsquery",
        # "tsvector",
        # "txid_snapshot",
        "uuid",
        "xml",
        # "box",
        # "circle",
        # "line",
        # "lseg",
        # "path",
        # "point",
        # "polygon",
    ],
)
def test_run_all_types(conn: Connection, data_type: str):
    schema = f"CREATE TABLE test (mycol {data_type})"
    conn.execute(schema)
    run(conn)
