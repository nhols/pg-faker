from datetime import date

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
README_SCHEMA = """
CREATE TABLE "user" (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE post (
    id SERIAL PRIMARY KEY,
    author_id INTEGER NOT NULL REFERENCES "user"(id),
    title VARCHAR(255) NOT NULL,
    content TEXT,
    published_at TIMESTAMPTZ,
    UNIQUE (author_id, title)
);
"""


@pytest.mark.parametrize(
    "schema",
    [SIMPLE_SCHEMA, FK_SCHEMA, MULTI_FK_SCHEMA, README_SCHEMA],
)
def test_run(conn: Connection, schema: sql.SQL):
    conn.execute(schema)
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
