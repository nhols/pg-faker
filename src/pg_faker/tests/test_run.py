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

ALL_TYPES_SCHEMA = """CREATE TABLE all_data_types (
    --col_bigint             bigint,
    --col_bigserial          bigserial,
    --col_bit                bit(4),
    --col_bit_varying        bit varying(8),
    col_boolean            boolean,
    --col_bytea              bytea,
    col_char               character(10),
    col_varchar            character varying(50),
    --col_cidr               cidr,
    col_date               date,
    col_double_precision   double precision,
    --col_inet               inet,
    --col_integer            integer,
    --col_interval           interval,
    col_json               json,
    col_jsonb              jsonb,
    --col_macaddr            macaddr,
    --col_macaddr8           macaddr8,
    col_money              money,
    col_numeric            numeric(10, 2),
    --col_pg_lsn             pg_lsn,
    --col_pg_snapshot        pg_snapshot,
    col_real               real,
    --col_smallint           smallint,
    --col_smallserial        smallserial,
    --col_serial             serial,
    col_text               text,
    col_time               time without time zone,
    col_timetz             time with time zone,
    col_timestamp          timestamp without time zone,
    col_timestamptz        timestamp with time zone,
    --col_tsquery            tsquery,
    --col_tsvector           tsvector,
    --col_txid_snapshot      txid_snapshot,
    col_uuid               uuid,
    col_xml                xml
);
"""

GEOMETRY_TYPES_SCHEMA = """CREATE TABLE geometry_types (
    col_box                box,
    col_circle             circle,
    col_line               line,
    col_lseg               lseg,
    col_path               path,
    col_point              point,
    col_polygon            polygon
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
