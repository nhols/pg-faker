
from psycopg import Connection, sql
from pg_faker import run

import pytest
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

@pytest.mark.parametrize("schema", [SIMPLE_SCHEMA, FK_SCHEMA, MULTI_FK_SCHEMA,])
def test_run(conn:Connection, schema:sql.SQL):
    conn.execute(schema)
    run(conn)