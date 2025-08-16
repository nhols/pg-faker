from collections.abc import Generator

import psycopg
import pytest


@pytest.fixture(scope="function")
def conn() -> Generator[psycopg.Connection, None, None]:
    with psycopg.connect("user=postgres password=postgres host=localhost port=5432") as conn:
        yield conn
        conn.rollback()
