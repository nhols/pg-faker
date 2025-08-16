import logging
from typing import Any

from psycopg import Connection, sql

from pg_faker.strategies import Strategy

from .generate import Row, TableName, get_db
from .pg import ColName, get_schema
from .text_col_map import COL_NAME_STRATEGY_MAPPINGS

__all__ = [
    "run",
    "get_db",
    "insert_data",
    "COL_NAME_STRATEGY_MAPPINGS",
    "Row",
    "TableName",
    "ColName",
]


def insert_data(conn: Connection, data: dict[TableName, list[Row]]) -> None:
    for tbl, rows in data.items():
        logging.info(f"Inserting into {tbl} with {len(rows)} rows")
        with conn.cursor() as cursor:
            if rows:
                columns = ", ".join(rows[0].keys())
                columns = list(rows[0].keys())
                values = [tuple(row[col] for col in columns) for row in rows]
                sql_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(*tbl.split(".")),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )
                cursor.executemany(sql_query, values)


def run(
    conn: Connection,
    row_counts: dict[TableName, int] | None = None,
    tbl_override_strategies: dict[TableName, dict[ColName, Strategy[Any, Any]]] | None = None,
    col_name_strategy_mappings: dict[tuple[str, ...], Strategy[str, Any]] | None = None,
) -> None:
    schema = get_schema(conn)
    # TODO load existing data from the database and pass here
    data = get_db(
        schema,
        row_counts=row_counts,
        tbl_override_strategies=tbl_override_strategies,
        col_name_strategy_mappings=col_name_strategy_mappings,
    )
    for tbl, rows in data.items():
        logging.info(f"Inserting into {tbl} with {len(rows)} rows")
        with conn.cursor() as cursor:
            if rows:
                columns = ", ".join(rows[0].keys())
                columns = list(rows[0].keys())
                values = [tuple(row[col] for col in columns) for row in rows]
                sql_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(*tbl.split(".")),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )
                cursor.executemany(sql_query, values)
