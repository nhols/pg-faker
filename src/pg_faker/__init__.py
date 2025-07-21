import logging

from psycopg import Connection, sql

from .generate import Row, TableName, get_db
from .pg import get_schema


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


def run(conn: Connection, row_counts: dict[TableName, int] | None = None) -> None:
    schema = get_schema(conn)
    data = get_db(schema, row_counts)
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
