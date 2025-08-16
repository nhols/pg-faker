import logging
from collections import defaultdict, deque
from typing import Any, TypedDict

from psycopg import Connection

logger = logging.getLogger(__name__)


TableName = str
ColName = str

Row = dict[ColName, Any]
Table = list[Row]

RowCounts = dict[TableName, int]


class ColInfo(TypedDict):
    col_name: str
    pgtype: str
    nullable: bool
    character_maximum_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    enum_values: list[str] | None


class FkConstraint(TypedDict):
    local_table: TableName
    foreign_table: TableName
    local_foreign_mapping: dict[ColName, ColName]


UniqueConstraint = tuple[ColName, ...]


class TableInfo(TypedDict):
    table: TableName
    columns: dict[ColName, ColInfo]
    unique_constraints: list[UniqueConstraint]
    fk_constraints: list[FkConstraint]


_COL_INFO_QUERY = """
WITH user_enums AS (
  SELECT
    n.nspname      AS enum_schema,
    t.typname      AS enum_name,
    array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
  FROM pg_type t
  JOIN pg_enum e
    ON t.oid = e.enumtypid
  JOIN pg_namespace n
    ON n.oid = t.typnamespace
  WHERE t.typtype = 'e'
    AND n.nspname NOT IN ('pg_catalog','information_schema')
  GROUP BY 1,2
),
cols AS (
  SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    c.udt_name,
    c.is_nullable,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale
  FROM information_schema.columns c
  JOIN pg_tables t
    ON c.table_schema = t.schemaname
    AND c.table_name = t.tablename
  WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
)
SELECT
  c.table_schema,
  c.table_name,
  c.column_name,
  c.udt_name,
  c.is_nullable,
  c.character_maximum_length,
  c.numeric_precision,
  c.numeric_scale,
  ue.enum_values
FROM cols c
LEFT JOIN user_enums ue
  ON c.table_schema = ue.enum_schema
  AND c.udt_name     = ue.enum_name
ORDER BY
  c.table_schema,
  c.table_name,
  c.column_name;
"""


def get_col_info(conn: Connection) -> dict[TableName, dict[ColName, ColInfo]]:
    """
    Get column information for all user tables in the database.
    Returns a dictionary where keys are 'schema_name.table_name' and values are dictionaries of column names
    and their types.
    """
    with conn.cursor() as cursor:
        cursor.execute(_COL_INFO_QUERY)
        col_info = {}
        for (
            schema_name,
            table_name,
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            enum_values,
        ) in cursor.fetchall():
            full_table_name = f"{schema_name}.{table_name}"
            if full_table_name not in col_info:
                col_info[full_table_name] = {}
            col_info[full_table_name][column_name] = {
                "col_name": column_name,
                "pgtype": data_type,
                "nullable": is_nullable == "YES",
                "character_maximum_length": character_maximum_length,
                "numeric_precision": numeric_precision,
                "numeric_scale": numeric_scale,
                "enum_values": enum_values,
            }
    return col_info


_UNIQUE_CONSTRAINTS_QUERY = """
SELECT
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    kcu.ordinal_position
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
    AND tc.table_name = kcu.table_name
WHERE tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
  AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position;
"""


def get_unique_constraints(conn: Connection) -> dict[TableName, list[tuple[ColName, ...]]]:
    """
    Get unique constraints for all user tables in the database.
    Returns a dictionary where keys are table names and values are lists of tuples representing unique constraints.
    Handles composite unique constraints.
    """
    with conn.cursor() as cursor:
        cursor.execute(_UNIQUE_CONSTRAINTS_QUERY)
        constraints = {}
        for row in cursor.fetchall():
            schema_name, table_name, constraint_name, column_name, ordinal_position = row
            full_table_name = f"{schema_name}.{table_name}"
            if full_table_name not in constraints:
                constraints[full_table_name] = {}
            if constraint_name not in constraints[full_table_name]:
                constraints[full_table_name][constraint_name] = []
            constraints[full_table_name][constraint_name].append(column_name)

    return {tbl: [tuple(keys) for keys in cons.values()] for tbl, cons in constraints.items()}


_FK_CONSTRAINTS_QUERY = """
SELECT
    nsp.nspname            AS table_schema,
    cl.relname             AS local_table_name,
    fnsp.nspname           AS foreign_table_schema,
    fcl.relname            AS foreign_table_name,
    con.conname            AS constraint_name,
    local_col.attname      AS local_column_name,
    foreign_col.attname    AS foreign_column_name
FROM
    pg_constraint AS con
    JOIN pg_class AS cl
      ON cl.oid = con.conrelid
    JOIN pg_namespace AS nsp
      ON nsp.oid = cl.relnamespace
    JOIN pg_class AS fcl
      ON fcl.oid = con.confrelid
    JOIN pg_namespace AS fnsp
      ON fnsp.oid = fcl.relnamespace

    -- explode the array of local key columns, keeping their position
    JOIN LATERAL unnest(con.conkey)
        WITH ORDINALITY AS src_local(colnum, ord)
      ON TRUE
    JOIN pg_attribute AS local_col
      ON local_col.attrelid = con.conrelid
     AND local_col.attnum   = src_local.colnum

    -- explode the array of foreign key columns, matching by ordinality
    JOIN LATERAL unnest(con.confkey)
        WITH ORDINALITY AS src_foreign(colnum, ord)
      ON src_foreign.ord = src_local.ord
    JOIN pg_attribute AS foreign_col
      ON foreign_col.attrelid = con.confrelid
     AND foreign_col.attnum  = src_foreign.colnum

WHERE
    con.contype = 'f'            -- only foreign keys
ORDER BY
    table_schema,
    local_table_name,
    constraint_name,
    src_local.ord;
"""


def get_fk_constraints(conn: Connection) -> dict[TableName, list[FkConstraint]]:
    """
    Get foreign key constraints for all user tables in the database.
    Returns a list of dictionaries with local and foreign table names and their column mappings.
    """
    with conn.cursor() as cursor:
        cursor.execute(_FK_CONSTRAINTS_QUERY)
        fk_constraints = {}
        for row in cursor.fetchall():
            (
                table_schema,
                local_table_name,
                foreign_table_schema,
                foreign_table_name,
                constraint_name,
                local_column_name,
                foreign_column_name,
            ) = row
            local_table = f"{table_schema}.{local_table_name}"
            foreign_table = f"{foreign_table_schema}.{foreign_table_name}"
            if local_table not in fk_constraints:
                fk_constraints[local_table] = {}
            if constraint_name not in fk_constraints[local_table]:
                fk_constraints[local_table][constraint_name] = {
                    "local_table": local_table,
                    "foreign_table": foreign_table,
                    "local_foreign_mapping": {},
                }
            fk_constraints[local_table][constraint_name]["local_foreign_mapping"][local_column_name] = (
                foreign_column_name
            )
        return {tbl: list(fkcs.values()) for tbl, fkcs in fk_constraints.items()}


def get_schema(conn: Connection) -> list[TableInfo]:
    col_infos = get_col_info(conn)
    unique_constraints = get_unique_constraints(conn)
    fk_constraints = get_fk_constraints(conn)
    return [
        TableInfo(
            table=tbl,
            columns=cols,
            unique_constraints=unique_constraints.get(tbl, []),
            fk_constraints=fk_constraints.get(tbl, []),
        )
        for tbl, cols in col_infos.items()
    ]


def topo_sort_tables(
    fk_constraints: dict[TableName, list[FkConstraint]], all_tables: list[TableName]
) -> list[TableName]:
    """
    Topologically sort tables based on foreign key constraints.
    Returns a list of table names in the order they should be processed.
    """
    graph = defaultdict(set)
    indegree = defaultdict(int)

    for local_table, constraints in fk_constraints.items():
        for constraint in constraints:
            foreign_table = constraint["foreign_table"]
            graph[foreign_table].add(local_table)
            indegree[local_table] += 1

    queue = deque(tbl for tbl in graph if indegree[tbl] == 0)
    sorted_tables = []

    while queue:
        table = queue.popleft()
        sorted_tables.append(table)
        for dependent in graph[table]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                queue.append(dependent)

    if len(sorted_tables) != len(graph):
        raise ValueError("Cycle detected in foreign key constraints")
    missing_tables = set(all_tables) - set(sorted_tables)
    return sorted_tables + list(missing_tables)
