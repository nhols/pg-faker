import logging
from collections import defaultdict, deque
from collections.abc import Callable, Generator, Hashable, Iterable, Sequence
from typing import Any, ParamSpec, TypedDict

from .gen import (
    Strategy,
    bool_strategy,
    char_strategy,
    date_strategy,
    dict_strategy,
    fixed_strategy,
    int_strategy,
    json_strategy,
    list_strategy,
    nullable,
    numeric_strategy,
    one_of,
    timestamp_strategy,
    uuid_strategy,
)
from .col_name_strat_map import COL_NAME_STRATEGY_MAPPINGS
from psycopg import Connection

logger = logging.getLogger(__name__)

MIN_ROWS = 10
MAX_ROWS = 1000
MAX_JOIN_ROWS = 100_000

P = ParamSpec("P")

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
    Returns a dictionary where keys are 'schema_name.table_name' and values are dictionaries of column names and their types.
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


def col_info_to_strategy(col_info: ColInfo) -> Strategy[Any, Any]:
    pgtype = col_info["pgtype"]
    if pgtype == "uuid":
        strat = uuid_strategy()
    elif pgtype == "date":
        strat = date_strategy()
    elif pgtype in ("timestamptz", "timestamp"):
        strat = timestamp_strategy()
    elif pgtype in ("varchar", "text"):
        mapped_strat = None
        if not col_info["character_maximum_length"]:
            for words, map_strat in COL_NAME_STRATEGY_MAPPINGS.items():
                if all(word in col_info["col_name"] for word in words):
                    mapped_strat = map_strat
                    break
        strat = mapped_strat or char_strategy(col_info["character_maximum_length"] or None)
    elif pgtype == "numeric" or pgtype.startswith("float"):
        strat = numeric_strategy(col_info["numeric_precision"] or 53, col_info["numeric_scale"] or 0)
    elif pgtype == "bool":
        strat = bool_strategy()
    elif pgtype in ("int2", "int4", "int8"):
        strat = int_strategy(col_info["numeric_precision"] or 32)
    elif pgtype in ("json", "jsonb"):
        strat = json_strategy()
    elif col_info["enum_values"]:
        strat = one_of(col_info["enum_values"])
    else:
        raise ValueError(f"Unsupported pgtype: {pgtype}")
    strat: Strategy[Any, Any]
    if col_info["nullable"]:
        return nullable(strat)
    return strat


def select(row: Row, cols: set[ColName]) -> Row:
    """
    Select only the specified columns from a row.
    """
    return {col: row[col] for col in cols if col in row}


def rename(row: Row, col_mapping: dict[ColName, ColName]) -> Row:
    """
    Rename columns in a row based on the provided mapping.
    """
    return {col_mapping.get(col, col): value for col, value in row.items()}


def select_values(row: Row, cols: set[ColName]) -> tuple[Any]:
    """
    Select only the specified columns from a row and return their values.
    """
    return tuple(row[col] for col in cols if col in row)


def inner_join(rows1: Iterable[Row], rows2: Iterable[Row], on_cols: set[ColName]) -> Generator[Row, None, None]:
    for row1 in rows1:
        for row2 in rows2:
            if select_values(row1, on_cols) == select_values(row2, on_cols):
                yield {**row1, **row2}


def cross_join(rows1: Iterable[Row], rows2: Iterable[Row]) -> Generator[Row, None, None]:
    for row1 in rows1:
        for row2 in rows2:
            yield {**row1, **row2}


def get_fk_constrained_options(
    fk_constraints: list[FkConstraint], data: dict[TableName, list[Row]], max_rows: int = MAX_ROWS
) -> tuple[set[ColName], Strategy[Row, [list[Row]]] | None]:
    seen_cols = set()
    constrained_rows = []
    for fk in fk_constraints:
        foreign_table = fk["foreign_table"]
        local_cols = set(fk["local_foreign_mapping"].keys())
        foreign_cols = set(fk["local_foreign_mapping"].values())
        rows = [select(row, foreign_cols) for row in data.get(foreign_table, [])]
        if not rows:
            return {col for fk in fk_constraints for col in fk["local_foreign_mapping"].keys()}, None
        col_map = {v: k for k, v in fk["local_foreign_mapping"].items()}
        rows = [rename(row, col_map) for row in rows]
        overlap_cols = local_cols.intersection(seen_cols)
        seen_cols.update(local_cols)

        if overlap_cols:
            constrained_rows = inner_join(constrained_rows, rows, on_cols=overlap_cols)
        else:
            constrained_rows = cross_join(constrained_rows, rows)
    sampled_constrained_rows: list[Row] = []
    for row in constrained_rows:
        if len(sampled_constrained_rows) < max_rows:
            sampled_constrained_rows.append(row)
        else:
            break
    # TODO allow nulls on all nullable columns
    return seen_cols, one_of(sampled_constrained_rows) if sampled_constrained_rows else None


def get_row(
    col_infos: dict[ColName, ColInfo],
    fk_constraints: list[FkConstraint],
    data: dict[TableName, list[Row]],
    override_strategies: dict[ColName, Strategy[Any, Any]] | None = None,
) -> (
    Strategy[
        Row,
        [
            dict[str, Strategy[Any, Any]],
            Sequence[Strategy[dict[str, Any], Any]] | None,
        ],
    ]
    | None
):
    override_strategies = override_strategies or {}
    fk_constrained_cols, fk_strat = get_fk_constrained_options(fk_constraints, data)
    if fk_constrained_cols and fk_strat is None:
        logger.info(f"No values found for foreign key constrained columns: {fk_constrained_cols}")
        if not all(col_infos[col]["nullable"] for col in fk_constrained_cols):
            logger.warning(f"Some foreign key constrained columns are not nullable, zero rows will be generated")
            return None
        fk_strat = one_of([{col: None} for col in fk_constrained_cols])
    if ovlp := fk_constrained_cols.intersection(override_strategies.keys()):
        logger.warning(f"Override strategy for foreign key constrained columns will be ignored: {ovlp}")
    strategies = {
        col_name: override_strategies.get(col_name) or col_info_to_strategy(col_info)
        for col_name, col_info in col_infos.items()
        if col_name not in fk_constrained_cols
    }
    return dict_strategy(strategies, others=[fk_strat] if fk_strat else None)


def get_table(
    table_info: TableInfo,
    data: dict[TableName, list[Row]],
    row_count: int | None,
) -> (
    Strategy[
        list[Row],
        [
            Strategy[
                Row,
                [
                    dict[str, Strategy[Any, Any]],
                    Sequence[Strategy[dict[str, Any], Any]] | None,
                ],
            ],
            int,
            int,
            Sequence[Callable[[Row], Hashable]] | None,
            int,
        ],
    ]
    | Strategy[list, [list]]
):
    row_strategy = get_row(table_info["columns"], table_info["fk_constraints"], data)
    if row_strategy is None:
        logger.warning(f"No row strategy generated for table {table_info['table']}, returning empty list strategy")
        return fixed_strategy([])
    unique_bys = tuple(lambda row, uc=uc: tuple(row[col] for col in uc) for uc in table_info["unique_constraints"])
    return list_strategy(
        row_strategy,
        min_length=row_count if row_count is not None else MIN_ROWS,
        max_length=row_count if row_count is not None else MAX_ROWS,
        unique_by=unique_bys,
    )


def get_db(schema: list[TableInfo], row_counts: RowCounts | None = None) -> dict[TableName, list[Row]]:
    logger.info("Generating database schema")
    strats: dict[TableName, Strategy[list[Row], Any]] = {}
    data: dict[TableName, list[Row]] = {}
    schema_ = {tbl_info["table"]: tbl_info for tbl_info in schema}
    sorted_tbls = topo_sort_tables(
        {tbl_info["table"]: tbl_info["fk_constraints"] for tbl_info in schema},
        [tbl_info["table"] for tbl_info in schema],
    )
    for tbl in sorted_tbls:
        logger.info(f"Processing table: {tbl}")
        table_strat = get_table(
            schema_[tbl],
            data,
            row_counts.get(tbl) if row_counts else None,
        )
        strats[tbl] = table_strat
        data[tbl] = table_strat.gen()
        logger.info(f"Generated {len(data[tbl])} rows for table {tbl}")
    return data
