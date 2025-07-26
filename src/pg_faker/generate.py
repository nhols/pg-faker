import logging
import time
from collections.abc import Callable, Generator, Hashable, Iterable
from typing import Any

from .pg import (
    ColInfo,
    ColName,
    FkConstraint,
    Row,
    RowCounts,
    TableInfo,
    TableName,
    topo_sort_tables,
)
from .strategies import (
    Strategy,
    UnenforceableUniqueConstraintError,
    binary_strategy,
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
    strategy_wrapper,
    time_strategy,
    timestamp_strategy,
    uuid_strategy,
    xml_strategy,
)
from .text_col_map import COL_NAME_STRATEGY_MAPPINGS

logger = logging.getLogger(__name__)

MIN_ROWS = 10
MAX_ROWS = 1000
MAX_JOIN_ROWS = 100_000


def col_info_to_strategy(col_info: ColInfo) -> Strategy[Any, Any]:
    pgtype = col_info["pgtype"]
    if pgtype == "uuid":
        strat = uuid_strategy(cast_to=None)
    elif pgtype == "date":
        strat = date_strategy()
    elif pgtype in ("timestamptz", "timestamp"):
        strat = timestamp_strategy()
    elif pgtype in ("time", "timetz"):
        strat = time_strategy()
    elif pgtype in ("varchar", "text", "bpchar"):
        mapped_strat = None
        if not col_info["character_maximum_length"]:
            # TODO make this configurable
            for words, map_strat in COL_NAME_STRATEGY_MAPPINGS.items():
                if all(word in col_info["col_name"] for word in words):
                    mapped_strat = map_strat
                    break
        strat = mapped_strat or (
            char_strategy(max_chars=col_info["character_maximum_length"])
            if col_info["character_maximum_length"]
            else char_strategy()
        )
    elif pgtype in ("numeric", "money") or pgtype.startswith("float"):
        precision = col_info["numeric_precision"] or 53
        scale = col_info["numeric_scale"] or 0
        strat = numeric_strategy(left_digits=precision - scale, right_digits=scale)
    elif pgtype == "bool":
        strat = bool_strategy()
    elif pgtype in ("int2", "int4", "int8"):
        precision = col_info["numeric_precision"] or 32
        max_value: int = 2 ** (precision - 1) - 1
        min_value = -1 * max_value - 1
        strat = int_strategy(min_value=min_value, max_value=max_value)
    elif pgtype in ("json", "jsonb"):
        strat = json_strategy()
    elif col_info["enum_values"]:
        strat = one_of(col_info["enum_values"])
    elif pgtype in ("bit", "varbit"):
        strat = (
            binary_strategy(length=col_info["character_maximum_length"])
            if col_info["character_maximum_length"]
            else binary_strategy()
        )
    elif pgtype == "xml":
        strat = xml_strategy()
    else:
        raise ValueError(f"Unsupported pgtype: {pgtype}, col_info: {col_info}")
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
    fk_constraints: list[FkConstraint],
    data: dict[TableName, list[Row]],
    max_rows: int = MAX_ROWS,
) -> tuple[set[ColName], Strategy[Row, [list[Row]]] | None]:
    # TODO break fk_constraints into connected subgraphs to decrease combinatorial space of sampled_constrained_rows
    seen_cols = set()
    first_loop = True
    constrained_rows = []
    for fk in fk_constraints:
        foreign_table = fk["foreign_table"]
        local_cols = set(fk["local_foreign_mapping"].keys())
        foreign_cols = set(fk["local_foreign_mapping"].values())
        # TODO randomise order of foreign table rows to avoid bias towards the first rows if `max_rows` is hit`
        rows = [select(row, foreign_cols) for row in data.get(foreign_table, [])]
        # NULL != NULL A row in the foreign table must have all foreign key columns not None to be referencable
        rows = [row for row in rows if all(value is not None for value in row.values())]
        if not rows:
            return {col for fk in fk_constraints for col in fk["local_foreign_mapping"].keys()}, None
        col_map = {v: k for k, v in fk["local_foreign_mapping"].items()}
        rows = [rename(row, col_map) for row in rows]
        overlap_cols = local_cols.intersection(seen_cols)
        seen_cols.update(local_cols)
        if first_loop:
            constrained_rows = rows
            first_loop = False
            continue
        if overlap_cols:
            constrained_rows = inner_join(constrained_rows, rows, on_cols=overlap_cols)
        else:
            constrained_rows = cross_join(constrained_rows, rows)
    sampled_constrained_rows: list[Row] = []
    for row in constrained_rows:
        if len(sampled_constrained_rows) < max_rows:
            sampled_constrained_rows.append(row)
        else:
            logger.warning(
                f"Combinatorial space of foreign key constrained rows has reached max of {max_rows}, stopping sampling"
            )
            break
    return seen_cols, one_of(sampled_constrained_rows) if sampled_constrained_rows else None


class UnSatisfiableFkConstraintError(Exception):
    """
    Raised when a foreign key constraint cannot be satisfied due to lack of data in the foreign table.
    """


@strategy_wrapper
def get_row(
    col_infos: dict[ColName, ColInfo],
    fk_constraints: list[FkConstraint],
    data: dict[TableName, list[Row]],
    override_strategies: dict[ColName, Strategy[Any, Any]] | None = None,
) -> Row:
    override_strategies = override_strategies or {}

    fk_constrained_cols = {col for fk in fk_constraints for col in fk["local_foreign_mapping"].keys()}
    null_fk_col_strats: list[Strategy[Row, [Row]]] = []
    null_fk_col_names: set[str] = set()
    for fk_col in fk_constrained_cols:
        strat = override_strategies.get(fk_col) or col_info_to_strategy(col_infos[fk_col])
        if strat.gen() is None:
            null_fk_col_strats.append(fixed_strategy({fk_col: None}))
            null_fk_col_names.add(fk_col)
    # NULL != NULL in SQL: If an FK constrained col value is NULL, that fk constraint is not enforced on that row
    enforceable_fk_constraints = [
        fk_constraint
        for fk_constraint in fk_constraints
        if not null_fk_col_names.intersection(fk_constraint["local_foreign_mapping"].keys())
    ]
    fk_constrained_cols, fk_strat = get_fk_constrained_options(enforceable_fk_constraints, data)
    if fk_constrained_cols and fk_strat is None:
        msg = f"No values found for foreign key constrained columns: {fk_constrained_cols}"
        raise UnSatisfiableFkConstraintError(msg)

    if ovlp := fk_constrained_cols.intersection(override_strategies.keys()):
        logger.warning(f"Override strategy for foreign key constrained columns will be ignored: {ovlp}")
    already_handled_cols = null_fk_col_names.union(fk_constrained_cols)
    strategies = {
        col_name: override_strategies.get(col_name) or col_info_to_strategy(col_info)
        for col_name, col_info in col_infos.items()
        if col_name not in already_handled_cols
    }
    others = list(null_fk_col_strats)
    if fk_strat is not None:
        others = others + [fk_strat]
    return dict_strategy(strategies, others=others or None).gen()


def get_uc_hasher(uc: tuple[ColName, ...]) -> Callable[[Row], Hashable]:
    """
    Returns a function that hashes a row based on the columns in the unique constraint.
    """

    def row_hasher(row: Row) -> Hashable:
        hash = tuple(row[col] for col in uc if col in row)
        if any(value is None for value in hash):
            raise UnenforceableUniqueConstraintError(
                f"Row {row} has `NULL` values in unique constraint columns {uc}, cannot enforce unique constraint"
            )
        return hash

    return row_hasher


def get_table(
    table_info: TableInfo,
    data: dict[TableName, list[Row]],
    row_count: int | None,
    override_strategies: dict[ColName, Strategy[Any, Any]] | None = None,
) -> Strategy[list[Row], Any]:
    try:
        row_strategy = get_row(table_info["columns"], table_info["fk_constraints"], data, override_strategies)
        _ = row_strategy.gen()
    except UnSatisfiableFkConstraintError:
        logger.warning(f"No row strategy generated for table {table_info['table']}, returning empty list strategy")
        return fixed_strategy([])

    unique_bys = tuple(get_uc_hasher(uc) for uc in table_info["unique_constraints"])
    return list_strategy(
        row_strategy,
        min_length=row_count if row_count is not None else MIN_ROWS,
        max_length=row_count if row_count is not None else MAX_ROWS,
        unique_bys=unique_bys,
    )


def get_db(
    schema: list[TableInfo],
    data: dict[TableName, list[Row]] | None = None,
    row_counts: RowCounts | None = None,
    tbl_override_strategies: dict[TableName, dict[ColName, Strategy[Any, Any]]] | None = None,
) -> dict[TableName, list[Row]]:
    # TODO allow for composite column overrides
    logger.info("Generating database schema")
    data = data or {}
    strats: dict[TableName, Strategy[list[Row], Any]] = {}
    schema_ = {tbl_info["table"]: tbl_info for tbl_info in schema}
    sorted_tbls = topo_sort_tables(
        {tbl_info["table"]: tbl_info["fk_constraints"] for tbl_info in schema},
        [tbl_info["table"] for tbl_info in schema],
    )

    for tbl in sorted_tbls:
        logger.info(f"Processing table: {tbl}")
        start_time = time.perf_counter()
        table_strat = get_table(
            schema_[tbl],
            data,
            row_counts.get(tbl) if row_counts else None,
            tbl_override_strategies.get(tbl) if tbl_override_strategies else None,
        )
        strats[tbl] = table_strat
        data[tbl] = table_strat.gen()
        elapsed = time.perf_counter() - start_time
        logger.info(f"Generated {len(data[tbl])} rows for table {tbl} in {elapsed:.3f} seconds")
    return data
