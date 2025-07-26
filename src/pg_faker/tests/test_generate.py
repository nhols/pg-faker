import pytest

from pg_faker.generate import UnSatisfiableFkConstraintError, get_fk_constrained_options, get_row, get_table
from pg_faker.pg import ColInfo, FkConstraint, TableInfo
from pg_faker.strategies import fixed_strategy


def text_col(colname: str) -> ColInfo:
    return ColInfo(
        col_name=colname,
        pgtype="text",
        nullable=False,
        character_maximum_length=None,
        numeric_precision=None,
        numeric_scale=None,
        enum_values=None,
    )


@pytest.fixture
def schema() -> list[TableInfo]:
    return [
        TableInfo(
            table="parent1",
            columns={"a": text_col("a"), "b": text_col("b")},
            unique_constraints=[("a", "b")],
            fk_constraints=[],
        ),
        TableInfo(
            table="parent2",
            columns={"a": text_col("b"), "b": text_col("c")},
            unique_constraints=[("b", "c")],
            fk_constraints=[],
        ),
        TableInfo(
            table="child",
            columns={"a": text_col("a"), "b": text_col("b"), "c": text_col("c")},
            unique_constraints=[],
            fk_constraints=[
                FkConstraint(
                    local_table="child",
                    foreign_table="parent1",
                    local_foreign_mapping={"a": "a", "b": "b"},
                ),
                FkConstraint(
                    local_table="child",
                    foreign_table="parent2",
                    local_foreign_mapping={"b": "b", "c": "c"},
                ),
            ],
        ),
    ]


@pytest.fixture
def parent_data() -> dict[str, list[dict[str, str]]]:
    return {
        "parent1": [{"a": f"a{i}", "b": f"b{i}"} for i in range(10)],
        "parent2": [{"b": f"b{i}", "c": f"c{i}"} for i in range(5, 10)],
    }


def test_get_fk_constrained_options(schema, parent_data):
    child_tbl_info = schema[2]
    cols, strat = get_fk_constrained_options(fk_constraints=child_tbl_info["fk_constraints"], data=parent_data)

    assert cols == {"a", "b", "c"}
    assert strat is not None
    allowed_rows = strat.args[0]
    assert len(allowed_rows) == 5


def test_get_row_null_local_col_in_fk_constraint(schema, parent_data):
    child_tbl_info = schema[2]

    row_strat = get_row(
        child_tbl_info["columns"],
        child_tbl_info["fk_constraints"],
        parent_data,
        # child.c is always None, so child(b,c) > parent2(b,c) fk constraint is not enforced
        {"c": fixed_strategy(None)},
    )
    possible_rows = [{"c": None, **r} for r in parent_data["parent1"]]
    assert row_strat.gen() in possible_rows


@pytest.mark.parametrize(
    "parent_data1_override",
    (
        pytest.param([], id="empty_parent1"),
        # child.a is not nullable so the fk constraint child(a,b) > parent1(a,b) cannot be satisfied
        pytest.param([{"parent1": [{"a": None, "b": "b0"}]}], id="parent1_with_null_a"),
    ),
)
def test_get_row_no_possible_rows(schema, parent_data, parent_data1_override):
    child_tbl_info = schema[2]
    parent_data["parent1"] = []

    row_strat = get_row(
        child_tbl_info["columns"],
        child_tbl_info["fk_constraints"],
        parent_data,
    )
    with pytest.raises(UnSatisfiableFkConstraintError):
        row_strat.gen()


@pytest.mark.parametrize(
    "parent_data1_override",
    (
        pytest.param([], id="empty_parent1"),
        pytest.param([{"parent1": [{"a": None, "b": "b0"}]}], id="parent1_with_null_a"),
    ),
)
def test_get_table_no_possible_rows_due_to_fk_constraints(schema, parent_data, parent_data1_override):
    child_tbl_info = schema[2]
    parent_data["parent1"] = []

    table_strat = get_table(
        child_tbl_info,
        parent_data,
        row_count=1,
    )
    assert table_strat.gen() == []
