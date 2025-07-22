import pytest

from pg_faker.generate import get_fk_constrained_options
from pg_faker.pg import ColInfo, FkConstraint, TableInfo


def text_col(colname: str) -> ColInfo:
    return ColInfo(
        col_name=colname,
        pgtype="text",
        nullable=True,
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


def test_get_fk_constrained_options(schema):
    data = {
        "parent1": [{"a": f"a{i}", "b": f"b{i}"} for i in range(10)],
        "parent2": [{"b": f"b{i}", "c": f"c{i}"} for i in range(5, 10)],
    }
    cols, strat = get_fk_constrained_options(fk_constraints=schema[2]["fk_constraints"], data=data)

    assert cols == {"a", "b", "c"}
    assert strat is not None
    allowed_rows = strat.args[0]
    assert len(allowed_rows) == 5
