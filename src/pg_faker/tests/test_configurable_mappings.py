from pg_faker import COL_NAME_STRATEGY_MAPPINGS, get_db
from pg_faker.pg import ColInfo, TableInfo
from pg_faker.strategies import fixed_strategy


def test_custom_col_name_strategy_mappings():
    """Test that custom column name strategy mappings work correctly."""

    # Create a simple table schema
    table_info = TableInfo(
        table="test.users",
        columns={
            "email": ColInfo(
                col_name="email",
                pgtype="text",
                nullable=False,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                enum_values=None,
            ),
            "custom_field": ColInfo(
                col_name="custom_field",
                pgtype="text",
                nullable=False,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                enum_values=None,
            ),
        },
        unique_constraints=[],
        fk_constraints=[],
    )

    # Test with default mappings
    data_default = get_db([table_info], row_counts={"test.users": 2})

    # Verify that email mapping worked (should contain '@' symbol)
    for row in data_default["test.users"]:
        assert "@" in row["email"], f"Email {row['email']} doesn't look like an email"

    # Test with custom mappings
    custom_mappings = COL_NAME_STRATEGY_MAPPINGS.copy()
    custom_mappings[("custom",)] = fixed_strategy("CUSTOM_VALUE")

    data_custom = get_db([table_info], row_counts={"test.users": 2}, col_name_strategy_mappings=custom_mappings)

    # Verify that custom mapping worked
    for row in data_custom["test.users"]:
        assert row["custom_field"] == "CUSTOM_VALUE", (
            f"custom_field should be 'CUSTOM_VALUE', got {row['custom_field']}"
        )
        assert "@" in row["email"], f"Email {row['email']} doesn't look like an email"


def test_col_name_strategy_mappings_with_none():
    """Test that passing None for col_name_strategy_mappings uses default mappings."""

    table_info = TableInfo(
        table="test.users",
        columns={
            "email": ColInfo(
                col_name="email",
                pgtype="text",
                nullable=False,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                enum_values=None,
            ),
        },
        unique_constraints=[],
        fk_constraints=[],
    )

    # Test with None (should use default mappings)
    data_none = get_db([table_info], row_counts={"test.users": 1}, col_name_strategy_mappings=None)

    # Verify that default email mapping worked
    for row in data_none["test.users"]:
        assert "@" in row["email"], f"Email {row['email']} doesn't look like an email"


def test_col_name_strategy_mappings_override_default():
    """Test that custom mappings can override default ones with proper ordering."""

    table_info = TableInfo(
        table="test.users",
        columns={
            "email": ColInfo(
                col_name="email",
                pgtype="text",
                nullable=False,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                enum_values=None,
            ),
            "work_email": ColInfo(
                col_name="work_email",
                pgtype="text",
                nullable=False,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                enum_values=None,
            ),
        },
        unique_constraints=[],
        fk_constraints=[],
    )

    # Create custom mappings with proper ordering - specific mappings first
    custom_mappings = {}
    custom_mappings[("work", "email")] = fixed_strategy("work@company.com")
    custom_mappings[("email",)] = fixed_strategy("personal@example.com")

    data_custom = get_db([table_info], row_counts={"test.users": 1}, col_name_strategy_mappings=custom_mappings)

    # Verify that custom mappings worked with correct precedence
    for row in data_custom["test.users"]:
        assert row["work_email"] == "work@company.com", (
            f"work_email should use specific mapping, got {row['work_email']}"
        )
        assert row["email"] == "personal@example.com", f"email should use general mapping, got {row['email']}"
