# pg-faker

`pg-faker` is a Python library that leverages [Faker](https://github.com/xfxf/faker-python) to generate fake data for PostgreSQL databases. It introspects your database schema to understand column types, relationships (foreign keys), and constraints (unique constraints), and then populates your tables with procedurally generated data that confirms to the schema.

## High-Level Description

The library is designed to make it easy to populate a development or testing database with sensible data. It automatically handles most common data types, follows foreign key relationships to ensure data integrity, and respects unique constraints.


There are two primary APIs for interacting with `pg-faker`:

1.  **`run()`**: The simplest way to get started. You provide a database connection, and `run()` handles everything: schema introspection, data generation, and insertion.
2.  **`get_db()`**: A lower-level API that gives you more control. It generates the data as a Python dictionary but does not write it to the database, allowing you to inspect or modify the data before insertion.

## How `run()` Works

The `run` function automates the process of populating your database:

1.  **Introspects Schema**: It connects to your database and queries the system catalogs to build a model of your schema. This includes tables, columns, data types, nullability, foreign keys, and unique constraints.
2.  **Define Strategy**: Based on the introspected schema, it defines a data generation strategy. It maps PostgreSQL types to appropriate data generators (e.g., `VARCHAR` -> random strings, `INTEGER` -> random integers, `TIMESTAMP` -> random timestamps). It understands dependencies, so it will generate data for parent tables before child tables.
3.  **Generate Data**: It executes the strategy to generate a set of rows for each table, respecting all constraints.
4.  **Write to DB**: Finally, it inserts the generated data into your database.

## Example Usage

Let's assume you have the following SQL schema:

```sql
CREATE TABLE "user" (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE post (
    id SERIAL PRIMARY KEY,
    author_id INTEGER NOT NULL REFERENCES "user"(id),
    title VARCHAR(255) NOT NULL,
    content TEXT,
    published_at TIMESTAMPTZ,
    UNIQUE (author_id, title)
);
```

### Using `run()`

The `run()` function is the most straightforward way to populate your database.

```python
import psycopg
from pg_faker import run

# Connect to your database
with psycopg.connect("dbname=test user=test password=test host=localhost") as conn:
    # Define custom row counts for each table
    row_counts = {
        "public.user": 3,
        "public.post": 5,
    }

    # Run the data generation and insertion process
    run(
        conn,
        row_counts=row_counts,
    )
```

In this example, `pg-faker` generates 3 users and 5 posts with default data based on column types.

### Overriding Generation Strategies

You can customize the data generation by providing your own strategies. A strategy is a function that returns a value for a column. You can create one easily using the `@strategy_wrapper` decorator.

Let's create a custom strategy to generate post titles that start with "How to" and a strategy to generate post contents with real words.

```python
import psycopg
import psycopg.rows
from faker import Faker

from pg_faker import run
from pg_faker.strategies import strategy_wrapper

fake = Faker()


@strategy_wrapper
def how_to_title_strategy(min_words=3, max_words=6):
    """Generates a title starting with 'How to'."""
    words = fake.words(nb=fake.random_int(min=min_words, max=max_words))
    return f"How to {' '.join(words)}"


paragraph_strategy = strategy_wrapper(fake.paragraph)

# Connect to your database
with psycopg.connect("dbname=test user=test password=test host=localhost") as conn:
    row_counts = {
        "public.user": 3,
        "public.post": 5,
    }

    # Override the default data generation strategy for the 'title' and 'content' columns
    override_strategies = {
        "public.post": {
            "title": how_to_title_strategy(min_words=2, max_words=4),
            "content": paragraph_strategy(nb_sentences=2),
        }
    }

    # Run the data generation and insertion process
    run(
        conn,
        row_counts=row_counts,
        tbl_override_strategies=override_strategies,
    )

    # Query and print the generated data to see our custom titles
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT * FROM public.user;")
        users = cur.fetchall()
        print("Generated users:")
        for user in users:
            print(user)

        cur.execute("SELECT *FROM public.post;")
        posts = cur.fetchall()
        print("Generated posts:")
        for post in posts:
            print(post)
```

```
test=# select * from public.user;
     id     |          email           |       name        |       created_at
------------+--------------------------+-------------------+------------------------
 1975445732 | evang@example.net        | Gregory Vazquez   | 2025-07-26 17:20:20+01
 -260727603 | austin50@example.net     | Alexis Simon      | 2025-07-26 17:20:20+01
  731954615 | brownnatasha@example.net | Colleen Henderson | 2025-07-26 17:20:20+01
(3 rows)

test=# select * from public.post;
     id      | author_id  |             title              |                                           content                                            |      published_at
-------------+------------+--------------------------------+----------------------------------------------------------------------------------------------+------------------------
  1222876346 | -260727603 | How to affect do relate        | Begin seem himself compare. Animal Republican strategy speak affect north everybody suggest. | 2025-07-26 17:20:20+01
  1216478157 | -260727603 | How to culture recently office | Budget computer little challenge party up.                                                   | 2025-07-26 17:20:20+01
   184296559 |  731954615 | How to film it he determine    | Hold method station. Minute exactly determine future exactly key vote more.                  | 2025-07-26 17:20:20+01
 -1507982496 | -260727603 | How to individual miss         | Dog girl left. Pm very property town.                                                        | 2025-07-26 17:20:20+01
  2086241808 | 1975445732 | How to teach begin push        | Five protect score anyone into social campaign.                                              | 2025-07-26 17:20:20+01
(5 rows)
```
In this example:
- We define `how_to_title_strategy` to generate our desired titles.
- We pass this strategy to `run()` via `tbl_override_strategies`.
- `pg-faker` handles the rest:
    - It first generates records for the parent table (`user`) before generating records for the child table (`post`), ensuring that all foreign key constraints are satisfied and `post.author_id` always references an existing `user.id`.
    - It respects the `UNIQUE` constraints on `user.email` and the composite key `(post.author_id, post.title)`.
    - It generates appropriate data for all other columns based on their types (`TEXT`, `TIMESTAMPTZ`, etc.).

### Using `get_db()`

If you need to inspect or manipulate the data before inserting it, `get_db()` is the right tool. It returns the data as a dictionary.

```python
import psycopg
from pg_faker import get_db
from pg_faker.pg import get_schema

# Connect to your database
with psycopg.connect("dbname=test user=test password=test host=localhost") as conn:
    # 1. Introspect the schema
    schema = get_schema(conn) # You can also define your own schema

    # 2. Define custom row counts
    row_counts = {
        "public.user": 3,
        "public.post": 5,
    }

    data = get_db(
        schema,
        row_counts=row_counts,
    )

    # `data` is a dictionary like:
    # {
    #     'public.user': [
    #         {'id': 1, 'email': '...', 'name': '...', 'created_at': ...},
    #         ...
    #     ],
    #     'public.post': [
    #         {'id': 1, 'author_id': ..., 'title': '...', 'content': '...', 'published_at': ...},
    #         ...
    #     ]
    # }

    print(f"Generated {len(data['public.user'])} users.")
    print(f"Generated {len(data['public.post'])} posts.")

    # You can now insert this data into the database yourself if needed.
```

This example achieves a similar result to the `run()` example but gives you an intermediate `data` object to work with.


## Configurable Column Name Strategy Mappings

`pg-faker` automatically generates more realistic data for columns based on their names. For example, a column named `email` will automatically generate email addresses instead of random strings. This is done through a configurable mapping system.

### Default Column Name Mappings

By default, `pg-faker` includes the following column name to strategy mappings:

- `address` → addresses
- `name` → person names
- `email` → email addresses
- `phone_number`, `telephone` → phone numbers
- `city` → city names
- `country` → country names
- `zip_code`, `postal_code` → zip codes
- `street`, `street_address` → street addresses
- `currency` → currency codes
- `company name` → company names
- `counterparty name`, `customer name`, `supplier name` → counterparty names

### Customizing Column Name Mappings

You can customize these mappings by providing your own `col_name_strategy_mappings` parameter to either `run()` or `get_db()`:

```python
import psycopg
from pg_faker import run, COL_NAME_STRATEGY_MAPPINGS
from pg_faker.strategies import fixed_strategy

# Create custom mappings - order matters!
# More specific mappings should come first
custom_mappings = {}

# Add specific mappings first
custom_mappings[("department",)] = fixed_strategy("Engineering")
custom_mappings[("work", "email")] = fixed_strategy("employee@company.com")

# Then add the default mappings
custom_mappings.update(COL_NAME_STRATEGY_MAPPINGS)

with psycopg.connect("dbname=test user=test password=test host=localhost") as conn:
    run(
        conn,
        row_counts={"public.users": 10},
        col_name_strategy_mappings=custom_mappings
    )
```

**Important:** When creating custom mappings, the order matters! More specific mappings should be listed before more general ones. In the example above, we first add our custom mappings, then add the default mappings. This ensures that `("work", "email")` matches before the more general `("email",)` mapping.

### How Column Name Matching Works

The matching is based on whether all words in a mapping key are present in the column name. For example:
- A mapping key `("company", "name")` will match columns like `company_name`, `supplier_company_name`, etc.
- A mapping key `("email",)` will match columns like `email`, `user_email`, `contact_email`, etc.

The first matching mapping is used, so more specific mappings should be listed before more general ones.

### Using with `get_db()`

You can also use custom column name strategy mappings with the `get_db()` function:

```python
from pg_faker import get_db, COL_NAME_STRATEGY_MAPPINGS
from pg_faker.strategies import fixed_strategy

# Create custom mappings with proper ordering
custom_mappings = {}
custom_mappings[("status",)] = fixed_strategy("active")
custom_mappings.update(COL_NAME_STRATEGY_MAPPINGS)

data = get_db(
    schema,
    row_counts=row_counts,
    col_name_strategy_mappings=custom_mappings
)
```


## Handling CHECK Constraints

`pg-faker` does not automatically detect or enforce `CHECK` constraints in your schema. This means that randomly generated data may violate these constraints, resulting in errors when inserting data into the database.

### Example: CHECK Constraint on Age

Suppose you have a table with a `CHECK` constraint:

```sql
CREATE TABLE person (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER NOT NULL CHECK (age >= 18)
);
```

If you use the default data generation, `pg-faker` may generate values for `age` that are less than 18, causing an error when inserting rows.

#### Solution: Use a Custom Strategy

You can provide a custom strategy to ensure that only valid values are generated for columns with `CHECK` constraints. For example:

```python
from pg_faker import run
from pg_faker.strategies import int_strategy

override_strategies = {
    "public.person": {
        "age": int_strategy(min_value=18),
    }
}

run(
    conn,
    tbl_override_strategies=override_strategies,
)
```

By supplying a custom strategy for the `age` column, you ensure that all generated values satisfy the `CHECK (age >= 18)` constraint, preventing insertion errors.


## Limitations

`pg-faker` currently has the following limitations:

- **Cyclic Foreign Key Constraints**: Schemas with cycles in their foreign key relationships are not supported. Attempting to use `pg-faker` with such schemas will result in an error ("Cycle detected in foreign key constraints").

- **Supported Data Types**: The data types supported by `pg-faker` are determined by the logic in `col_info_to_strategy`. The following PostgreSQL types are supported:

  - `uuid`
  - `date`
  - `timestamptz`, `timestamp`
  - `time`, `timetz`
  - `varchar`, `text`, `bpchar`
  - `numeric`, and types starting with `float`
  - `bool`
  - `int2`, `int4`, `int8`
  - `json`, `jsonb`
  - `enum` (columns with enum values)
  - `bit`, `varbit`
  - `xml`

Any other PostgreSQL data types are not currently supported and will result in an error if encountered. This includes, but is not limited to:

  - `array`
  - `bytea`
  - `cidr`
  - `inet`
  - `interval`
  - `macaddr`
  - `macaddr8`
  - `pg_lsn`
  - `pg_snapshot`
  - `tsquery`
  - `tsvector`
  - `txid_snapshot`
  - `box`
  - `circle`
  - `line`
  - `lseg`
  - `path`
  - `point`
  - `polygon`

If you attempt to use `pg-faker` with an unsupported type, you will receive an error such as `ValueError: Unsupported pgtype: ...`. Support for additional types may be added in future releases. Contributions are welcome!