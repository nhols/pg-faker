from typing import Any

from faker import Faker
from .gen import Strategy, counterparty_name_strategy

fake = Faker()

COL_NAME_STRATEGY_MAPPINGS: dict[tuple[str, ...], Strategy[str, Any]] = {
    ("address",): Strategy(fake.address),
    ("name",): Strategy(fake.name),
    ("email",): Strategy(fake.email),
    ("phone_number",): Strategy(fake.phone_number),
    ("telephone",): Strategy(fake.phone_number),
    ("city",): Strategy(fake.city),
    ("country",): Strategy(fake.country),
    ("zip_code",): Strategy(fake.zipcode),
    ("postal_code",): Strategy(fake.zipcode),
    ("street",): Strategy(fake.street_address),
    ("street_address",): Strategy(fake.street_address),
    ("currency",): Strategy(fake.currency_code),
    ("company", "name"): Strategy(fake.company),
    ("counterparty", "name"): counterparty_name_strategy(),
    ("customer", "name"): counterparty_name_strategy(),
    ("supplier", "name"): counterparty_name_strategy(),
}
