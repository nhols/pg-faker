import functools
import json
import logging
import random
from collections.abc import Callable, Hashable, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any, ParamSpec
from uuid import UUID

from faker import Faker

logger = logging.getLogger(__name__)

fake = Faker()


P = ParamSpec("P")


class Strategy[T: Any, **P]:
    def __init__(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def gen(self) -> T:
        return self.func(*self.args, **self.kwargs)

    def __str__(self) -> str:
        return f"Strategy({self.func.__name__}, {self.args}, {self.kwargs})"

    def __repr__(self) -> str:
        return f"Strategy({self.func.__name__}\n\t{self.args}\n\t{self.kwargs})"


def strategy_wrapper[T: Any, **P](func: Callable[P, T]) -> Callable[P, Strategy[T, P]]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Strategy[T, P]:
        return Strategy(func, *args, **kwargs)

    return wrapper


numeric_strategy = strategy_wrapper(fake.pydecimal)

int_strategy = strategy_wrapper(fake.pyint)

char_strategy = strategy_wrapper(fake.pystr)

uuid_strategy = strategy_wrapper(fake.uuid4)

date_strategy = strategy_wrapper(fake.date_between_dates)

timestamp_strategy = strategy_wrapper(fake.date_time_between_dates)

time_strategy = strategy_wrapper(fake.time)

bool_strategy = strategy_wrapper(fake.boolean)

json_strategy = strategy_wrapper(fake.json)

binary_strategy = strategy_wrapper(fake.binary)

xml_strategy = strategy_wrapper(fake.xml)


@strategy_wrapper
def counterparty_name_strategy() -> str:
    individual = fake.name() + fake.company_suffix()
    return fake.random_element([fake.company(), individual])


@strategy_wrapper
def one_of[T: Any](enum_values: list[T]) -> T:
    return fake.random_element(enum_values)


@strategy_wrapper
def nullable[T: Any, **P](strategy: Strategy[T, P], prob_null: float = 0.1) -> T | None:
    return strategy.gen() if random.random() > prob_null else None


@strategy_wrapper
def dict_strategy(
    strategies: dict[str, Strategy[Any, Any]],
    others: Sequence[Strategy[dict[str, Any], Any]] | None = None,
) -> dict[str, Any]:
    result = {key: value.gen() for key, value in strategies.items()}
    if others:
        for other in others:
            other_gen = other.gen()
            overlap = set(result.keys()) & set(other_gen.keys())
            if overlap:
                logger.warning(f"Key overlap in dict_strategy: {overlap}")
            result.update(other_gen)
    return result


class UnenforceableUniqueConstraintError(Exception):
    """
    Raised when a unique constraint cannot be enforced due to missing data.
    This can happen if the data does not provide enough unique values for the columns in the constraint.
    """


@strategy_wrapper
def list_strategy[T: Any, **P](
    strategy: Strategy[T, P],
    min_length: int = 5,
    max_length: int = 10,
    unique_bys: Sequence[Callable[[T], Hashable]] | None = None,
    max_iter: int = 10000,
) -> list[T]:
    """
    Generates a list of items using the provided strategy.

    Items are generated until either the list reaches a predetermined length between `min_length` and `max_length`,
    or the maximum number of iterations is reached.

    If `unique_bys` is provided, the function checks that adding the generated item to the list of previously generated
    items does not violate any of the provided uniqueness constraints. If a generated item would violate a uniqueness
    constraint, it is skipped and the function continues to generate new items.

    Args:
        strategy: The strategy to generate each item.
        min_length: Minimum length of the generated list.
        max_length: Maximum length of the generated list.
        unique_bys: A sequence of functions that return a hashable value for uniqueness checks. If any hashable value has been seen before, the item is skipped.
        max_iter: Maximum number of iterations to try generating items.
    """
    items = []

    seen = {i: set() for i in range(len(unique_bys))} if unique_bys is not None else {}
    length = random.randint(min_length, max_length)
    for _ in range(max_iter):
        if len(items) >= length:
            # already generated enough items
            break
        item = strategy.gen()
        if unique_bys:
            new_hashes = {}
            for i, ub in enumerate(unique_bys):
                try:
                    new_hash = ub(item)
                except UnenforceableUniqueConstraintError:
                    # if the unique constraint cannot be enforced, don't check it for this item
                    continue
                new_hashes[i] = new_hash
            if any(new_hash in seen[i] for i, new_hash in new_hashes.items()):
                # if any unique bys are not unique, don't update hashes or items
                continue
            for i, new_hash in new_hashes.items():
                seen[i].add(new_hash)
        items.append(item)
    if len(items) < min_length:
        logger.warning(f"Generated list has fewer items than min_length: {len(items)} < {min_length}")
    return items


@strategy_wrapper
def fixed_strategy[T: Any](value: T) -> T:
    """Returns a strategy that always returns the given value."""
    return value
