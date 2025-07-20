import functools
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


@strategy_wrapper
def numeric_strategy(precision: int = 53, scale: int = 0) -> Decimal:
    return fake.pydecimal(left_digits=precision - scale, right_digits=scale)


@strategy_wrapper
def int_strategy(precision: int = 32) -> int:
    max_value: int = 2 ** (precision - 1) - 1
    min_value = -1 * max_value - 1
    return fake.pyint(min_value=min_value, max_value=max_value)


@strategy_wrapper
def char_strategy(max_length: int | None = None) -> str:
    if max_length:
        return fake.pystr(max_chars=max_length)
    return fake.pystr()


@strategy_wrapper
def uuid_strategy() -> UUID:
    return fake.uuid4(cast_to=None)


@strategy_wrapper
def date_strategy() -> date:
    return fake.date_between_dates()


@strategy_wrapper
def timestamp_strategy() -> datetime:
    return fake.date_time_between_dates()


@strategy_wrapper
def bool_strategy() -> bool:
    return fake.boolean()


@strategy_wrapper
def json_strategy() -> str:
    return fake.json()


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


@strategy_wrapper
def list_strategy[T: Any, **P](
    strategy: Strategy[T, P],
    min_length: int = 5,
    max_length: int = 10,
    unique_by: Sequence[Callable[[T], Hashable]] | None = None,
    max_iter: int = 10000,
) -> list[T]:
    items = []

    seen = {i: set() for i in range(len(unique_by))} if unique_by is not None else {}
    length = random.randint(min_length, max_length)
    for _ in range(max_iter):
        if len(items) >= length:
            break
        item = strategy.gen()
        if unique_by:
            new_hashes = {i: ub(item) for i, ub in enumerate(unique_by)}
            if any(
                new_hash in seen_hashes
                for new_hash, seen_hashes in zip(new_hashes.values(), seen.values(), strict=True)
            ):
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
