import asyncio
import contextlib
import logging
from enum import Enum
from functools import reduce, wraps
from typing import (
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    cast
)

T = TypeVar("T")
Number = Union[int, float]

LOGGER = logging.getLogger(__name__)


class ReprEnum(Enum):
    def __repr__(self):
        return f"<{self.__class__.__name__}.{self.name}>"


class NoValue(ReprEnum):
    """Useful when an argument is optional, but ``None`` is still a valid value"""

    NO_VALUE = object()


NO_VALUE = NoValue.NO_VALUE

OptVal = Union[NoValue, T]


def uniq(items: Iterable[T], key=id) -> Iterator[T]:
    """Guarantees the iterator will not have repeated elements."""
    seen: Set[Union[int, str]] = set()
    seen_add = seen.add
    for item in items:
        k = key(item)
        if k not in seen:
            seen_add(k)
            yield item


def pipe(*functions: Callable) -> Callable:
    """Given a list of functions as arguments, compose the functions flowing the
    left-to-right (``compose`` in reverse order)::

        pipe(f, g)(c) == g(f(x))
    """
    return lambda y: reduce(lambda acc, x: x(acc), functions, y)


async def wait_for(awaitable: Awaitable, timeout: Optional[Number] = None):
    """Wrapper around `asyncio.wait_for` but returns ``None`` after timeout"""
    wait: Optional[Awaitable] = None
    try:
        wait = asyncio.wait_for(awaitable, timeout)
        return await wait
    except asyncio.TimeoutError:
        if timeout is not None:
            # In theory, if no timeout is set, it should wait forever
            raise
        LOGGER.debug("Timeout reached (%s s). Returning `None`", timeout)
        return None
    except asyncio.CancelledError:
        if wait is not None and hasattr(wait, "cancel"):
            try:
                wait.cancel()  # type: ignore
            except Exception:
                msg = "Unexpected error when cancelling awaitable"
                LOGGER.error(msg, exc_info=True)


F = TypeVar("F", bound=Callable)


def suppress(*ex: Type[Exception]) -> Callable[[F], F]:
    def _decorator(fn: F) -> F:
        @wraps(fn)
        def _fn(*args, **kwargs):
            with contextlib.suppress(*ex):
                return fn(*args, **kwargs)

        return cast(F, _fn)

    return _decorator
