import asyncio
import logging
from enum import Enum
from typing import Awaitable, Iterable, Iterator, Optional, Set, TypeVar, Union

T = TypeVar("T")
Number = Union[int, float]

LOGGER = logging.getLogger(__name__)


class ReprEnum(Enum):
    def __repr__(self):
        return f"<{self.__class__.__name__}.{self.name}>"


class NoArg(ReprEnum):
    """Useful when an argument is optional, but ``None`` is still a valid value"""

    NO_ARG = object()


NO_ARG = NoArg.NO_ARG

OptionalArg = Union[NoArg, T]


def uniq(items: Iterable[T], key=id) -> Iterator[T]:
    seen: Set[Union[int, str]] = set()
    seen_add = seen.add
    for item in items:
        k = key(item)
        if k not in seen:
            seen_add(k)
            yield item


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
