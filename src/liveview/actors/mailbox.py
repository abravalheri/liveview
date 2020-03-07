from __future__ import annotations  # noqa

import asyncio
from asyncio import Queue
from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload
)

from ..exceptions import init_with_docstring
from ..utils import NO_VALUE, OptVal
from .pattern import Pattern as _Pattern, TuplePattern, pattern

T = TypeVar("T")
S = TypeVar("S")
Pattern = Union[_Pattern, TuplePattern]
Topic = Union[str, tuple]


def is_topic(value):
    return isinstance(value, (str, tuple))


class NoValueFound(asyncio.QueueEmpty):
    """It is impossible to find in the queue any value that matches the pattern."""

    __init__ = init_with_docstring  # noqa


def _select_value(value, selectors, callbacks):
    for j in enumerate(selectors):
        if selectors[j].match(value):
            if callbacks:
                return value, callbacks[j]
            return value, None

    return NO_VALUE, None


class Mailbox(Generic[T]):
    """Similar to `asyncio.Queue`_, but able to perform "selective receive".

    See `~.select`.
    """

    # ---- Queue API ----
    def __init__(
        self,
        maxsize=0,
        *,
        _queue: Optional[Queue[T]] = None,
        _pending: Optional[List[T]] = None,
    ):
        """PRIVATE ARGS: _queue and _pending are not part of the public API)."""
        self._queue = Queue(maxsize) if _queue is None else _queue
        self._pending = [] if _pending is None else _pending

    @property
    def maxsize(self):
        return self._queue.maxsize

    def empty(self):
        return self._queue.empty() and len(self._pending) < 1

    async def get(self):
        """See `asyncio.Queue.get`_."""
        if len(self._pending) > 0:
            return self._pending.pop()
        return await self._queue.get()

    def full(self):
        if self.maxsize <= 0 or self.qsize() < self.maxsize:
            return False
        return True

    def get_nowait(self, default=NO_VALUE):
        """Similar to `asyncio.Queue.get_nowait`_, but can return a default value.

        When the argument ``default`` is passed, instead of raising an exception if the
        queue is empty, the default value is passed.
        """
        if len(self._pending) > 0:
            return self._pending.pop()
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            if default is not NO_VALUE:
                return default
            raise

    async def join(self):
        return await self._queue.join()

    async def put(self, item: T):
        await self._queue.put(item)

    def put_nowait(self, item: T):
        if self.full():
            raise asyncio.QueueFull

        self._queue.put_nowait(item)

    def qsize(self) -> int:
        return len(self._pending) + self._queue.qsize()

    def task_done(self):
        self._queue.task_done()

    # ---- Select API ----

    @overload  # noqa
    async def select(self, *args: Topic) -> Tuple[T, ...]:
        ...

    @overload  # noqa
    async def select(self, selection: Dict[Topic, Callable[[T], S]]) -> S:  # noqa
        ...

    async def select(self, *args):  # noqa
        selectors, callbacks, value, callback = self._select_common(args)
        if value is NO_VALUE:
            value, callback = await self._select_on_queue(selectors, callbacks)

        if callback:
            return callback(value)

        return value

    def select_nowait(self, *args, default=NO_VALUE):  # noqa
        selectors, callbacks, value, callback = self._select_common(args)
        if value is NO_VALUE:
            value, callback = self._select_nowait(selectors, callbacks, default)

        if callback:
            return callback(value)

        return value

    def _select_common(self, args):
        """This internal method avoid code duplication between get and get_nowait,
        encapsulating the common parts.
        """
        n = len(args)
        if n == 1 and isinstance(args, dict):
            selectors = list(args[0].keys())
            callbacks = list(args[0].items())
        elif any(not is_topic(a) for a in args):
            # TODO Better exception
            raise ValueError("Argument should be Topic or dict")
        else:
            selectors = args
            callbacks = []

        selectors = [pattern(s) for s in selectors]
        value, callback = self._select_on_pending(selectors, callbacks)

        return selectors, callbacks, value, callback

    def _select_on_pending(
        self, selectors: List[Pattern], callbacks: Optional[List[Callable[[T], S]]]
    ) -> Tuple[OptVal[T], Optional[Callable[[T], S]]]:

        for i in range(len(self._pending)):
            value, callback = _select_value(self._pending[i], selectors, callbacks)
            if value is not NO_VALUE:
                del self._pending[i]
                return value, callback

        return NO_VALUE, None

    async def _select_on_queue(
        self, selectors: List[Pattern], callbacks: Optional[List[Callable[[T], S]]]
    ) -> Tuple[OptVal[T], Optional[Callable[[T], S]]]:
        while True:
            item = await self._queue.get()
            value, callback = _select_value(item, selectors, callbacks)
            if value:
                return value, callback
            else:
                self._pending.append(item)

    def _select_nowait(
        self,
        selectors: List[Pattern],
        callbacks: Optional[List[Callable[[T], S]]],
        default: OptVal[T] = NO_VALUE,
    ) -> Tuple[OptVal[T], Optional[Callable[[T], S]]]:
        try:
            item = self._queue.get_nowait()
            value, callback = _select_value(item, selectors, callbacks)
            if value:
                return value, callback
            else:
                self._pending.append(item)
                raise NoValueFound
        except asyncio.QueueEmpty:
            if default is not NO_VALUE:
                return default, None
            raise

    def _format(self):
        return f"[queued: {self._queue.qsize()} + pending: {len(self._pending)}]"

    def __str__(self):
        return f"<{type(self).__name__}{self._format()}>"

    def __repr__(self):
        return f"<{type(self).__name__}{self._format()} at {id(self):#x} >"
