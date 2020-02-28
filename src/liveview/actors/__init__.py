import asyncio
import fnmatch
import logging
import re
from asyncio import Future, Queue
from collections import deque
from enum import Enum, auto
from typing import (
    Any,
    Awaitable,
    Callable,
    Deque,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional
)
from typing import Pattern as Regex
from typing import Set, Tuple, TypeVar, Union, overload

from ..exceptions import init_with_docstring
from .registry import Registry

T = TypeVar("T")
S = TypeVar("S")
Ref = Union[str, "Actor"]
Recipient = TypeVar("Recipient", str, "Actor", Tuple["Actor", Awaitable])
Topic = TypeVar("Topic", str, Tuple[str, str])
PatternLike = Union[str, Regex, "Pattern"]
Number = Union[int, float]

LOGGER = logging.getLogger(__name__)


class _ReprEnum(Enum):
    def __repr__(self):
        return f"<{self.__class__.__name__}.{self.name}>"


class LcmToken(_ReprEnum):
    REPLY = auto()
    NOREPLY = auto()
    STOP = auto()
    DOWN = auto()
    EXIT = auto()
    IGNORE = auto()
    NORMAL = auto()
    SHUTDOWN = auto()


REPLY, NOREPLY, STOP, DOWN, EXIT, IGNORE, NORMAL, SHUTDOWN = list(LcmToken)


class NoArg(_ReprEnum):
    """Useful when an argument is optional, but ``None`` is still a valid value"""

    NO_ARG = object()


NO_ARG = NoArg.NO_ARG

OptionalArg = Union[NoArg, T]


class SuccessToken(_ReprEnum):
    OK = auto()
    FAIL = auto()


OK, FAIL = list(SuccessToken)


class Response(tuple, Generic[T]):
    status: SuccessToken
    value: T

    def __new__(cls, status: SuccessToken, value: T):
        self = tuple.__new__(cls, (status, value))
        self.status = status
        self.value = value
        return self

    def map(self, fn):
        if self.status == FAIL:
            return self
        try:
            return Ok(fn(self.value))
        except Exception as ex:
            return Fail(ex)

    def fix(self, fn):
        if self.status == OK:
            return self
        try:
            return Ok(fn(self.value))
        except Exception as ex:
            return Fail(ex)


class Ok(Response):
    def __new__(cls, value):
        return super().__new__(cls, OK, value)


class Fail(Response):
    def __new__(cls, value):
        return super().__new__(cls, FAIL, value)

    @property
    def reason(self):
        return self.value


class TopicToken(_ReprEnum):
    CALL = auto()
    CAST = auto()
    OTHER = auto()


CALL, CAST, OTHER = list(TopicToken)
TopicType = Union[str, Tuple[TopicToken, str]]
ReplyToType = Union["Actor", Tuple["Actor", Future]]


class Message(tuple, Generic[T]):
    to: "Actor"
    topic: TopicType
    payload: T
    sender: "Actor"
    reply: Optional[Future]
    """The parameter ``reply`` is not part of the public API of the library"""

    def __new__(
        cls,
        to: "Actor",
        topic: TopicType,
        payload: T,
        sender: "Actor",
        reply: Optional[Future] = None,
    ):
        self = tuple.__new__(cls, (to, topic, payload, sender, reply))
        self.to = to
        self.topic = topic
        self.payload = payload
        self.sender = sender
        self.reply = reply
        return self


class Call(Message[T]):
    def __new__(cls, to, topic, payload, sender, reply):
        return super().__new__(cls, to, (CALL, topic), payload, sender, reply)


class Cast(Message[T]):
    reply: None = None

    def __new__(cls, to, topic, payload, sender):
        return super().__new__(cls, to, (CAST, topic), payload, sender)


class Broadcast(Cast[T]):
    reply: None = None


def uniq(items: Iterable[T], key=id) -> Iterator[T]:
    seen: Set[Union[int, str]] = set()
    seen_add = seen.add
    for item in items:
        k = key(item)
        if k not in seen:
            seen_add(k)
            yield item


class InvalidPattern(ValueError):
    """The given object is not a string or regex"""

    def __init__(self, value):
        msg = (
            f"Pattern should be a string or an object with a ``fullmatch`` method, "
            "{value} given."
        )
        super().__init__(msg, value)


def _compile_patten(value: Union[str, Regex]):
    """Compile pattern to be matched.

    If the string have the format ``/.../`` (starting and leading ``/``) it
    will be compiled to a regex object.

    If the string have any of the characters ``*, ?, [`` it will be compiled
    according to fnmatch
    """
    if isinstance(value, str):
        if value[0] == "/" and value[-1] == "/":
            return re.compile(value.strip("/"))

        if any(ch in value for ch in ("*", "?", "[")):
            return re.compile(fnmatch.translate(value))

        return value

    if not hasattr(value, "fullmatch"):
        raise InvalidPattern(value)

    return value


class Pattern:
    def __new__(cls, value: PatternLike) -> "Pattern":
        if isinstance(value, cls):
            return value
        return super().__new__(cls)

    def __init__(self, value: Union[str, Regex]):
        pattern = _compile_patten(value)
        self.value = pattern

    def _filter(self, items: Iterable[Tuple[str, T]]) -> Iterator[T]:
        pattern = self.value

        if isinstance(pattern, str):
            return (v for k, v in items if pattern == k)

        matcher = pattern.fullmatch
        return (v for k, v in items if matcher(k))

    def filter(self, items: Iterable[Tuple[str, T]]) -> Iterator[T]:
        """Return a iterator with all the items that match the pattern"""
        return uniq(self._filter(items))

    def matcher(self):
        pattern = self.value
        if isinstance(pattern, str):
            return lambda item: pattern == item
        return pattern.fullmatch

    @overload
    def first(self, items: Iterable[Tuple[str, T]], default: S) -> Union[T, S]:
        ...

    @overload  # noqa
    def first(self, items: Iterable[Tuple[str, T]]) -> T:
        ...

    def first(  # noqa
        self, items: Iterable[Tuple[str, T]], default: OptionalArg[S] = NO_ARG
    ) -> Union[T, S]:
        if default is NO_ARG:
            return next(self.filter(items))

        return next(self.filter(items), default)


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


class NotRegistered(SystemError):
    """Actor is not registered yet and this operation requires the actor to be
    registered in a `Registry` object
    """

    __init__ = init_with_docstring


class Actor:
    def __init__(
        self,
        _registry: Optional[Registry] = None,
        _queue: Optional[Queue] = None,
        _links: Optional[Deque["Actor"]] = None,
        _monitors: Optional[Dict[int, "Actor"]] = None,
    ):
        self._registry = _registry
        self._queue = _queue or Queue()
        self._links = _links or deque()
        self._monitors: Dict[int, "Actor"] = _monitors or {}
        self._monitor_counter = 0
        self._callbacks: Dict[TopicToken, List[Callable]] = {
            CALL: [],
            CAST: [],
            OTHER: [],
        }
        self._loop: Optional[Awaitable] = None

    # ---- Metadata API ----

    @property
    def id(self):
        return str(id(self))

    # ---- Life-Cycle API ----

    def link(self, other: "Actor"):
        """Link the current actor to other"""
        self._links.append(other)
        other._links.append(self)

    def unlink(self, other: "Actor"):
        """Remove link between this actor and other"""
        self._links.remove(other)
        other._links.remove(self)

    def start(self, init_arg, *, link: Optional["Actor"] = None, **options):
        """Start this actor"""
        if link:
            self.link(link)
        # TODO

    def spawn(self, other: "Actor", init_arg, *, alias=None, link=False, **options):
        """Start other actor (uses the same registry)"""
        if self._registry is not None:
            if other.id not in self._registry:
                self._registry.register(other)
            if alias is not None and alias not in self._registry:
                self._registry.register(other, alias=alias)
        if link:
            options["link"] = self
        other.start(init_arg, **options)

    def monitor(self, other: "Actor"):
        i = self._monitor_counter
        self._monitors[i] = other
        self._monitor_counter += 1
        return i

    def demonitor(self, monitor_ref: int) -> "Actor":
        return self._monitors.pop(monitor_ref)

    # ---- Messaging API ----

    def _solve(self, ref: Union[str, "Actor"]) -> "Actor":
        if isinstance(ref, Actor):
            return ref
        if self._registry is not None:
            return self._registry[ref]
        raise NotRegistered(operation="find actor", actor_reference=ref)

    def send(
        self, topic: str, payload: Any = None, reply=False, *, to: Ref, wait=False
    ) -> Union[Future, Awaitable, None]:
        """Send a message from this actor to another one"""

        if reply:
            loop = asyncio.get_running_loop()
            response: Optional[Future] = loop.create_future()
        else:
            response = None

        target = self._solve(to)
        msg = Message(target, topic, payload, self, response)

        if wait:

            async def _defer():
                await target._queue.put(msg)
                return response

            return _defer()

        target._queue.put_nowait(msg)
        return response

    @overload
    def receive(self) -> Awaitable:
        ...

    @overload  # noqa
    def receive(self, wait: bool) -> Any:
        ...

    def receive(self, wait=True):  # noqa
        """Receive a message from other actor.
        When wait is `False`, will return `None` if queue is empty
        """
        if wait:
            return self._queue.get()
        else:
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return None

    def cast(self, topic: str, payload: Any = None, *, to: Ref):
        """Send (cast) a message to another actor, without waiting for answer"""
        to = self._solve(to)
        return to._queue.put_nowait(Cast(to, topic, payload, self))

    def broadcast(
        self,
        topic: str,
        payload: Any = None,
        *,
        to: Union[str, Regex, List["Actor"], None] = None,
    ):
        if isinstance(to, list):
            receivers: Iterator["Actor"] = (self._solve(ref) for ref in to)
        elif self._registry is None:
            raise NotRegistered(operation="broadcast")
        elif to is None:
            receivers = self._registry.actors
        else:
            receivers = Pattern(to).filter(self._registry.aliases)

        for target in receivers:
            target._queue.put_nowait(Broadcast(target, topic, payload, self))

    async def call(self, topic: str, payload: Any = None, *, on: Ref):
        # Right now, a future is being used to simplify getting the response back.
        # (No complex mailbox needs to be implemented, we don't have to re-enque
        # messages received while waiting for a response, etc...)
        # This approach, however, might make the Message Object "unserializable"
        # (pickle).
        # Therefore, in a more serious implementation this would need to change.
        # Erlang uses process monitor/demonitor with a unique id var for it, but then,
        # in erlang is just easy to do pattern match in the received messages and all
        # the ones that don't match are kept in the mailbox...
        loop = asyncio.get_running_loop()
        reply = loop.create_future()
        to = self._solve(on)
        message: Call = Call(to, topic, payload, self, reply)

        await to._queue.put(message)
        return await reply

    # ---- Callback API ----
    def init(self, function):
        # TODO
        pass

    def handle_call(self, topic):
        # TODO
        pass

    def handle_cast(self, topic):
        # TODO
        pass

    def handle_info(self, topic):
        # TODO
        pass

    def trap_exit(self, topic):
        # TODO
        pass

    def terminate(self, reason):
        # TODO
        pass
