import asyncio
import fnmatch
import re
import time
from asyncio import Future, Queue
from collections import deque
from enum import Enum, auto
from typing import Any, Awaitable, Dict, Generic, Iterable, Iterator, Optional
from typing import Pattern as Regex
from typing import Set, Tuple, TypeVar, Union, overload

T = TypeVar("T")
S = TypeVar("S")
Ref = Union[str, "Actor"]
Recipient = TypeVar("Recipient", str, "Actor", Tuple["Actor", Awaitable])
Topic = TypeVar("Topic", str, Tuple[str, str])
PatternLike = Union[str, Regex, "Pattern"]
Number = Union[int, float]


class _ReprEnum(Enum):
    def __repr__(self):
        return f"<{self.__class__.__name__}.{self.name}>"


class ReplyToken(_ReprEnum):
    REPLY = auto()
    NOREPLY = auto()


REPLY, NOREPLY = list(ReplyToken)


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
        super().__new__(cls, OK, value)


class Fail(Response):
    def __new__(cls, value):
        super().__new__(cls, FAIL, value)

    @property
    def reason(self):
        return self.value


class TopicToken(Enum):
    CALL = auto()
    CAST = auto()
    BROADCAST = auto()


CALL, CAST, BROADCAST = list(TopicToken)
TopicType = Union[str, Tuple[TopicToken, str]]
ReplyToType = Union["Actor", Tuple["Actor", Future]]


class Message(tuple, Generic[T]):
    to: "Actor"
    topic: TopicType
    payload: T
    reply_to: ReplyToType

    def __new__(cls, to: "Actor", topic: TopicType, payload: T, reply_to: ReplyToType):
        self = tuple.__new__(cls, (to, topic, payload, reply_to))
        self.to = to
        self.topic = topic
        self.payload = payload
        self.reply_to = reply_to


class Call(Message[T]):
    def __new__(cls, to, topic, payload, reply_to):
        super().__new__(cls, to, (CALL, topic), payload, reply_to)


class Cast(Message[T]):
    def __new__(cls, to, topic, payload, reply_to):
        super().__new__(cls, to, (CAST, topic), payload, reply_to)


class Broadcast(Cast[T]):
    def __new__(cls, to, topic, payload, reply_to):
        Message.__new__(cls, to, (BROADCAST, topic), payload, reply_to)


def uniq(items: Iterable[T], key=id) -> Iterator[T]:
    seen: Set[Tuple[int, str]] = set()
    seen_add = seen.add
    for item in items:
        k = key(item)
        if k not in seen:
            seen_add(key)
            yield item


class InvalidPattern(ValueError):
    """The given object is not a string or regex"""

    def __init__(self, value):
        msg = (
            "Pattern should be a string or an object with a "
            "``fullmatch`` method, {} given.".format(type(value))
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
        self.value = _compile_patten(value)

    def _filter(self, items: Iterable[Tuple[str, T]]) -> Iterator[T]:
        pattern = self.value

        if isinstance(pattern, str):
            return (v for k, v in items if pattern == k)

        matcher = pattern.fullmatch
        return (v for k, v in items if matcher(k))

    def filter(self, items: Iterable[Tuple[str, T]]) -> Iterator[T]:
        """Return a iterator with all the items that match the pattern"""
        return uniq(self._filter(items))

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


class Actor:
    def __init__(
        self,
        registry: "Registry",
        queue: Optional[Queue] = None,
        links: Optional[deque] = None,
    ):
        self.registry = registry
        self.queue = queue or Queue()
        self.links = links or deque()

    def start(self):
        pass

    def start_link(self, other):
        # TODO
        self.links.append(other)

    def _solve(self, ref: Union[str, "Actor"]) -> "Actor":
        if isinstance(ref, Actor):
            return ref
        return self.registry[ref]

    @overload
    def send(self, msg: Message):
        ...

    @overload  # noqa
    def send(self, payload: Any, to: Ref, topic: Union[str, Tuple[str]] = "*"):
        ...

    def send(self, payload, to, topic="*"):  # noqa
        if isinstance(payload, Message):
            msg = payload
        else:
            msg = Message(to, topic, msg, self)

        self.queue.put_nowait(Message(self._solve(msg.to), *msg[1:]))

    async def recv(self, timeout: Optional[Number] = None):
        try:
            if timeout == 0:
                return self.queue.get_nowait()
            else:
                return await asyncio.wait_for(self.queue.get(), timeout)
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            if timeout is None:
                # In theory, if not timeout is set, it should wait forever
                raise

    def cast(self, payload, to, topic="*"):
        self.send(Cast(to, topic, payload, self))

    def broadcast(self, payload, to: Union[str, Regex] = "*", topic="*"):
        for ref in Pattern(to).filter(self.registry.items()):
            self.send(Broadcast(ref, topic, payload, self))

    def send_request(
        self, payload, to: Ref, topic="*", timeout: Optional[Number] = None
    ) -> Future:
        loop = asyncio.get_running_loop()
        response = loop.create_future()
        message: Message = Call(to, topic, payload, (self, response))

        if timeout == 0:
            self.queue.put_nowait(message)
            return response
        else:

            async def _send():
                await asyncio.wait_for(self.queue.put(message), timeout)
                return await response

            return loop.create_task(_send())

    async def check_response(self, response, timeout):
        return await asyncio.wait_for(response, timeout)

    async def call(self, payload, to: Ref, topic="*", timeout: Optional[Number] = None):
        start = time.monotonic()
        response = self.send_request(payload, to, topic, timeout)
        sent_finished = time.monotonic()
        if timeout is None:
            time_left = None
        else:
            time_left = max(0, sent_finished - start)
        return await self.check_response(response, time_left)


class Registry:
    def __init__(self, actors: Optional[Dict[str, Actor]] = None):
        self.actors = actors or {}

    def __getitem__(self, key: str) -> Actor:
        return self.actors[key]

    def __contain__(self, key: str) -> bool:
        return key in self.actors

    @property
    def all(self) -> Iterator[Actor]:
        return uniq(self.actors.values())

    def items(self) -> Iterable[Tuple[str, Actor]]:
        return self.actors.items()

    def register(self, alias: Optional[str] = None, actor: Optional[Actor] = None):
        actor: Actor = actor or Actor(self)
        ref: str = alias or str(id(actor))
        if ref in self.actors:
            raise KeyError("Actor already registered", ref)
        self.actors[ref] = actor
        return actor
