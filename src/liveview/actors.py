import asyncio
import fnmatch
import logging
import re
from asyncio import Future, Queue
from collections import deque
from enum import Enum, auto
from typing import Any, Awaitable, Dict, Generic, Iterable, Iterator, List, Optional
from typing import Pattern as Regex
from typing import Set, Tuple, TypeVar, Union, overload

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
    BROADCAST = auto()


CALL, CAST, BROADCAST = list(TopicToken)
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

    def send_nowait(self):
        """This method is not part of the public API of the library"""
        self.to.queue.put_nowait(self)
        return self.reply

    async def send(self) -> Optional[Future]:
        """This method is not part of the public API of the library"""
        await self.to.queue.put(self)
        return self.reply


class Call(Message[T]):
    def __new__(cls, to, topic, payload, sender, reply):
        return super().__new__(cls, to, (CALL, topic), payload, sender, reply)


class Cast(Message[T]):
    reply: None = None

    def __new__(cls, to, topic, payload, sender):
        return super().__new__(cls, to, (CAST, topic), payload, sender)


class Broadcast(Cast[T]):
    reply: None = None

    def __new__(cls, to, topic, payload, sender):
        return Message.__new__(cls, to, (BROADCAST, topic), payload, sender)


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
            "Pattern should be a string or an object with a "
            "``fullmatch`` method, {} given.".format(type(value))
        )
        super().__init__(msg, value)


class RegisterKeyTaken(KeyError):
    """Trying to register an actor with a name that is already taken"""

    def __init__(self, ref):
        super().__init__("There is already an actor registered under this key", ref)


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


class Actor:
    def __init__(
        self,
        registry: "Registry",
        queue: Optional[Queue] = None,
        links: Optional[deque] = None,
        monitors: Optional[Dict[int, "Actor"]] = None,
    ):
        self.registry = registry
        self.queue = queue or Queue()
        self.links = links or deque()
        self.monitors: Dict[int, "Actor"] = monitors or {}
        self._monitor_counter = 0

    @property
    def id(self):
        return str(id(self))

    def link(self, other):
        """Link the current actor to other"""
        self.links.append(other)
        other.links.append(self)

    def unlink(self, other):
        """Remove link between this actor and other"""
        self.links.remove(other)
        other.links.remove(self)

    def start(self):
        """Start this actor"""
        # TODO

    def start_link(self, other):
        """`link` this actor to other and `start` it subsequently

        Opposite of `spawn_link`.
        """
        self.link(other)
        self.start()

    def spawn(self, other):
        """Start other actor (uses the same registry)"""
        if other.id not in self.registry:
            self.registry.register(other)
        other.start(self)

    def spawn_link(self, other):
        """`link` other actor to this one and start it subsequently.

        Opposite of `start_link`.
        """
        if other.id not in self.registry:
            self.registry.register(other)
        other.start_link(self)

    def monitor(self, other):
        i = self._monitor_counter
        self.monitors[i] = other
        self._monitor_counter += 1
        return i

    def demonitor(self, monitor_ref: int):
        return self.monitors.pop(monitor_ref)

    def _solve(self, ref: Union[str, "Actor"]) -> "Actor":
        if isinstance(ref, Actor):
            return ref
        return self.registry[ref]

    def send(
        self, topic: str, payload: Any = None, reply=False, *, to: Ref, wait=False
    ) -> Union[Future, Awaitable, None]:
        """Send a message from this actor to another one"""

        if reply:
            loop = asyncio.get_running_loop()
            response: Optional[Future] = loop.create_future()
        else:
            response = None

        to = self._solve(to)
        msg = Message(to, topic, payload, self, response)
        if wait:
            return msg.send()
        else:
            return msg.send_nowait()

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
            return self.queue.get()
        else:
            try:
                return self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return None

    def cast(self, topic: str, payload: Any = None, *, to: Ref):
        """Send (cast) a message to another actor, without waiting for answer"""
        return Cast(self._solve(to), topic, payload, self).send_nowait()

    def broadcast(
        self,
        topic: str,
        payload: Any = None,
        *,
        to: Union[str, Regex, List["Actor"]] = "*",
    ):
        if isinstance(to, list):
            receivers: Iterator["Actor"] = (self._solve(ref) for ref in to)
        else:
            receivers = Pattern(to).filter(self.registry.items())
        for ref in receivers:
            Broadcast(ref, topic, payload, self).send_nowait()

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

        await message.send()
        return await reply

    def terminate(self, reason):
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


class Registry:
    def __init__(self, actors: Optional[Dict[str, Actor]] = None):
        self.actors = actors or {}

    def __getitem__(self, key: str) -> Actor:
        return self.actors[key]

    def __contains__(self, key: str) -> bool:
        return key in self.actors

    def __iter__(self) -> Iterator[str]:
        return iter(self.actors)

    @property
    def all(self) -> Iterator[Actor]:
        return uniq(self.actors.values())

    def items(self) -> Iterable[Tuple[str, Actor]]:
        return self.actors.items()

    def register(self, alias: Optional[str] = None, actor: Optional[Actor] = None):
        actor: Actor = actor or Actor(self)
        ref: str = alias or str(id(actor))
        if ref in self.actors:
            raise RegisterKeyTaken(ref)
        self.actors[ref] = actor
        return actor
