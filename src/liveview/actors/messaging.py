import logging
from asyncio import Future
from enum import IntEnum
from typing import TYPE_CHECKING, Callable, Generic, Optional, Tuple, TypeVar, Union

if TYPE_CHECKING:
    from . import Actor, MonitorRef as Reference  # noqa

T = TypeVar("T")
S = TypeVar("S")

LOGGER = logging.getLogger(__name__)


class Token(IntEnum):
    OTHER = -1
    OK = 0
    ERROR = 1
    CALL = 2
    CAST = 3
    REPLY = 4
    NOREPLY = 5
    STOP = 6
    DOWN = 7
    EXIT = 8
    IGNORE = 9
    NORMAL = 10
    SHUTDOWN = 11


(
    OTHER,
    OK,
    ERROR,
    CALL,
    CAST,
    REPLY,
    NOREPLY,
    STOP,
    DOWN,
    EXIT,
    IGNORE,
    NORMAL,
    SHUTDOWN,
) = list(Token)


ResponseCallback = Union[Callable[[T], S], Callable[[T], "Response[S]"]]
MappedResponse = Union["Response[T]", "Response[S]"]


class Response(tuple, Generic[T]):
    status: Token
    value: T

    def __new__(cls, status: Token, value: T):
        self = tuple.__new__(cls, (status, value))
        self.status = status
        self.value = value
        return self

    def success(self, fn: ResponseCallback) -> MappedResponse:
        if self.status == ERROR:
            return self
        try:
            result = fn(self.value)
            if isinstance(result, Response):
                return result
            return Ok(fn(self.value))
        except Exception as ex:
            return Error(ex)

    def failure(self, fn: ResponseCallback) -> MappedResponse:
        if self.status == OK:
            return self
        try:
            result = fn(self.value)
            if isinstance(result, Response):
                return result
            return Ok(fn(self.value))
        except Exception as ex:
            return Error(ex)


class Ok(Response):
    def __new__(cls, value):
        return super().__new__(cls, OK, value)


class Error(Response):
    def __new__(cls, value):
        return super().__new__(cls, ERROR, value)

    @property
    def reason(self):
        return self.value


ReplyToType = Union["Actor", Tuple["Actor", Future]]


class Message(tuple, Generic[T]):
    to: "Actor"
    categorty: Literal[OTHER, CALL, CAST, ERROR, REPLY
    topic: str
    payload: T
    sender: "Actor"
    reference: Optional["Reference"] = None
    """The parameter ``monitor_ref`` is not part of the public API of the library"""

    def __new__(
        cls,
        to: "Actor",
        topic: Topic,
        payload: T,
        sender: "Actor",
        reference: Optional["Reference"] = None,
    ):
        self = tuple.__new__(cls, (to, topic, payload, sender, reference))
        self.to = to
        self.topic = topic
        self.payload = payload
        self.sender = sender
        self.reference = reference
        return self


class Other(Message[T]):
    def __new__(cls, to, topic, payload, sender, reference=None):
        return super().__new__(cls, to, (OTHER, topic), payload, sender)


class Call(Message[T]):
    def __new__(cls, to, topic, payload, sender, reference=None):
        return super().__new__(cls, to, (CALL, topic), payload, sender)


class Cast(Message[T]):
    def __new__(cls, to, topic, payload, sender):
        return super().__new__(cls, to, (CAST, topic), payload, sender)
