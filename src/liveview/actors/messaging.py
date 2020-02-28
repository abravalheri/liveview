import logging
from asyncio import Future
from enum import auto
from typing import TYPE_CHECKING, Generic, Optional, Tuple, TypeVar, Union

from ..utils import ReprEnum

if TYPE_CHECKING:
    from . import Actor

T = TypeVar("T")

LOGGER = logging.getLogger(__name__)


class LcmToken(ReprEnum):
    REPLY = auto()
    NOREPLY = auto()
    STOP = auto()
    DOWN = auto()
    EXIT = auto()
    IGNORE = auto()
    NORMAL = auto()
    SHUTDOWN = auto()


REPLY, NOREPLY, STOP, DOWN, EXIT, IGNORE, NORMAL, SHUTDOWN = list(LcmToken)


class SuccessToken(ReprEnum):
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


class TopicToken(ReprEnum):
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
