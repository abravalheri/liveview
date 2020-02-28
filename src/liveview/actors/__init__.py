import asyncio
import logging
from asyncio import Future, Queue
from collections import deque
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Deque,
    Dict,
    Iterator,
    List,
    Optional
)
from typing import Pattern as Regex
from typing import Tuple, TypeVar, Union, overload

from ..exceptions import init_with_docstring
from .messaging import CALL, CAST, OTHER, Broadcast, Call, Cast, Message, TopicToken
from .pattern import Pattern

if TYPE_CHECKING:
    from .registry import Registry  # noqa

T = TypeVar("T")
S = TypeVar("S")
Ref = Union[str, "Actor"]
Recipient = TypeVar("Recipient", str, "Actor", Tuple["Actor", Awaitable])

LOGGER = logging.getLogger(__name__)


class NotRegistered(SystemError):
    """Actor is not registered yet and this operation requires the actor to be
    registered in a `Registry` object
    """

    __init__ = init_with_docstring


class Actor:
    def __init__(
        self,
        _registry: Optional["Registry"] = None,
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
