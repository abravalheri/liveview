"""Very simplified "actor system" based on erlang's GenServer.

Actors here do not really run in parallel, instead they run concurrently using
`asyncio`.
"""
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
    NewType,
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
MonitorRef = NewType("MonitorRef", int)

LOGGER = logging.getLogger(__name__)


class NotRegistered(SystemError):
    """Actor is not registered yet and this operation requires the actor to be
    registered in a `Registry` object
    """

    __init__ = init_with_docstring


class Actor:
    """Main interface of the "Actor" system."""

    def __init__(
        self,
        _registry: Optional["Registry"] = None,
        _queue: Optional[Queue] = None,
        _links: Optional[Deque["Actor"]] = None,
        _monitors: Optional[Dict[int, "Actor"]] = None,
    ):
        """Calling the constructor with arguments should be avoided.
        Arguments are part of the private API.
        """
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
        """Actor identifier."""
        return id(self)

    # ---- Life-Cycle API ----

    def link(self, other: "Actor"):
        """Link the current actor to other."""
        self._links.append(other)
        other._links.append(self)

    def unlink(self, other: "Actor"):
        """Remove link between this actor and other."""
        self._links.remove(other)
        other._links.remove(self)

    def start(self, init_arg, *, link: Optional["Actor"] = None, **options):
        """Start this actor."""
        if link:
            self.link(link)
        # TODO

    def spawn(self, other: "Actor", init_arg, *, alias=None, link=False, **options):
        """Start other actor (using the current actor's registry)."""
        if self._registry is not None:
            if other.id not in self._registry:
                self._registry.register(other)
            if alias is not None and alias not in self._registry:
                self._registry.register(other, alias=alias)
        if link:
            options["link"] = self
        other.start(init_arg, **options)

    def monitor(self, other: "Actor") -> MonitorRef:
        """Start monitoring another actor.

        When monitoring another actor, the current actor will receive
        messages in the case something ages badly.

        Returns:
            Reference that can be used later to stop the monitoring routine.
            See `~.demonitor`.
        """
        i = self._monitor_counter
        self._monitors[i] = other
        self._monitor_counter += 1
        return i

    def demonitor(self, monitor_ref: MonitorRef) -> "Actor":
        """Given some previously defined monitoring.

        Arguments:
            monitor_ref: Output of the `~.monitor` that stated the monitoring routine.
        """
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
        """Send a message from this actor to another one.

        Messages sent via this method are usually handled with a `~.handle_info`
        callback (or manually with the `~.receive` method).

        Arguments:
            topic: Main part of the message. The actor that receives this message will
                try to match on the topic to try to determine which callback will be
                applied. See `handle_info`.
            payload: Extra object that will be send along side with ``topic``.
                Ideally this should be serializable (JSON/pickle).

        Keyword Arguments:
            reply: Flag that indicates if the current actor should wait for a reply
                message. When true, the method will return an `Awaitable` object (with
                the response).  `False` by default.
            to: Reference to the actor that will receive the message
            wait: Flag that indicates if the current actor should wait to make sure the
                message was at least delivered. When `True`, the caller should use an
                `await` directive. `False` by default.

        Note:
            When both ``reply`` and ``wait`` are `True`, the caller should `await` first
            to the message to be sent and then for the response::

                reply = await actor.send(msg, to=other, reply=True, wait=True)
                response = await reply
        """

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
    def receive(self, wait: bool) -> Any:  # noqa
        ...

    def receive(self, wait=True):  # noqa
        """Receive a message from other actor.
        When wait is `False`, will return `None` if queue is empty.

        This is a low level method, a `~.handle_info` callback should cover the majority
        of the use cases.

        Arguments:
            wait: When `True` (default) this method works asynchronously and therefore
                should be used with an `await` directive. When `False` the method will
                return immediately (if no message is received `None` will be passed).
        """
        if wait:
            return self._queue.get()
        else:
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return None

    def cast(self, topic: str, payload: Any = None, *, to: Ref):
        """Send (cast) a message to another actor, without waiting for answer.

        The target actor will process this message using a `~.handle_cast` callback.
        The arguments of this method are similar to those in `~.send`.
        """
        to = self._solve(to)
        return to._queue.put_nowait(Cast(to, topic, payload, self))

    def broadcast(
        self,
        topic: str,
        payload: Any = None,
        *,
        to: Union[str, Regex, List["Actor"], None] = None,
    ):
        """Send (cast) a message to a group of actors, without waiting for answer.

        This method works similarly to `~.cast` however needs a list of actors or a
        valid name pattern as the ``to`` argument, so it can send multiple messages.

        Raises:
            NotRegistered: If a pattern is used but the actor is not registered in any
                `registry <Registry>`.
        """
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
        """Send a ``call`` message to an actor synchronously and wait for the answer.

        The target actor will process this message using a `~.handle_call` callback.
        The arguments of this method are similar to those in `~.send`.

        Returns:
            An `Awaitable` object with the value send back by the target actor.
        """
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
