"""Very simplified "actor system" based on erlang's GenServer.

Actors here do not really run in parallel, instead they run concurrently using
`asyncio`.
"""
import asyncio
import logging
from asyncio import Queue
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
        _signaling_queue: Optional[Queue] = None,
        _links: Optional[Deque["Actor"]] = None,
        _monitors: Optional[Dict[int, "Actor"]] = None,
    ):
        """Calling the constructor with arguments should be avoided.
        Arguments are part of the private API.
        """
        self._registry = _registry
        self._queue = _queue or Queue()
        # ^  Regular queue where regular messages should arrive
        self._signaling_queue = _signaling_queue or Queue()
        # ^  Privileged queue where error notifications and "reply" messages should
        #    arrive.
        #    To avoid receiving new requests while still waiting for replies or to avoid
        #    an error slipping unnoticed while we listen to regular messages, a
        #    separated queue is necessary.
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

    def _add_monitor(self, other: "Actor") -> MonitorRef:
        """Be monitored by another actor.

        The other actor will receive messages in the case something ages badly.

        Returns:
            Reference that can be used later to stop the monitoring routine.
            See `~.demonitor`.
        """
        i = self._monitor_counter
        self._monitors[i] = other
        self._monitor_counter += 1
        return MonitorRef(i)

    def _remove_monitor(self, monitor_ref: MonitorRef) -> "Actor":
        """Remove some previously defined monitoring.

        Arguments:
            monitor_ref: Output of the `~.monitor` that stated the monitoring routine.
        """
        return self._monitors.pop(monitor_ref)

    def monitor(self, other: "Actor") -> MonitorRef:
        """Start monitoring another actor.

        When monitoring another actor, the current actor will receive
        messages in the case something ages badly.

        Returns:
            Reference that can be used later to stop the monitoring routine.
            See `~.demonitor`.
        """
        return other._add_monitor(self)

    def demonitor(self, other: "Actor", monitor_ref: MonitorRef) -> "Actor":
        """Remove some previously defined monitoring.

        Arguments:
            other: Actor being monitored.
            monitor_ref: Output of the `~.monitor` that stated the monitoring routine.
        """
        return other._remove_monitor(monitor_ref)

    # ---- Messaging API ----

    def _solve(self, ref: Union[str, "Actor"]) -> "Actor":
        if isinstance(ref, Actor):
            return ref
        if self._registry is not None:
            return self._registry[ref]
        raise NotRegistered(operation="find actor", actor_reference=ref)

    def send(
        self, topic: str, payload: Any = None, *, to: Ref, wait=False
    ) -> Union[Awaitable, None]:
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
            to: Reference to the actor that will receive the message
            wait: Flag that indicates if the current actor should wait to make sure the
                message was at least delivered. When `True`, the caller should use an
                `await` directive on the return value of this function.
                `False` by default.

        Returns:
            Awaitable: if ``wait`` is `True`
        """

        target = self._solve(to)
        msg = Message(target, topic, payload, self)

        if wait:
            return target._queue.put(msg)

        target._queue.put_nowait(msg)
        return None

    async def send_error(self, msg, to: Ref):
        # TODO
        target = self._solve(to)
        await target._signaling_queue.put(msg)

    async def send_reply(self, msg, to: Ref):
        # TODO
        target = self._solve(to)
        await target._signaling_queue.put(msg)

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
            Value send back by the target actor.
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
        target = self._solve(on)
        monitor_ref = self.monitor(target)
        message: Call = Call(target, topic, payload, self, monitor_ref)
        try:
            await target._queue.put(message)
            return await self._signaling_queue.get()
            # TODO do something with error
        finally:
            self.demonitor(target, monitor_ref)

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
