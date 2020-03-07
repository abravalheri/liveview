"""Very simplified "actor system" based on erlang's GenServer.

Actors here do not really run in parallel, instead they run concurrently using
`asyncio`.
"""
import asyncio
import logging
from asyncio import Queue
from collections import deque
from enum import auto
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
    Optional,
    Pattern as Regex,
    Tuple,
    TypeVar,
    Union,
    overload
)

from ..exceptions import init_with_docstring
from .messaging import (
    CALL,
    CAST,
    DOWN,
    ERROR,
    EXIT,
    IGNORE,
    OK,
    OTHER,
    SHUTDOWN,
    STOP,
    Call,
    Cast,
    Error,
    Message,
    Other,
    ReprEnum,
    Response,
    Token as TopicToken
)
from .pattern import pattern

if TYPE_CHECKING:
    from .registry import Registry  # noqa

T = TypeVar("T")
S = TypeVar("S")
Ref = Union[str, "Actor"]
Recipient = TypeVar("Recipient", str, "Actor", Tuple["Actor", Awaitable])
MonitorRef = NewType("MonitorRef", int)

LOGGER = logging.getLogger(__name__)


class CallbackToken(ReprEnum):
    INIT = auto()
    TERMINATE = auto()


INIT, TERMINATE = list(CallbackToken)


class NotRegistered(SystemError):
    """Actor is not registered yet and this operation requires the actor to be
    registered in a `Registry` object.
    """

    __init__ = init_with_docstring


class UnexpectedResponse(SystemError):
    """Received response does not comply with the callback specification."""

    __init__ = init_with_docstring


def _get_nowait(queue):
    try:
        return queue.get_nowait()
    except asyncio.QueueEmpty:
        return None


def _extract_error(msg) -> Optional[Response]:
    if isinstance(msg, Exception):
        return Error(msg)
    if (
        isinstance(msg, tuple)
        and len(msg) == 2
        and msg[0] in (ERROR, DOWN, EXIT, SHUTDOWN)
    ):
        return Response(*msg)

    return None


class Actor:
    """Main interface of the "Actor" system."""

    def __init__(
        self,
        _registry: Optional["Registry"] = None,
        _queue: Optional[Queue] = None,
        _reply_queue: Optional[Queue] = None,
        _links: Optional[Deque["Actor"]] = None,
        _monitors: Optional[Dict[int, "Actor"]] = None,
    ):
        """Calling the constructor with arguments should be avoided.
        Arguments are part of the private API.
        """
        self._registry = _registry
        self._queue = _queue or Queue()
        # ^  Regular queue where regular messages should arrive
        self._reply_queue = _reply_queue or Queue()
        # ^  Privileged queue where error notifications and "reply" messages should
        #    arrive.
        #    To avoid receiving new requests while still waiting for replies or to avoid
        #    an error slipping unnoticed while we listen to regular messages, a
        #    separated queue is necessary.
        self._links = _links or deque()
        self._monitors: Dict[int, "Actor"] = _monitors or {}
        self._monitor_counter = 0
        self._callbacks: Dict[Union[TopicToken, CallbackToken], List[Callable]] = {
            CALL: [],
            CAST: [],
            OTHER: [],
            INIT: [],
            TERMINATE: [],
        }
        self._loop: Optional[Awaitable] = None
        self._running = False

    # ---- Metadata API ----

    @property
    def id(self):
        """Actor identifier."""
        return id(self)

    def __str__(self):
        alias = self._registry.get_alias(self) if self._registry else None
        return f"<Actor id={self.id} alias={alias}>"

    # ---- Implementation ----

    async def _run(self, init_arg, **options):
        try:
            while self._running:
                # Process received message
                msg, = await asyncio.gather(self._queue.get(), return_exceptions=True)
                self._handle_message(msg)
                self._queue.task_done()
        except Exception as ex:
            self._handle_error(Error(ex))

    async def _handle_message(self, msg):
        error = _extract_error(msg)
        if error:
            self._handle_error(error)

    async def _handle_call(self, msg):
        pass

    async def _handle_cast(self, msg):
        pass

    async def _handle_info(self, msg):
        pass

    async def _handle_error(self, msg):
        self._running = False
        # TODO

    # ---- Life-Cycle API ----

    def link(self, other: "Actor"):
        """Link the current actor to other."""
        self._links.append(other)
        other._links.append(self)

    def unlink(self, other: "Actor"):
        """Remove link between this actor and other."""
        self._links.remove(other)
        other._links.remove(self)

    async def start(self, init_arg, *, link: Optional["Actor"] = None, **options):
        """Start this actor."""
        if link:
            self.link(link)
        # TODO
        response = await self._init(init_arg)
        if response.status == STOP:
            return Error(response.reason)
        if response.status == IGNORE:
            return IGNORE
        if response.status != OK:
            return Error(UnexpectedResponse(response, "Expecting OK, IGNORE or STOP"))

        state = response.value
        self._running = True
        self._loop = asyncio.create_task(self._run(state))
        return self._loop

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
        msg: Message = Other(target, topic, payload, self)

        if wait:
            return target._queue.put(msg)

        target._queue.put_nowait(msg)
        return None

    async def send_reply(self, msg, to: Ref):
        # TODO
        target = self._solve(to)
        await target._reply_queue.put(msg)

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
            return _get_nowait(self._queue)

    def cast(self, topic: str, payload: Any = None, *, to: Ref):
        """Send (cast) a message to another actor, without waiting for answer.

        The target actor will process this message using a `~.handle_cast` callback.
        The arguments of this method are similar to those in `~.send`.
        """
        target = self._solve(to)
        return target._queue.put_nowait(Cast(target, topic, payload, self))

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
            match = pattern(to)
            receivers = (v for k, v in self._registry.aliases if match(k))

        for target in receivers:
            target._queue.put_nowait(Cast(target, topic, payload, self))

    async def call(self, topic: str, payload: Any = None, *, on: Ref):
        """Send a ``call`` message to an actor synchronously and wait for the answer.

        The target actor will process this message using a `~.handle_call` callback.
        The arguments of this method are similar to those in `~.send`.

        Returns:
            Value send back by the target actor.
        """
        target = self._solve(on)
        monitor_ref = self.monitor(target)
        message: Call = Call(target, topic, payload, self, monitor_ref)
        try:
            await target._queue.put(message)
            return await self._reply_queue.get()
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
