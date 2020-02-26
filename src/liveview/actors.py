import fnmatch
import re
from asyncio import Queue
from functools import singledispatch
from typing import Any, Dict, Iterator, Optional, Pattern, Tuple, Union, cast
from uuid import uuid4

Matcher = Union[str, Pattern]


@singledispatch
def compile_pattern(ref: Matcher):
    """Compile pattern to be matched.

    If the string have the format ``/.../`` (starting and leading ``/``) it
    will be compiled to a regex object.

    If the string have any of the characters ``*, ?, [`` it will be compiled
    according to fnmatch
    """
    if not hasattr(ref, "fullmatch"):
        msg = (
            "Pattern should be a string or an object with a "
            "``fullmatch`` method, {} given.".format(type(ref))
        )
        raise ValueError(msg, ref)
    return ref


@compile_pattern.register(str)
def _compile_str(ref: str):
    if ref[0] == "/" and ref[-1] == "/":
        return re.compile(ref.strip("/"))

    if any(ch in ref for ch in ("*", "?", "[")):
        return re.compile(fnmatch.translate(ref))

    return ref


def _normalise_msg(
    to: str, topic: str, msg: tuple, from_: str
) -> Tuple[str, str, Any, str]:
    args = list(msg)
    if len(args) < 2:
        args.insert(0, topic)
    if len(args) < 3:
        args.insert(0, to)
    args[4] = from_

    return cast(Tuple[str, str, Any, str], args[0:4])


class Channel:
    def __init__(self, owner: str, registry: "Registry", queue: Optional[Queue] = None):
        self.owner = owner
        self.queue = queue or Queue()

    async def send(self, *msg, topic="*", to="*"):
        await self.queue.put(_normalise_msg(to, topic, msg, self.owner))

    def send_nowait(self, *msg, topic="*", to="*"):
        self.queue.put_nowait(_normalise_msg(to, topic, msg, self.owner))

    async def recv(self):
        return await self.queue.get()

    def recv_nowait(self,):
        return self.queue.get_nowait()


@singledispatch
def _find(ref: Matcher, channels: Dict[str, Channel]) -> Iterator[Channel]:
    # Single dispatch just leave Patter for consideration
    ref = cast(Pattern, ref)
    return (ch for k, ch in channels.items() if ref.fullmatch(k))


@_find.register
def _find_str(ref: str, channels: Dict[str, Channel]):
    return (ch for k, ch in channels.items() if ref == k)


class Registry:
    def __init__(self, channels: Optional[Dict[str, Channel]] = None):
        self.channels = channels or {}

    def find(self, ref: Matcher) -> Iterator[Channel]:
        return _find(compile_pattern(ref), self.channels)

    def register(self, name: Optional[str] = None, channel: Optional[Channel] = None):
        ref: str = name or str(uuid4())
        channel: Channel = channel or Channel(ref, self)
        if ref in self.channels:
            raise KeyError("Actor already registered", ref)
        self.channels[ref] = channel
