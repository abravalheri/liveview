import fnmatch
import re
from typing import Iterable, Iterator
from typing import Pattern as Regex
from typing import Tuple, TypeVar, Union, overload

from ..utils import NO_ARG, OptionalArg, uniq

PatternLike = Union[str, Regex, "Pattern"]
T = TypeVar("T")
S = TypeVar("S")


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
    def first(self, items: Iterable[Tuple[str, T]]) -> T:  # noqa
        ...

    def first(  # noqa
        self, items: Iterable[Tuple[str, T]], default: OptionalArg[S] = NO_ARG
    ) -> Union[T, S]:
        if default is NO_ARG:
            return next(self.filter(items))

        return next(self.filter(items), default)
