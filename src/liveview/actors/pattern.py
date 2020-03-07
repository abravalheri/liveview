import fnmatch
import re
from pprint import pformat
from typing import (
    Any,
    AnyStr,
    Callable,
    Iterable,
    Iterator,
    Match,
    Optional,
    Pattern as Regex,
    Protocol,
    TypeVar,
    Union,
    cast
)

from ..utils import NO_VALUE, suppress

Anything = Ellipsis


class InvalidPattern(ValueError):
    """The given object is not a string or regex"""

    def __init__(self, value):
        msg = (
            f"Pattern should be a string or an object with a ``fullmatch`` method, "
            "{value} given."
        )
        super().__init__(msg, value)


T = TypeVar("T")
S = TypeVar("S")
M = TypeVar("M", bound="Matchable")
FlagsType = Union[int, re.RegexFlag]
Matcher = Union[
    Callable[[Any], Any],
    Callable[[AnyStr], Optional[Match]],
    Callable[[AnyStr, FlagsType], Optional[Match]],
]
Matchable = Union["_Matchable", Regex]
PatternLike = Union[str, Matchable, "Pattern"]


class _Matchable(Protocol):
    """Polymorphic typing that applies to regexes or any other object that tries to
    mimic them by implementing the `~.fullmatch` method.
    """

    def fullmatch(self, value: Any) -> Any:
        """Mimics `re.fullmatch`."""
        ...

    @staticmethod
    def is_instance(value):
        return hasattr(value, "fullmatch") and callable(value.fullmatch)


def compile_pattern(value) -> Matcher:
    """Compile pattern to be matched.

    This function returns a function that accepts one argument and returns a
    truth-y/false-y value if the pattern matches or not the argument.

    If ``value`` is a string and have the format ``/.../`` (starts and ends with a
    ``/``) it will be compiled to a regex.

    If ``value`` is a string and have any of the characters ``*, ?, [`` it will be
    compiled to a regex according to fnmatch.

    If ``value`` has a ``fullmatch`` value, the same method will be returned.

    If ``value`` is ``...`` (`Ellipsis`), the returned function will **ALWAYS** match.

    Otherwise, a function that just applies the ``==`` operator will be applied.
    """
    if isinstance(value, str):
        if value[0] == "/" and value[-1] == "/":
            # TODO implement regex modifiers
            return suppress(TypeError)(re.compile(value.strip("/")).fullmatch)

        if any(ch in value for ch in ("*", "?", "[")):
            return suppress(TypeError)(re.compile(fnmatch.translate(value)).fullmatch)

    if _Matchable.is_instance(value):
        return suppress(TypeError)(value.fullmatch)

    if value is Ellipsis:
        return lambda _: True

    return lambda x: x == value


class Pattern:
    def __new__(cls, value: PatternLike) -> "Pattern":
        if isinstance(value, cls):
            return value
        return object.__new__(cls)

    def __init__(self, value: Union[str, Matchable]):
        self._value = value
        self._matcher: Matcher = compile_pattern(value)

    @property
    def matcher(self) -> Matcher:
        return self._matcher

    def __call__(self, value: Any) -> Any:
        matcher = cast(Callable[[Any], Any], self._matcher)
        return matcher(value)

    def __str__(self):
        return f"{type(self).__name__}({pformat(self._value, sort_dicts=False)})"

    def __repr__(self):
        return (
            f"<{type(self).__name__} at {id(self):#x} "
            "{pformat(self._value, sort_dicts=False)}>"
        )


class TuplePattern(Pattern):
    def __new__(cls, value: Union["TuplePattern", tuple]) -> "TuplePattern":
        if isinstance(value, cls):
            return value
        return object.__new__(cls)

    def __init__(self, value: Any):
        if not isinstance(value, tuple):
            value = (value,)
        self._value = value
        self._pattern = tuple(compile_pattern(p) for p in value)

    def __call__(self, value: Iterable) -> bool:
        matchers = cast(Iterator[Callable[[Any], Any]], iter(self._pattern))
        items = iter(value)
        if any(not matcher(item) for matcher, item in zip(matchers, items)):
            return False
        # We also need to check if they have the same size
        if not (
            next(matchers, NO_VALUE) is NO_VALUE and next(items, NO_VALUE) is NO_VALUE
        ):
            # If they had the same size, both iterators will be exhausted
            # and the `next` would return the default value
            return False
        return True

    @property
    def matcher(self) -> Matcher:
        return self.__call__


def pattern(value, *other) -> Union[Pattern, TuplePattern]:
    if len(other) > 0:
        value = (value, *other)

    if isinstance(value, (tuple, TuplePattern)):
        return TuplePattern(value)

    return Pattern(value)
