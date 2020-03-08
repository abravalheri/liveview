import fnmatch
import re as _re
from pprint import pformat
from typing import Any, Callable, Iterator, Protocol, Union, cast

from ..utils import NO_VALUE

Anything = Ellipsis


class InvalidPattern(ValueError):
    """The given object is not a string or regex"""

    def __init__(self, value):
        msg = (
            f"Pattern should be a string or an object with a ``fullmatch`` method, "
            "{value} given."
        )
        super().__init__(msg, value)


FlagsType = Union[int, _re.RegexFlag]
Matcher = Callable[[Any], Any]


class Matchable(Protocol):
    """Polymorphic typing that applies to regexes or any other object that tries to
    mimic them by implementing the `~.fullmatch` method.
    """

    def fullmatch(self, value: Any) -> Any:
        """Mimics `_re.fullmatch`."""
        ...


def _compile_pattern(value) -> Matcher:
    """Factory function that compiles pattern to be matched.

    This function returns a function that accepts one argument and returns a
    truth-y/false-y value if the pattern matches or not the argument.

    If ``value`` has a ``fullmatch`` value, the same method will be returned.

    If ``value`` is ``...`` (`Ellipsis`), the returned function will **ALWAYS** match.

    Otherwise, a function that just applies the ``==`` operator will be applied.
    """
    if hasattr(value, "fullmatch"):
        return cast(Matchable, value).fullmatch

    if isinstance(value, (list, tuple)):
        return _compile_seq_pattern(value)

    if value is Ellipsis:
        return lambda _: True

    if isinstance(value, Pattern):
        return value.__call__

    return lambda x: x == value


def _compile_seq_pattern(value: Union[tuple, list]) -> Matcher:
    _pattern = tuple(_compile_pattern(p) for p in value)

    def __call__(value: Any) -> Any:
        matchers = cast(Iterator[Callable[[Any], Any]], iter(_pattern))
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

    return __call__


class Pattern:
    """Callable object that matches the __init__ arg with the __call__ arg.

    The preferable way of creating patterns however is to use the auxiliary functions:
    `re`, `glob` and `pattern`.
    """

    def __init__(self, value: Any):
        self._value = value
        self._matcher = _compile_pattern(value)

    def __call__(self, value: Any) -> Any:
        matcher = self._matcher
        try:
            return matcher(value)
        except TypeError:
            return False

    def _format_inner(self):
        return pformat(self._value, sort_dicts=False)

    def __repr__(self):
        return f"{type(self).__name__}({self._format_inner()})"


def re(value: str, flags: FlagsType = 0):
    """Given a string with a Regular Expression pattern, create a `Pattern` object."""
    return Pattern(_re.compile(value, flags))


def glob(value: str):
    """Given a string with a simple glob pattern, create a `Pattern` object."""
    return Pattern(_re.compile(fnmatch.translate(value)))


def pattern(value, *other) -> Pattern:
    """Create a pattern that mathches the argument.

    Tuples and Lists will be analysed recursively, so `re`, `glob` and `pattern` can be
    nested.
    """
    if len(other) > 0:
        value = (value, *other)

    if isinstance(value, Pattern):
        return value

    return Pattern(value)
