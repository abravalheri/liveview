"""Collection of classes/functions for throwing exceptions in the package"""
from typing import Optional, overload


@overload
def init_with_docstring(self, *args) -> None:
    ...


@overload
def init_with_docstring(self, *args, **kwargs) -> None:
    ...


def init_with_docstring(self, msg: Optional[str] = None, *args, **kwargs):  # noqa
    msg = msg or self.__class__.__doc__
    args = list(args)
    if kwargs:
        args.append(kwargs)
    if msg is not None:
        args.insert(0, msg)
    Exception.__init__(self, *args)
