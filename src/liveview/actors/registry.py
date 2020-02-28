from collections import defaultdict, deque
from typing import (
    TYPE_CHECKING,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union
)

from ..exceptions import init_with_docstring

if TYPE_CHECKING:
    from . import Actor


class AliasTaken(KeyError):
    """Trying to register an actor with an alias that is already taken"""

    __init__ = init_with_docstring


class NotFound(KeyError):
    """No actor corresponding tho the given reference was registered in the Registry"""

    __init__ = init_with_docstring


class Registry:
    def __init__(
        self,
        actors: Optional[List["Actor"]] = None,
        aliases: Optional[Dict[str, int]] = None,
    ):
        self._actors: Dict[int, "Actor"] = {actor.id: actor for actor in (actors or [])}
        self._aliases: Dict[str, int] = aliases or {}
        rev_aliases: Dict[int, Deque[str]] = defaultdict(deque)
        for alias, id_ in self._aliases.items():
            rev_aliases[id_].append(alias)
        self._rev_aliases = rev_aliases

    def __getitem__(self, ref: Union[str, int]) -> "Actor":
        try:
            if isinstance(ref, str):
                id_ = self._aliases[ref]
            else:
                id_ = ref

            return self._actors[id_]
        except KeyError as ex:
            raise NotFound(reference=ref) from ex

    def __contains__(self, actor: Union[int, str, "Actor"]) -> bool:
        if isinstance(actor, int):
            ref: Union[int, None] = actor
        elif isinstance(actor, str):
            ref = self._aliases.get(actor)
        else:
            ref = id(actor)
        return ref in self._actors

    def __iter__(self) -> Iterator["Actor"]:
        return iter(self._actors.values())

    @property
    def actors(self) -> Iterator["Actor"]:
        return iter(self._actors.values())

    @property
    def aliases(self) -> Iterator[Tuple[str, "Actor"]]:
        return ((k, self._actors[v]) for k, v in self._aliases.items())

    def register(self, actor: "Actor", alias: Optional[str] = None) -> "Actor":
        actor: "Actor" = actor
        id_ = id(actor)
        self._actors[id_] = actor
        if alias is not None:
            if alias in self._aliases:
                raise AliasTaken(alias=alias)
            self._aliases[alias] = id_
            self._rev_aliases[id_].append(alias)

        actor._registry = self  # Accessing protected API
        return actor

    def unregister(self, actor: Optional["Actor"] = None, alias: Optional[str] = None):
        if alias is not None:
            self._aliases.pop(alias)
        if actor is not None:
            id_ = id(actor)
            self._actors.pop(id_, None)
            # Also remove all the aliases this actor may have
            aliases: Iterable[str] = self._rev_aliases.pop(id_, [])
            for alias in aliases:
                self._aliases.pop(alias, None)
