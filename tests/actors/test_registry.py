import pytest

from liveview.actors import Actor
from liveview.actors.registry import Registry


class RegistryMixin:
    @pytest.fixture
    def registry(self):
        return Registry()


class TestRegistry(RegistryMixin):
    def test_register_getitem(self, registry):
        actor = registry.register(Actor(), "actor")
        assert actor is not None
        assert "Actor" in actor.__class__.__name__
        assert actor is registry["actor"]

    def test_register_taken(self, registry):
        registry.register(Actor(), "actor")
        with pytest.raises(KeyError):
            registry.register(Actor(), "actor")

    def test_register_annonymous__contain(self, registry):
        actor = registry.register(Actor())
        assert id(actor) in registry

    def test_all(self, registry):
        actors = [registry.register(Actor()) for _ in range(5)]

        # Re register some actors
        registry.register(actors[0], "abc")
        registry.register(actors[0], "def")
        registry.register(actors[1], "ghi")

        assert len(list(registry.actors)) == 5

    def test_get_aliases(self, registry):
        actor = registry.register(Actor(), alias="a1")
        registry.register(actor, alias="a2")
        assert list(registry.get_aliases(actor)) == ["a1", "a2"]
        assert registry.get_alias(actor) == "a1"

        actor = registry.register(Actor())
        assert list(registry.get_aliases(actor)) == []
        assert registry.get_alias(actor) is None
