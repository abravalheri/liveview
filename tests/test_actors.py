import asyncio

import pytest

from liveview.actors import (
    BROADCAST,
    CALL,
    CAST,
    FAIL,
    OK,
    Broadcast,
    Call,
    Cast,
    Fail,
    Ok,
    Pattern,
    Registry,
    Response,
    uniq
)


class TestPattern:
    @pytest.fixture
    def examples(self):
        return {"ab": 1, "abc": 2, "42": 3, "456": 4}.items()

    def test_find_str(self, examples):
        results = list(Pattern("42").filter(examples))
        assert len(results) == 1
        assert results[0] == 3

    def test_find_fnmatch(self, examples):
        results = list(Pattern("4*").filter(examples))
        assert len(results) == 2
        assert results == [3, 4]

    def test_find_re(self, examples):
        results = list(Pattern("/ab.*/").filter(examples))
        assert len(results) == 2
        assert results == [1, 2]

    def test_first(self, examples):
        assert Pattern("4*").first(examples) == 3

    def test_matcher(self, examples):
        matcher = Pattern("*a*").matcher()
        assert matcher("a")
        assert matcher("ba")
        assert matcher("aba")


class TestResponse:
    def test_map(self):
        def square(x):
            return x ** 2

        assert Response(OK, 9).map(square).value == 81
        assert Ok(9).map(square).value == 81
        error = SystemError("bla", 2)
        assert Response(FAIL, error).map(square).value is error
        assert Fail(error).map(square).value is error

    def test_fix(self):
        def square(ex):
            x = ex.args[1]
            return x ** 2

        error = SystemError("bla", 2)
        assert Response(OK, 9).fix(square).value == 9
        assert Ok(9).fix(square).value == 9
        assert Response(FAIL, error).fix(square).value == 4
        assert Fail(error).fix(square).value == 4


class TestMessage:
    def test_topic_types(self):
        assert (
            Call("actor_ref", "topic", {"some": "payload"}, "sender", None).topic[0]
            == CALL
        )
        assert (
            Cast("actor_ref", "topic", {"some": "payload"}, "sender").topic[0] == CAST
        )
        broadcast = Broadcast("actor_ref", "topic", {"some": "payload"}, "sender")
        assert broadcast.topic[0] == BROADCAST
        assert isinstance(broadcast, Cast)


def test_uniq():
    assert len(list(uniq([1, 1, 1, 2, 2, 2]))) == 2
    obj1 = object()
    obj2 = object()
    assert len(list(uniq([obj1, obj1, obj1, obj2, obj2]))) == 2


class RegistryMixin:
    @pytest.fixture
    def registry(self):
        return Registry()


class TestRegistry(RegistryMixin):
    def test_register_getitem(self, registry):
        actor = registry.register("actor")
        assert actor is not None
        assert "Actor" in actor.__class__.__name__
        assert actor is registry["actor"]

    def test_register_taken(self, registry):
        registry.register("actor")
        with pytest.raises(KeyError):
            registry.register("actor")

    def test_register_annonymous__contain(self, registry):
        actor = registry.register()
        assert str(id(actor)) in registry

    def test_all(self, registry):
        actors = [registry.register() for _ in range(5)]

        # Re register some actors
        registry.register("abc", actors[0])
        registry.register("def", actors[0])
        registry.register("ghi", actors[1])

        assert len(list(registry.all)) == 5


class ActorMixin(RegistryMixin):
    @pytest.fixture
    def actor(self, registry):
        return registry.register


class TestActor(ActorMixin):
    @pytest.mark.asyncio
    async def test_send_receive(self, actor):
        actor1 = actor()
        actor2 = actor()

        actor1.send("hello world", to=actor2)
        msg1 = await actor2.receive()
        assert msg1.payload == "hello world"
        assert msg1.sender == actor1
        actor2.send("Ok!", to=msg1.sender)
        msg2 = await actor1.receive()
        assert msg2.payload == "Ok!"
        assert msg2.sender == actor2

    @pytest.mark.asyncio
    async def test_send_receive_task(self, actor):
        actor1 = actor()
        actor2 = actor()

        async def respond():
            msg1 = await actor2.receive()
            assert msg1.sender == actor1
            assert msg1.payload == "hello world"
            actor2.send("Ok!", to=msg1.sender)

        asyncio.create_task(respond())

        actor1.send("hello world", to=actor2)
        msg2 = await actor1.receive()
        assert msg2.payload == "Ok!"
        assert msg2.sender == actor2

    @pytest.mark.asyncio
    async def test_call_receive(self, actor):
        actor1 = actor()
        actor2 = actor()

        async def respond():
            msg = await actor2.receive()
            assert msg.topic == (CALL, "some-topic")
            assert msg.sender == actor1
            reply = msg.reply
            if not reply.cancelled():
                reply.set_result(42)

        asyncio.create_task(respond())

        response = await actor1.call(actor2, "some-topic", {"some-payload"})
        assert response == 42
