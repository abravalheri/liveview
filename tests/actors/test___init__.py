import asyncio

import pytest

from liveview.actors import (
    CALL,
    CAST,
    FAIL,
    OK,
    Actor,
    Broadcast,
    Call,
    Cast,
    Fail,
    Ok,
    Pattern,
    Response,
    uniq
)

from .test_registry import RegistryMixin


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
        assert broadcast.topic[0] == CAST
        assert isinstance(broadcast, Cast)


def test_uniq():
    assert len(list(uniq([1, 1, 1, 2, 2, 2]))) == 2
    obj1 = object()
    obj2 = object()
    assert len(list(uniq([obj1, obj1, obj1, obj2, obj2]))) == 2


class ActorMixin(RegistryMixin):
    @pytest.fixture
    def actor(self, registry):
        return lambda alias=None: registry.register(Actor(), alias)


class TestActor(ActorMixin):
    @pytest.mark.asyncio
    async def test_send_receive(self, actor):
        actor1 = actor()
        actor2 = actor()

        actor1.send("hello world", to=actor2)
        msg1 = await actor2.receive()
        assert msg1.topic == "hello world"
        assert msg1.sender == actor1
        actor2.send("Ok!", to=msg1.sender)
        msg2 = await actor1.receive()
        assert msg2.topic == "Ok!"
        assert msg2.sender == actor2

    @pytest.mark.asyncio
    async def test_send_receive_task(self, actor):
        actor1 = actor()
        actor2 = actor()

        async def respond():
            msg1 = await actor2.receive()
            actor2.send("Ok!", to=msg1.sender)

        asyncio.create_task(respond())

        await actor1.send("hello world", to=actor2, wait=True)
        msg2 = await actor1.receive()
        assert msg2.topic == "Ok!"
        assert msg2.sender == actor2

    @pytest.mark.asyncio
    async def test_call_receive(self, actor):
        actor1 = actor()
        actor2 = actor()

        async def respond():
            msg = await actor2.receive()
            # Asserts inside asyncio will not be considered
            reply = msg.reply
            result = 0
            if msg.topic == (CALL, "some-topic") and msg.sender == actor1:
                result = 42
            if not reply.cancelled():
                reply.set_result(result)

        asyncio.create_task(respond())

        response = await actor1.call("some-topic", {"some-payload"}, on=actor2)
        assert response == 42

    @pytest.mark.asyncio
    async def test_cast_receive(self, actor):
        actor1 = actor()
        actor2 = actor()

        async def send():
            actor1.cast("some-topic", 42, to=actor2)

        asyncio.create_task(send())

        msg = await actor2.receive()
        assert msg.topic == (CAST, "some-topic")
        assert msg.sender == actor1
        assert msg.payload == 42

    @pytest.mark.asyncio
    async def test_broadcast_receive(self, actor):
        actor1 = actor()
        others = [actor() for _ in range(5)]

        async def send():
            actor1.broadcast("some-topic", 42)

        asyncio.create_task(send())

        msgs = await asyncio.gather(*[a.receive() for a in others])
        for msg in msgs:
            assert msg.topic == (CAST, "some-topic")
            assert msg.sender == actor1
            assert msg.payload == 42

    @pytest.mark.asyncio
    async def test_broadcast_receive__filter(self, actor):
        actor1 = actor()
        others = [actor("i" * i) for i in range(5)]

        async def send():
            actor1.broadcast("some-topic", 42, to="iii*")
            # Now we broadcast for just the actors named with 3 or more 'i'

        asyncio.create_task(send())

        done, pending = await asyncio.wait([a.receive() for a in others], timeout=0.5)
        assert len(done) == 2
        for p in pending:
            p.cancel()
