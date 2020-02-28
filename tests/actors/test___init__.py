import asyncio

import pytest

from liveview.actors import Actor
from liveview.actors.messaging import CALL, CAST

from .test_registry import RegistryMixin


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
