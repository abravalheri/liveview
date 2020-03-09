import asyncio

import pytest

from liveview.actors.mailbox import Mailbox
from liveview.actors.pattern import Anything, glob, re


class TestMailbox:
    def test_pedicates(self):
        m = Mailbox(maxsize=1)
        assert m.empty()

        m.put_nowait(1)
        assert m.full()

    @pytest.mark.asyncio
    async def test_put_nowait_get(self):
        m = Mailbox()

        async def getter():
            return await m.get()

        async def putter():
            m.put_nowait(4)
            return "DONE"

        a, b = await asyncio.gather(getter(), putter(), return_exceptions=True)
        assert a == 4
        assert b == "DONE"

    @pytest.mark.asyncio
    async def test_put_get(self):
        m = Mailbox()

        async def getter():
            return await m.get()

        async def putter():
            await m.put(4)
            return "DONE"

        a, b = await asyncio.gather(getter(), putter(), return_exceptions=True)
        assert a == 4
        assert b == "DONE"

    def test_get_nowait(self):
        m = Mailbox()
        assert m.get_nowait("DEFAULT") == "DEFAULT"

        with pytest.raises(asyncio.QueueEmpty):
            assert m.get_nowait()

        m.put_nowait(42)
        assert m.get_nowait() == 42

    def test_select_nowait(self):
        m = Mailbox()
        for value in ("abc-123", "feg", ("a", 1), "abc-456", "hjk-000"):
            m.put_nowait(value)

        assert m.select_nowait(("a", Anything)) == ("a", 1)
        assert m.qsize() == 4
        assert m.select_nowait(glob("feg*")) == "feg"
        assert m.qsize() == 3
        assert m.get_nowait() == "abc-123"
        assert m.qsize() == 2
        assert m.select_nowait(re("abc.*")) == "abc-456"
        assert m.qsize() == 1
        assert m.get_nowait() == "hjk-000"
        assert m.qsize() == 0

    @pytest.mark.asyncio
    async def test_select(self):
        m = Mailbox()

        async def selector():
            assert await m.select(("a", Anything)) == ("a", 1)
            assert m.qsize() == 4
            assert await m.select(glob("feg*")) == "feg"
            assert m.qsize() == 3
            assert await m.get() == "abc-123"
            assert m.qsize() == 2
            assert await m.select(re("abc.*")) == "abc-456"
            assert m.qsize() == 1
            assert await m.get() == "hjk-000"
            assert m.qsize() == 0

            return "ALL GOOD"

        async def putter():
            for value in ("abc-123", "feg", ("a", 1), "abc-456", "hjk-000"):
                await m.put(value)
                await asyncio.sleep(0)

            return "ALL GOOD"

        a, b = await asyncio.gather(selector(), putter(), return_exceptions=True)
        assert a == b == "ALL GOOD"

    @pytest.mark.only
    async def test_select_callback(self):
        m = Mailbox()

        async def selector(selection):
            value = await m.select(selection)
            m.task_done()
            return value

        async def putter(*values):
            for value in values:
                await m.put(value)
                await asyncio.sleep(0)
            return "ALL GOOD"

        SELECTION = {
            ("a", ...): (lambda x: ("MATCH1", x)),
            re("asdf.*"): (lambda x: ("MATCH2", x)),
        }

        example1 = (
            selector(SELECTION),
            putter(1, 2, "qwerty", ("b", 1), ("a", 42), "asdfg"),
        )
        a, b = await asyncio.gather(*example1, return_exceptions=True)
        example2 = (
            selector(SELECTION),
            putter(1, 2, "qwerty", "asdfg", ("b", 1), ("a", 42)),
        )
        c, d = await asyncio.gather(*example2, return_exceptions=True)
        assert b == d == "ALL GOOD"
        assert a == ("MATCH1", ("a", 42))
        assert c == ("MATCH2", "asdfg")
