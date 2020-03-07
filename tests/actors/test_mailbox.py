import asyncio

import pytest

from liveview.actors.mailbox import Mailbox


class TestMailbox:
    def test_pedicates(self):
        q = Mailbox(maxsize=1)
        assert q.empty()

        q.put_nowait(1)
        assert q.full()

    @pytest.mark.asyncio
    async def test_put_nowait_get(self):
        q = Mailbox()

        async def getter():
            return await q.get()

        async def putter():
            q.put_nowait(4)
            return "DONE"

        a, b = await asyncio.gather(getter(), putter(), return_exceptions=True)
        assert a == 4
        assert b == "DONE"

    @pytest.mark.asyncio
    async def test_put_get(self):
        q = Mailbox()

        async def getter():
            return await q.get()

        async def putter():
            await q.put(4)
            return "DONE"

        a, b = await asyncio.gather(getter(), putter(), return_exceptions=True)
        assert a == 4
        assert b == "DONE"

    def test_get_nowait(self):
        q = Mailbox()
        assert q.get_nowait("DEFAULT") == "DEFAULT"

        with pytest.raises(asyncio.QueueEmpty):
            assert q.get_nowait()

        q.put_nowait(42)
        assert q.get_nowait() == 42

    def test_select_nowait(self):
        q = Mailbox()
        for value in ("abc-123", "feg", "abc-456", "hjk-000"):
            q.put_nowait(value)

        assert q.select_nowait("feg*") == "feg"
        assert q.qsize() == 3
        assert q.get_nowait() == "abc-123"
        assert q.qsize() == 2
        assert q.select_nowait("abc*") == "abc=456"
        assert q.qsize() == 1
        assert q.get_nowait() == "hjk-000"
        assert q.qsize() == 0
