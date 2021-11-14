import pytest

from liveview.actors.messaging import (
    CALL,
    CAST,
    ERROR,
    OK,
    Call,
    Cast,
    Error,
    Ok,
    Response
)

pytestmark = pytest.mark.only


class TestResponse:
    def test_success(self):
        def square(x):
            return x ** 2

        assert Response(OK, 9).success(square).value == 81
        assert Ok(9).success(square).value == 81
        error = SystemError("bla", 2)
        assert Response(ERROR, error).success(square).value is error
        assert Error(error).success(square).value is error

    def test_failure(self):
        def square(ex):
            x = ex.args[1]
            return x ** 2

        error = SystemError("bla", 2)
        assert Response(OK, 9).failure(square).value == 9
        assert Ok(9).failure(square).value == 9
        assert Response(ERROR, error).failure(square).value == 4
        assert Error(error).failure(square).value == 4


class TestMessage:
    def test_topic_types(self):
        assert (
            Call("actor_ref", "topic", {"some": "payload"}, "sender", None).topic[0]
            == CALL
        )
        assert (
            Cast("actor_ref", "topic", {"some": "payload"}, "sender").topic[0] == CAST
        )
