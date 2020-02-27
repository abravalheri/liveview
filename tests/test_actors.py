import pytest

from liveview.actors import Pattern


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
