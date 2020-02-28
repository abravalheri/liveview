import pytest

from liveview.actors.pattern import Pattern


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
