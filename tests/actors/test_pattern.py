import pytest

from liveview.actors.pattern import pattern

pytestmark = pytest.mark.only


class TestPattern:
    @pytest.fixture
    def examples(self):
        return {"ab": 1, "abc": 2, "42": 3, "456": 4}.items()

    def test_find_str(self, examples):
        p = pattern("42")
        assert type(p).__name__ == "Pattern"
        for matcher in (p, p.matcher):
            results = [(k, v) for k, v in examples if matcher(k)]
            assert len(results) == 1
            assert results[0] == ("42", 3)

    def test_find_fnmatch(self, examples):
        p = pattern("4*")
        for matcher in (p, p.matcher):
            results = [v for k, v in examples if matcher(k)]
            assert len(results) == 2
            assert results == [3, 4]

    def test_find_re(self, examples):
        p = pattern("/ab.*/")
        for matcher in (p, p.matcher):
            results = [v for k, v in examples if matcher(k)]
            assert len(results) == 2
            assert results == [1, 2]

    def test_first(self, examples):
        p = pattern("4*")
        assert next(v for k, v in examples if p(k)) == 3

    def test_matcher(self, examples):
        matcher = pattern("*a*").matcher
        assert matcher("a")
        assert matcher("ba")
        assert matcher("aba")


class TestTuplePattern:
    @pytest.fixture
    def examples(self):
        return [(1, 2), (1, "abcdef", {"dict": 90}, 42), ("asdf", "zxcv")]

    def test_ellipsis__len(self, examples):
        # Ellipsis should match anything,
        # however the tuples need to have the same length
        p = pattern((..., Ellipsis))
        assert type(p).__name__ == "TuplePattern"
        for matcher in (p, p.matcher):
            result = [e for e in examples if matcher(e)]
            assert len(result) == 2
            assert result == [(1, 2), ("asdf", "zxcv")]

    def test_fnmatch__regex(self, examples):
        p = pattern("a*f", "/z.+/")
        assert type(p).__name__ == "TuplePattern"
        for matcher in (p, p.matcher):
            result = [e for e in examples if matcher(e)]
            assert len(result) == 1
            assert result == [("asdf", "zxcv")]

    def test_arbitrary(self, examples):
        p = pattern(1, "a*f", ..., 42)
        for matcher in (p, p.matcher):
            result = [e for e in examples if matcher(e)]
            assert len(result) == 1
            assert result == [(1, "abcdef", {"dict": 90}, 42)]
