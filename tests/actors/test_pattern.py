import pytest

from liveview.actors.pattern import glob, pattern, re


class TestPattern:
    @pytest.fixture
    def examples(self):
        return {"ab": 1, "abc": 2, "42": 3, "456": 4}.items()

    def test_find_str(self, examples):
        p = pattern("42")
        assert type(p).__name__ == "Pattern"
        results = [(k, v) for k, v in examples if p(k)]
        assert len(results) == 1
        assert results[0] == ("42", 3)

    def test_find_fnmatch(self, examples):
        p = glob("4*")
        results = [v for k, v in examples if p(k)]
        assert len(results) == 2
        assert results == [3, 4]

    def test_find_re(self, examples):
        p = re("ab.*")
        results = [v for k, v in examples if p(k)]
        assert len(results) == 2
        assert results == [1, 2]

    def test_first(self, examples):
        p = glob("4*")
        assert next(v for k, v in examples if p(k)) == 3

    def test_no_recursive(self):
        p = glob("4*")
        p2 = pattern(p)
        assert id(p) == id(p2)
        assert p == p2
        assert "Pattern(re.compile(" in str(p2)


class TestTuplePattern:
    @pytest.fixture
    def examples(self):
        return [(1, 2), (1, "abcdef", {"dict": 90}, 42), ("asdf", "zxcv")]

    def test_ellipsis__len(self, examples):
        # Ellipsis should match anything,
        # however the tuples need to have the same length
        p = pattern((..., Ellipsis))
        result = [e for e in examples if p(e)]
        assert len(result) == 2
        assert result == [(1, 2), ("asdf", "zxcv")]

    def test_fnmatch__regex(self, examples):
        p = pattern(glob("a*f"), re("z.+"))
        result = [e for e in examples if p(e)]
        assert len(result) == 1
        assert result == [("asdf", "zxcv")]

    def test_arbitrary(self, examples):
        p = pattern(1, glob("a*f"), ..., 42)
        result = [e for e in examples if p(e)]
        assert len(result) == 1
        assert result == [(1, "abcdef", {"dict": 90}, 42)]

    def test_no_recursive(self):
        p = pattern(re("a*.f"), re("z.+"))
        p2 = pattern(p)
        assert id(p) == id(p2)
        assert p == p2
        representation = str(p2).replace("'", '"')
        assert (
            representation
            == 'Pattern((Pattern(re.compile("a*.f")), Pattern(re.compile("z.+"))))'
        )
