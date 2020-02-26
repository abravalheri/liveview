import pytest

from liveview.actors import Registry


class TestRegistry:
    @pytest.fixture
    def registry(self):
        return Registry({'ab': 1, 'abc': 2, '42': 3, '456': 4})

    def test_find_str(self, registry):
        results = list(registry.find('42'))
        assert len(results) == 1
        assert results[0] == 3

    def test_find_fnmatch(self, registry):
        results = list(registry.find('4*'))
        assert len(results) == 2
        assert results == [3, 4]

    def test_find_re(self, registry):
        results = list(registry.find('/ab.*/'))
        assert len(results) == 2
        assert results == [1, 2]
