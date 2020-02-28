from liveview.utils import uniq


def test_uniq():
    assert len(list(uniq([1, 1, 1, 2, 2, 2]))) == 2
    obj1 = object()
    obj2 = object()
    assert len(list(uniq([obj1, obj1, obj1, obj2, obj2]))) == 2
