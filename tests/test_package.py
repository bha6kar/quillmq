import quillmq


def test_version_is_exposed():
    assert isinstance(quillmq.__version__, str)
    assert quillmq.__version__.count(".") == 2


def test_connect_is_exported():
    assert callable(quillmq.connect)
