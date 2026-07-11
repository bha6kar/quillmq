import pytest

from quillmq.exchange import Exchange, topic_match


def test_topic_star_matches_one_word():
    assert topic_match("wp.*.done", "wp.build.done")
    assert not topic_match("wp.*.done", "wp.build.stage.done")


def test_topic_hash_matches_zero_or_more_words():
    assert topic_match("wp.#", "wp")
    assert topic_match("wp.#", "wp.build.done")
    assert not topic_match("wp.#", "job.build")


def test_topic_exact_and_length_mismatch():
    assert topic_match("a.b", "a.b")
    assert not topic_match("a.b", "a")
    assert not topic_match("a", "a.b")


def test_direct_routes_exact_key():
    ex = Exchange("d", "direct")
    ex.bind("q1", "alpha")
    ex.bind("q2", "beta")
    assert ex.route("alpha") == ["q1"]
    assert ex.route("gamma") == []


def test_fanout_routes_all_regardless_of_key():
    ex = Exchange("f", "fanout")
    ex.bind("q1", "")
    ex.bind("q2", "ignored")
    assert ex.route("anything") == ["q1", "q2"]


def test_topic_routes_matching_patterns_deduped():
    ex = Exchange("t", "topic")
    ex.bind("q1", "wp.#")
    ex.bind("q1", "wp.build.*")  # same queue, must not duplicate
    ex.bind("q2", "#.done")
    assert ex.route("wp.build.done") == ["q1", "q2"]


def test_topic_star_does_not_span_multiple_words():
    ex = Exchange("t", "topic")
    ex.bind("q2", "*.done")
    assert ex.route("wp.build.done") == []  # '*' is exactly one word
    assert ex.route("build.done") == ["q2"]


def test_invalid_exchange_type_raises():
    with pytest.raises(ValueError):
        Exchange("bad", "nonsense")
