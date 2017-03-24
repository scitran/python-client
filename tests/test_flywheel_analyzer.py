import scitran_client.flywheel_analyzer as fa
import pytest


def test_find():
    assert fa.find([
        dict(label='pre'),
        dict(label='post'),
    ], label='post') == dict(label='post')


def test_find_callable():
    assert fa.find([
        dict(label='pre 2016-01-01'),
        dict(label='post 2016-01-02'),
    ], label=lambda val: val.startswith('post ')) == dict(label='post 2016-01-02')


def test_label_matcher_find_op():
    with pytest.raises(AssertionError) as e:
        fa.define_analysis('afq', lambda **kwargs: {}, label_matcher=lambda val: val.startswith('afq '))
    assert 'does not detect this operation' in str(e)

    fa.define_analysis('afq', lambda **kwargs: {}, label_matcher=lambda val: val.startswith('afq'))
