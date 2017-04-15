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


@pytest.mark.parametrize('files,result', [
    ([dict(name='hi.txt'), dict(name='hi.nii')], dict(id='123', name='hi.txt', type='acquisition')),
    ([], None),
])
def test_find_file(files, result):
    container = fa.FlywheelFileContainer(dict(_id='123', files=files))
    assert container.find_file('*.txt', default=None) == result


def test_find_file_no_match():
    container = fa.FlywheelFileContainer(dict(_id='', files=[]))
    with pytest.raises(Exception) as e:
        container.find_file('*.txt')
    assert 'Could not find' in str(e)


def test_find_file_multiple_matches():
    container = fa.FlywheelFileContainer(dict(_id='', files=[
        dict(name='hi.txt'),
        dict(name='hi2.txt'),
    ]))
    with pytest.raises(AssertionError) as e:
        container.find_file('*.txt')
    assert 'Multiple matches found' in str(e)
