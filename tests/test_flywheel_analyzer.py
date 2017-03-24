import scitran_client.flywheel_analyzer as fa


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
