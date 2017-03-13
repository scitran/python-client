import requests_mock
from scitran_client.st_auth import _is_valid_token

host = 'https://flywheel.io'


@requests_mock.mock()
def test_is_valid_token(mock):
    mock.get('{}/api/users/self'.format(host), status_code=200)
    assert _is_valid_token(host, 'yep')


@requests_mock.mock()
def test_is_valid_token_invalid(mock):
    mock.get('{}/api/users/self'.format(host), status_code=401)
    assert not _is_valid_token(host, 'yep')
