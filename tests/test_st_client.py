from scitran_client import compute_file_hash
import os


def test_compute_file_hash():
    assert compute_file_hash(
        os.path.join(os.path.dirname(__file__), 'fixtures/test.csv')
    ) == 'v0-sha384-301d915f78736ff43dd396b5607cade4dffc0cd31c94bb2b80aff005cac042d8826a0a766c5dc2884a942cf960177378'
