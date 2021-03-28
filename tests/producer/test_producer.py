import pytest
import re
import json
from time import struct_time, gmtime
from producer.httpchecker import HTTPChecker




@pytest.fixture()
def url():
    check_url = 'https://example.net' 
    yield check_url


def test_HTTPChecker(url):
    site = HTTPChecker(url, 1)
    msg = site.run_check()
    assert bool(re.match(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', site.ip)) == True
    assert type(gmtime(site.http_start)) is struct_time
    assert type(site.http_elapsed) is int
    assert site.http_elapsed > 0

    json_msg = json.loads(msg)
    assert type(json_msg) is dict


