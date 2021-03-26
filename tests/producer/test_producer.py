import pytest
import re
from time import struct_time, gmtime
from requests.models import Response
from producer import producer
from common.envconfigparser import EnvConfigParser


def test_resolve_host():
    ip, start, elapsed = producer.resolve_host('example.net')
    
    assert bool(re.match(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', ip)) == True
    assert type(gmtime(start)) is struct_time
    assert type(elapsed) is int
    assert elapsed > 0


def test_http_get():

    host = 'example.net'
    http_schema = 'https'
    timeout = 5
    ip, _, _ = producer.resolve_host(host)
    url_ip = '{}://{}'.format(
        http_schema,
        ip
    )

    start, req = producer.http_get(url_ip, host, timeout)

    assert type(gmtime(start)) is struct_time
    assert type(req) is Response
