import pytest
from consumer import consumer
from common.envconfigparser import EnvConfigParser

@pytest.fixture()
def config():
    config_default = {
        'AIVEN_LOG_PATH': 'stdout',
        'AIVEN_LOG_LEVEL': 'INFO',
        'AIVEN_DELAY': 5,
        'POSTGRESQL_TABLE_NAME': 'checks'
    }
    parser = EnvConfigParser()
    config = parser.get_parser('../../src/consumer/consumer.cfg', config_default)
    yield config


@pytest.fixture()
def msg():
    msg = {
                'meta': {},
                'dns': {},
                'http': {}
        }

    msg['meta']['host'] = 'testhost'

    msg['http']['start'] = 1
    msg['http']['schema'] = 'https'
    msg['http']['host'] = 'test'
    msg['http']['path'] = '/'
    msg['http']['url'] = 'test-url'
    msg['http']['regex'] = r'\dfdf'
    msg['http']['regex_found'] = True
    msg['http']['elapsed'] = 1
    msg['http']['status_code'] = 1
    msg['http']['reason'] = 'test code'


    msg['dns']['start'] = 1
    msg['dns']['elapsed'] = 2
    msg['dns']['ip'] = '1.2.3.4'
    yield msg

@pytest.fixture()
def test_insert_query():
    q = r"""
    INSERT INTO testtable (
            host,
            dns_start,
            dns_elapsed,
            ip,
            http_start,
            http_elapsed,
            http_schema,
            http_url_root,
            http_path,
            http_url,
            http_regex,
            http_status_code,
            http_status_code_reason,
            http_retgex_found
        ) VALUES (
            'testhost',
            1,
            2,
            '1.2.3.4',
            1,
            1,
            'https',
            'test',
            '/',
            'test-url',
            '\dfdf',
            1,
            'test code',
            True
        )
        """
    yield q.strip()


def test_read_uri_file():
    uri_string = consumer.read_uri_file('urifile.test')
    uri_test = "this is just a testing text"
    assert uri_test == uri_string


def test_build_insert_query(msg, test_insert_query):
    query = consumer.build_insert_query('testtable', msg)
    assert query == test_insert_query
