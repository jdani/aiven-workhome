import pytest
import json
import string
import random
from consumer.checksdb import ChecksDB
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
def messages():
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

    yield [json.dumps(msg)]


def test_ChecksDB(config, messages):
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))

    test_table = 'testtable' + '_' + random_string
    urifile = config.get('postgresql', 'uri_file')
    db = ChecksDB(urifile, test_table)
    assert db.conn.closed == False

    db.insert_json_messages(messages)
    assert db.count_rows(test_table) == 1
    db.drop_table(test_table)
    db.close()
    assert db.conn.closed == True
