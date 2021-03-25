#!/usr/bin/env python3

import os
import sys
import socket
import time
import json
import psycopg2
from kafka import KafkaConsumer
from envconfigparser import EnvConfigParser 
from loguru import logger


def read_uri_file(urifile):
    with open('/usr/share/postgresql/uri.txt', 'r') as cfg_file:
        uri = cfg_file.read().strip()
    return uri


def exist_table(table):
    cursor = db_conn.cursor()
    # https://stackoverflow.com/questions/10598002/how-do-i-get-tables-in-postgres-using-psycopg2
    cursor.execute("""
        select exists(select relname from pg_class where relname='{}')
    """.format(table)
    )
    exists = cursor.fetchone()[0]
    cursor.close()
    return exists



def create_table(table):
    # https://www.postgresqltutorial.com/postgresql-python/create-tables/
    create_table_sql = """
    CREATE TABLE {} (
    """.format(table)

    # id autoincremental as primary key
    create_table_sql += """
        id SERIAL PRIMARY KEY,
    """

    # host as string
    create_table_sql += """
        host VARCHAR(256),
    """

    # dns_start as float. When the http requests started
    create_table_sql += """
        dns_start FLOAT,
    """

    # dns_elapsed as int. Time elapsed in the dns query.
    create_table_sql += """
        dns_elapsed INT,
    """

    # ip as string. ip resolved for host as string.
    create_table_sql += """
        ip VARCHAR(15),
    """    

    # https_start as float. When the http requests started
    create_table_sql += """
        http_start FLOAT,
    """

    # http_elapsed as int. Time elapsed in the http query.
    create_table_sql += """
        http_elapsed INT,
    """

    # ip as string. ip resolved for host as string.
    create_table_sql += """
        http_schema VARCHAR(10),
    """

    # http_url_root as char. url root, without path.
    create_table_sql += """
        http_url_root VARCHAR(256),
    """

    # http_path as char. url path part.
    create_table_sql += """
        http_path VARCHAR(256),
    """

    # url as char. complete utl.
    create_table_sql += """
        http_url VARCHAR(512),
    """

    # http_regex as string. regex to apply to the respose of the query.
    create_table_sql += """
        http_regex VARCHAR(512),
    """

    # http_status_code as int. request status code.
    create_table_sql += """
        http_status_code INT,
    """

    # http_status_code_reason as string. status code reason.
    create_table_sql += """
        http_status_code_reason VARCHAR(512),
    """

   # http_retgex_found as boolean. status code reason.
    create_table_sql += """
        http_retgex_found BOOLEAN
    """

    create_table_sql += ")"

    create_table_sql = create_table_sql.replace("\n", " ")

    logger.debug("Create table SQL: {}".format(create_table_sql))

    cursor = db_conn.cursor()
    cursor.execute(create_table_sql)
    cursor.close()



def get_consumer():
    # https://github.com/aiven/aiven-examples/blob/master/kafka/python/consumer_example.py
    consumer = KafkaConsumer(
        bootstrap_servers=config.get('kafka', 'uri'),
        auto_offset_reset='earliest',
        group_id='workhome',
        security_protocol="SSL",
        ssl_cafile=config.get('kafka', 'ssl_cafile'),
        ssl_certfile=config.get('kafka', 'ssl_certfile'),
        ssl_keyfile=config.get('kafka', 'ssl_keyfile'),
        consumer_timeout_ms=1000,
    )

    consumer.subscribe([config.get('kafka', 'topic')])

    return consumer


def insert_into_db(json_msg):
    cursor = db_conn.cursor()
    msg = json.loads(json_msg)
    insert_query = build_insert_query(msg)
    cursor.execute(insert_query)
    cursor.close()



def build_insert_query(msg):
    query = """
        INSERT INTO {} (
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
            '{meta[host]}',
            {dns[start]},
            {dns[elapsed]},
            '{dns[ip]}',
            {http[start]},
            {http[elapsed]},
            '{http[schema]}',
            '{http[host]}',
            '{http[path]}',
            '{http[url]}',
            '{http[regex]}',
            {http[status_code]},
            '{http[reason]}',
            {http[regex_found]}
        )
    """.format(
        config.get('postgresql', 'table_name'),
        **msg
    )

    return query



def setup_logger(log_sink, log_level):
    # Remove default logger to prevent log twice every message
    logger.remove()

    # If log path set to stdout or not to an absolute path, set stdout as logs sink
    
    if not os.path.isabs(log_sink) or log_sink == 'stdout':
        log_sink = sys.stdout

    # If not a vald log level, set INFO
    log_valid_levels = "TRACE DEBUG INFO SUCCESS WARNING ERROR CRITICAL"
    
    if log_level not in log_valid_levels.replace(' ', ''):
        log_level = 'INFO'

    # Create new logger with new config
    logger.add(
            log_sink,
            level=log_level
    )


def main():


    log_sink = config.get('aiven', 'log_path')
    log_level = config.get('aiven', 'log_level').upper()
    setup_logger(log_sink, log_level)

    logger.info('Using config file: {}'.format('consumer.cfg'))

    try:
        loop_delay = config.getint('aiven', 'delay')
    except Exception:
        loop_delay = config_default['AIVEN_DELAY']
    
    while True:
        try:
            logger.info("Checking for new messages...")
            for message in kafka_consumer:
                insert_into_db(message.value.decode('utf-8'))
            db_conn.commit()
            logger.info("No more messages.")
            logger.debug("Consummer commited")
            kafka_consumer.commit()

            time.sleep(loop_delay)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt captured...")
            logger.info("Commiting kafka consumer and closing it.")
            kafka_consumer.commit()
            kafka_consumer.close()

            logger.info("Commiting pending actions to the db and closing the connection.")
            db_conn.commit()
            db_conn.close()

            # Exit
            sys.exit(1)



if __name__ == "__main__":
    
    
    # Load config here so config object is available in the whole module
    config_default = {
        'AIVEN_LOG_PATH': 'stdout',
        'AIVEN_LOG_LEVEL': 'INFO',
        'AIVEN_DELAY': 5,
        'POSTGRESQL_TABLE_NAME': 'checks'
    }

    parser = EnvConfigParser()
    config = parser.get_parser('consumer.cfg', config_default)

    table_name = config.get('postgresql','table_name')

    kafka_consumer = get_consumer()

    postgresql_uri = read_uri_file(config.get('postgresql', 'uri_file'))
    db_conn = psycopg2.connect(postgresql_uri)

    if not exist_table(table_name):
        logger.info("Table '{}' not found, trying to create it.".format(table_name))
        create_table(table_name)
        logger.info("Table '{}' created.".format(table_name))
    else:
        logger.debug("Table '{}' exists.".format(table_name))

    # Run main
    main()
