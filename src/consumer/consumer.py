#!/usr/bin/env python3

import os
import sys
import socket
import time
import json
import psycopg2
import threading
from kafka import KafkaConsumer
from common.envconfigparser import EnvConfigParser
from loguru import logger


def read_uri_file(urifile):
    with open(urifile, 'r') as cfg_file:
        uri = cfg_file.read().strip()
    return uri



def exists_table(conn, table):
    cursor = conn.cursor()
    # https://stackoverflow.com/questions/10598002/how-do-i-get-tables-in-postgres-using-psycopg2
    cursor.execute("""
        select exists(select relname from pg_class where relname='{}')
    """.format(table)
    )
    exists = cursor.fetchone()[0]
    cursor.close()
    return exists



def create_table(conn, table):
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

    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    cursor.close()



def get_consumer(server, cafile, certfile, keyfile, topic):
    # https://github.com/aiven/aiven-examples/blob/master/kafka/python/consumer_example.py
    consumer = KafkaConsumer(
        bootstrap_servers=server,
        auto_offset_reset='earliest',
        group_id='workhome',
        security_protocol="SSL",
        ssl_cafile=cafile,
        ssl_certfile=certfile,
        ssl_keyfile=keyfile,
        consumer_timeout_ms=1000,
    )

    consumer.subscribe([topic])
    return consumer



def insert_into_db(conn, table_name, messages):
    cursor = conn.cursor()
    for msg in messages:
        json_msg = msg.value.decode('utf-8')
        msg = json.loads(json_msg)
        insert_query = build_insert_query(table_name, msg)
        cursor.execute(insert_query)
    conn.commit()
    cursor.close()



def build_insert_query(table_name, msg):
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
        table_name,
        **msg
    )

    return query.strip()



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



def get_db_conn(psql_uri):
    conn = psycopg2.connect(psql_uri)
    return conn



def main():

    # Stablish db connection
    uri_file = config.get('postgresql', 'uri_file')
    logger.info("Reading uri from file {}".format(uri_file))
    uri = read_uri_file(uri_file)
    table_name = config.get('postgresql', 'table_name')

    logger.info("Stablishing DB connection")
    conn = get_db_conn(uri)

    # Create table if does not exist
    table_name = config.get('postgresql','table_name')
    if not exists_table(conn, table_name):
        logger.info("Table '{}' not found, trying to create it.".format(table_name))
        create_table(table_name)
        logger.info("Table '{}' created.".format(table_name))
    else:
        logger.debug("Table '{}' exists.".format(table_name))

    # Get kafka consumer
    logger.info("Getting kafka consumer")
    kafka_server = config.get('kafka', 'uri')
    kafka_cafile = config.get('kafka', 'ssl_cafile')
    kafka_certfile = config.get('kafka', 'ssl_certfile')
    kafka_keyfile = config.get('kafka', 'ssl_keyfile')
    kafka_topic = config.get('kafka', 'topic')
    kafka_consumer = get_consumer(
        kafka_server,
        kafka_cafile,
        kafka_certfile,
        kafka_keyfile,
        kafka_topic
    )

    try:
        loop_delay = config.getint('aiven', 'delay')
    except Exception:
        loop_delay = config_default['AIVEN_DELAY']
    logger.debug("Delay between loop iterations: {}".format(loop_delay))
    
    logger.info("Main loop started")
    while True:
        logger.debug("Main loop iteration starts")
        try:
            logger.info("Checking for new messages...")
            messages = []
            for message in kafka_consumer:
                messages.append(message)

            logger.info("Messages processed in this iteration: {}".format(len(messages)))

            if messages:
                logger.info("Processing messages")
                # Kafka consumer commited to save the last msg readed
                # only if new messages have been received. So not
                # commiting if not needed
                kafka_consumer.commit()
                logger.debug("Consummer commited")

                x = threading.Thread(
                    target=insert_into_db,
                    args=(conn, table_name, messages)
                )
                x.start()

            time.sleep(loop_delay)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt captured...")

            logger.info("Commiting kafka consumer and closing it.")
            kafka_consumer.commit()
            kafka_consumer.close()

            logger.info("Commiting pending actions to the db and closing the connection.")
            conn.commit()
            conn.close()

            # Exit
            sys.exit(1)

        logger.debug("Main loop iteration ends")



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

    log_sink = config.get('aiven', 'log_path')
    log_level = config.get('aiven', 'log_level').upper()
    setup_logger(log_sink, log_level)

    logger.info('Config loaded from: {}'.format('consumer.cfg'))

    # Run main
    main()
