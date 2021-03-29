#!/usr/bin/env python3

import os
import sys
import time
import threading
from checksdb import ChecksDB
from kafka import KafkaConsumer
from common.envconfigparser import EnvConfigParser
from loguru import logger


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
    # Defining vars from config to be used
    uri_file = config.get('postgresql', 'uri_file')
    logger.debug("DB URI FIle: {}".format(uri_file))

    table = config.get('postgresql', 'table_name')
    logger.debug("Table name: {}".format(table))

    loop_delay = config.getint('aiven', 'delay')
    logger.debug("Delay between loop iterations: {}".format(loop_delay))

    max_inserts = config.getint('postgresql', 'max_rows_per_insert')
    logger.debug("Max row to be inserted in the same query: {}".format(max_inserts))


    # Creating db object
    logger.info("Creating DB object. Connecting to db.")
    db = ChecksDB(uri_file, table)


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


    logger.info("Main loop started")
    while True:
        logger.debug("Main loop iteration starts")
        try:
            logger.info("Checking for new messages...")
            threads = []
            messages = []
            for message in kafka_consumer:
                # Append message to be processed
                messages.append(message.value.decode('utf-8'))
                # If messages reaches max_inserts
                if len(messages) == max_inserts:
                    logger.info("Inserting {} messages".format(max_inserts))
                    # Inser messages to the db in a new thread
                    threads.append(
                        threading.Thread(
                            target=db.insert_json_messages,
                            args=(messages, )
                        ))
                    # Start latest thread
                    threads[-1].start()
                    messages = []

            # Process pending messages if these have not reaches max_inserts
            if messages:
                logger.info("Inserting {} messages".format(len(messages)))
                threads.append(
                    threading.Thread(
                        target=db.insert_json_messages,
                        args=(messages, )
                    ))
                # Start latest thread
                threads[-1].start()

            # Commit kafka consumer to keep track of processed messages
            kafka_consumer.commit()
            logger.debug("Consummer commited")

            # Loop delay
            time.sleep(loop_delay)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt captured...")

            logger.info("Commiting kafka consumer and closing it.")
            kafka_consumer.commit()
            kafka_consumer.close()

            logger.info("Commiting pending actions to the db and closing the connection.")
            db.close()


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

    # Reading config file
    parser = EnvConfigParser()
    config = parser.get_parser('consumer.cfg', config_default)

    # Setting up logger
    log_sink = config.get('aiven', 'log_path')
    log_level = config.get('aiven', 'log_level').upper()
    setup_logger(log_sink, log_level)

    logger.info('Config loaded from: {}'.format('consumer.cfg'))

    # Run main
    main()
