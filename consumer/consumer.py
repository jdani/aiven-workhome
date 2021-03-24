#!/usr/bin/env python3

import os
import sys
import socket
import time
import json
from kafka import KafkaConsumer
from envconfigparser import EnvConfigParser 
from loguru import logger


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
    print(json.dumps(json_msg))



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
                print(message.value.decode('utf-8'))
            logger.info("No more messages.")
            logger.debug("Consummer commited")
            kafka_consumer.commit()

            time.sleep(loop_delay)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt captured...")
            logger.info("Commiting kafka consumer, closing it and exiting.")
            kafka_consumer.commit()
            kafka_consumer.close()
            sys.exit(1)



if __name__ == "__main__":
    
    # Load config here so config object is available in the whole module
    config_default = {
        'AIVEN_LOG_PATH': 'stdout',
        'AIVEN_LOG_LEVEL': 'INFO',
        'AIVEN_DELAY': 5,
        'POSTGRESQL_DB': 'http-monitor'
    }

    parser = EnvConfigParser()
    config = parser.get_parser('consumer.cfg', config_default)

    kafka_consumer = get_consumer()

    # Run main
    main()