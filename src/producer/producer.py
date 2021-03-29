#!/usr/bin/env python3
import os
import sys
import threading
import time
from httpchecker import HTTPChecker
from kafka import KafkaProducer
from common.envconfigparser import EnvConfigParser
from loguru import logger


def get_producer(uri, cafile, certfile, keyfile):
    # Generate and return kafka producer
    # https://github.com/aiven/aiven-examples/blob/master/kafka/python/producer_example.py
    producer = KafkaProducer(
        bootstrap_servers=uri,
        security_protocol="SSL",
        ssl_cafile=cafile,
        ssl_certfile=certfile,
        ssl_keyfile=keyfile,
    )
    logger.info("Connected to kafka")
    return producer


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


def check_and_produce_wrapper(producer, site):
    # Wrapper function to be called by the new thread
    # Run check
    logger.info("Running check")
    msg = site.run_check()

    # Send msg to kafka producer as a json string
    logger.info("Sending msg to kafka")
    produce_message(producer, msg)


def produce_message(producer, message):
    # Send msg to kafka
    topic = config.get('kafka', 'topic')
    logger.debug("sending msg to kafka topic '{}'".format(topic))
    producer.send(topic, message.encode("utf-8"))


def main():
    # Setup logger
    log_sink = config.get('aiven', 'log_path')
    log_level = config.get('aiven', 'log_level').upper()
    setup_logger(log_sink, log_level)

    logger.info('Using config file: {}'.format('producer.cfg'))

    # Setup kafka
    kafka_uri = config.get('kafka', 'uri'),
    kafk_cafile = config.get('kafka', 'ssl_cafile')
    kafka_certfile = config.get('kafka', 'ssl_certfile')
    kafka_keyfile = config.get('kafka', 'ssl_keyfile')
    kafka_producer = get_producer(
        kafka_uri,
        kafk_cafile,
        kafka_certfile,
        kafka_keyfile
    )
    logger.info("Connecting to kafka")

    # Needed config vars
    loop_delay = config.getint('aiven', 'delay')
    logger.debug('Loop delay: {}'.format(loop_delay))
    http_url = config.get('site', 'url')
    logger.debug('HTTP URL: {}'.format(http_url))
    http_regex = config.get('site', 'regex')
    logger.debug('HTTP regex: {}'.format(http_regex))
    http_timeout = float(config.getint('site', 'timeout'))
    logger.debug('HTTP Timeout: {}'.format(http_timeout))

    # Create site object
    logger.info("Created HTTPChecker object")
    site = HTTPChecker(
        http_url,
        http_timeout,
        regex=http_regex
    )

    # Main loop
    logger.info("Main loop starts")
    while True:
        logger.debug("Main loop iteration starts")
        try:
            # Running the check in a separated thread makes the check run more regularly and close
            # to the delay defined in config['aiven']['delay']. This way the time that takes the check itself
            # is not accumulated to the config delay.
            logger.info("Running check in separated thread")
            x = threading.Thread(target=check_and_produce_wrapper, args=(
                kafka_producer,
                site
            ))
            x.start()
            time.sleep(loop_delay)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt captured...")
            logger.info("Closing kafka producer and exiting.")
            kafka_producer.flush()
            kafka_producer.close()
            sys.exit(1)


if __name__ == "__main__":
    # Load config here so config object is available in the whole module
    config_default = {
        'AIVEN_LOG_PATH': 'stdout',
        'AIVEN_LOG_LEVEL': 'INFO',
        'AIVEN_DELAY': 5,
        'SITE_URL': 'https://example.net',
        'SITE_TIMEOUT': 1,
    }

    parser = EnvConfigParser()
    config = parser.get_parser('producer.cfg', config_default)

    # Run main
    main()
