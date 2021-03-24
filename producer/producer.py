#!/usr/bin/env python3

import os
import requests
import urllib3
import sys
import re
import socket
import time
import json
import threading
from kafka import KafkaProducer
from envconfigparser import EnvConfigParser 
from loguru import logger


def get_producer():
    # https://github.com/aiven/aiven-examples/blob/master/kafka/python/producer_example.py
    producer = KafkaProducer(
        bootstrap_servers=config.get('kafka', 'uri'),
        security_protocol="SSL",
        ssl_cafile=config.get('kafka', 'ssl_cafile'),
        ssl_certfile=config.get('kafka', 'ssl_certfile'),
        ssl_keyfile=config.get('kafka', 'ssl_keyfile'),
    )
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



def run_check():

    # GenerDefining vars to generate a more readable code
    host = config.get('site', 'host')
    logger.debug('Host: {}'.format(host))
    
    http_schema = config.get('site', 'http_schema')
    logger.debug('HTTP schema: {}'.format(http_schema))
    
    http_path = config.get('site', 'path')
    logger.debug('HTTP path: {}'.format(http_path))
    
    http_hostname = "{}://{}".format(
        http_schema,
        host
    )
    logger.debug('HTTP host: {}'.format(http_hostname))
    
    if http_path != '/':
        url = '{}/{}'.format(
            http_hostname,
            http_path
        )
    else:
        url = http_hostname
    logger.debug('URL: {}'.format(url))

    http_regex = config.get('site', 'regex')
    logger.debug('HTTP regex: {}'.format(http_regex))


    # https://stackoverflow.com/questions/38174877/python-measuring-dns-and-roundtrip-time
    dns_start = time.time()
    site_ip = socket.gethostbyname(host)
    dns_stop = time.time()
    dns_elapsed = int(( dns_stop - dns_start ) * 1000000)

    http_host_ip = "{}://{}/{}".format(
        http_schema,
        site_ip,
        http_path
    )
    logger.debug('HTTP Host IP: {}'.format(http_host_ip))
    


    logger.info("Site to monitor: {}".format(url))
    logger.debug("Regex to look for in site: {}".format(http_regex))



    header = {'Host': host}

    logger.info("Accesing {}".format(url))

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    # https://stackoverflow.com/questions/27234905/programmatically-access-virtual-host-site-from-ip-python-iis
    r = requests.get(http_host_ip, headers=header, verify=False)
    regex_found = False
    if r.status_code == 200:
        if re.findall(http_regex, r.text):
            regex_found = True
            logger.info("Regex found!")
    

    msg = {
            'meta': {},
            'dns': {},
            'http': {}
    }

    msg['meta']['start'] = dns_start
    msg['meta']['host'] = host

    msg['http']['schema'] = http_schema
    msg['http']['host'] = http_hostname
    msg['http']['path'] = http_path
    msg['http']['url'] = url
    msg['http']['status_code'] = r.status_code
    msg['http']['elapsed'] = r.elapsed.microseconds
    msg['http']['regex'] = http_regex
    msg['http']['regex_found'] = regex_found

    msg['dns']['elapsed'] = dns_elapsed
    msg['dns']['ip'] = site_ip

    r.close()

    if msg['http']['status_code'] >= 400 and msg['http']['status_code'] <= 599:
        logger.error("Host could not be retrieved")

    produce_message(json.dumps(msg))



def produce_message(message):
    kafka_producer.send(config.get('kafka', 'topic'), message.encode("utf-8"))



def main():


    log_sink = config.get('aiven', 'log_path')
    log_level = config.get('aiven', 'log_level').upper()
    setup_logger(log_sink, log_level)

    logger.info('Using config file: {}'.format('producer.cfg'))

    try:
        loop_delay = config.getint('aiven', 'delay')
    except Exception:
        loop_delay = config_default['AIVEN_DELAY']
    
    while True:
        try:
            
            # Running the check in a separated thread makes the check run more regularly and close
            # to the delay defined in config['aiven']['delay']. This way the time that takes the check itself
            # is not accumulated to the config delay.
            x = threading.Thread(target=run_check)
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
        'SITE_HTTP_SCHEMA': 'https',
        'SITE_HOST': 'example.net',
        'SITE_PATH': '/',
    }

    parser = EnvConfigParser()
    config = parser.get_parser('producer.cfg', config_default)

    kafka_producer = get_producer()

    # Run main
    main()