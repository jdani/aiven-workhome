#!/usr/bin/env python3

import os
import requests
import sys
import re
import socket
import time
import json
from envconfigparser import EnvConfigParser 
from loguru import logger


def setup_logger(config):
    # Remove default logger to prevent log twice every message
    logger.remove()

    # If log path set to stdout or not to an absolute path, set stdout as logs sink
    log_sink = config.get('aiven', 'log_path')
    if not os.path.isabs(log_sink) or log_sink == 'stdout':
        log_sink = sys.stdout

    # If not a vald log level, set INFO
    log_valid_levels = "TRACE DEBUG INFO SUCCESS WARNING ERROR CRITICAL"
    log_level = config.get('aiven', 'log_level').upper()
    if log_level not in log_valid_levels.replace(' ', ''):
        log_level = 'INFO'

    # Create new logger with new config
    logger.add(
            log_sink,
            level=log_level
    )

def main():
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

    setup_logger(config)
    logger.info('Using config file: {}'.format('producer.cfg'))

    


    # Generating vars to generate a more readable code
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
    dns_start = time.time() * 1000000
    site_ip = socket.gethostbyname(host)
    dns_stop = time.time() * 1000000
    dns_elapsed = dns_stop - dns_start

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
    msg['http']['elapsed'] = r.elapsed.microseconds * 1000
    msg['http']['regex'] = http_regex
    msg['http']['regex_found'] = regex_found

    msg['dns']['elapsed'] = dns_elapsed
    msg['dns']['ip'] = site_ip

    if r.status_code >= 400 and r.status_code <= 599:
        logger.error("Host could not be retrieved")
    print(json.dumps(msg))







if __name__ == "__main__":
    main()