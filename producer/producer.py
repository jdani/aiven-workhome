#!/usr/bin/env python3

import os
import requests
import sys
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
    }

    parser = EnvConfigParser()
    config = parser.get_parser('producer.cfg', config_default)

    setup_logger(config)
    logger.info('Using config file: {}'.format('producer.cfg'))
    logger.debug('This is a debug message')






if __name__ == "__main__":
    main()