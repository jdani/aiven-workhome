import urllib3
import time
import socket
import requests
import re
import json
from loguru import logger


class HTTPChecker:
    def __init__(self, str_url, timeout, regex=None):
        logger.debug("New HTTPChecker instance for URL: {}".format(str_url))
        self.url = urllib3.util.parse_url(str_url)
        self.http_timeout = timeout
        self.http_regex = regex


    def __resolve_host(self):
        logger.info("Resolving host: {}".format(self.url.host))
        self.dns_start = time.time()
        self.ip = socket.gethostbyname(self.url.host)
        logger.debug("Resolved to {}".format(self.ip))
        dns_stop = time.time()
        self.dns_elapsed = int((dns_stop - self.dns_start) * 1000000)


    def __http_check(self):
        # The idea is to access an http service by IP and avoid any DNS
        # request. This does not work in most cases, where multiple domains
        # are served behind the same IP using virtual hosts. So it is needed
        # to define the host in the header.
        # https://stackoverflow.com/questions/27234905/programmatically-access-virtual-host-site-from-ip-python-iis

        # Complete URL using IP instead of host
        url_with_ip = str(str(self.url)).replace(
            "{}://{}".format(
                self.url.scheme,
                self.url.host
            ),
            "{}://{}".format(
                self.url.scheme,
                self.ip
            )
        )

        header = {'Host': self.url.host}

        # Disable non-checking certificate warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        r = None
        s = requests.Session()
        s.max_redirects = 0

        try:
            logger.info('Running HTTP Check')
            self.http_start = time.time()
            r = s.get(
                url_with_ip,
                headers=header,
                verify=False,
                allow_redirects=False,
                timeout=self.http_timeout
            )
        except requests.exceptions.TooManyRedirects:
            logger.error('Too many redirects. Please, be sure the host is not redirecting.')

        if r:
            logger.info('Processing HTTP response')
            self.http_status_code = r.status_code
            self.http_elapsed = r.elapsed.microseconds
            self.http_reason = r.reason

            self.http_regex_found = False
            if self.http_status_code == 200 and self.http_regex:
                self.regex_found = bool(re.findall(self.http_regex, r.text))

            if self.http_status_code >= 400 and self.http_status_code <= 599:
                logger.error('Host could not be retrieved [{}]'.format(r.status_code))

            r.close()

        else:
            # If there is no request information
            logger.error('Site {} is not accesible.'.format(self.url))
            self.http_status_code = 0
            self.http_elapsed = None
            self.http_reason = "Request failed. Check logs."
            self.http_regex_found = None


    def __generate_msg(self):
        logger.debug('Generating return message')
        msg = {
            'meta': {},
            'dns': {},
            'http': {}
        }

        msg['meta']['host'] = self.url.host

        msg['http']['start'] = self.http_start
        msg['http']['schema'] = self.url.scheme
        msg['http']['host'] = self.url.host
        msg['http']['path'] = self.url.path
        msg['http']['url'] = str(self.url)
        msg['http']['regex'] = self.http_regex

        msg['dns']['start'] = self.dns_start
        msg['dns']['elapsed'] = self.dns_elapsed
        msg['dns']['ip'] = self.ip

        msg['http']['status_code'] = self.http_status_code
        msg['http']['elapsed'] = self.http_elapsed
        msg['http']['reason'] = self.http_reason
        msg['http']['regex_found'] = self.http_regex_found

        return json.dumps(msg)


    def run_check(self):
        logger.info('Running check')
        self.__resolve_host()
        self.__http_check()
        return self.__generate_msg()
