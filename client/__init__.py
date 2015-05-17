"""Graypools CacheClient package.

This package provides the CacheClient which wraps the standard AsyncHTTPClient
with the Datasource Cache system.

Copyright (c) 2015 graypools LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import sys
import os
import logging
import zipfile
import gzip
import datetime
from tornado import gen
from tornado.httpclient import HTTPRequest
from tornado.httpclient import HTTPError
from tornado.simple_httpclient import AsyncHTTPClient
from tornado.httputil import format_timestamp
from io import BytesIO
from .cache import Cache, Response

# Logging stuff.
LOG_DIR = 'log'
LOG_FILE = os.path.join(LOG_DIR, 'client.log')
LOG_MSG_FORMAT = '%(asctime)s [{0}] %(levelname)s - %(message)s'
LOG_DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_LOG_LEVEL = logging.INFO

MAX_CLIENTS = 10
CONNECT_TIMEOUT = 5000
REQUEST_TIMEOUT = 400
USER_AGENT = 'Mozilla/5.0 (compatible; CacheClient 0.1; [contact]@[site])'

# HTTP Stuff
LOCATION_HEADER = 'Location'
SOFT_REDIRECT = 302
FILE_UNCHANGED = 304
IF_MODIFIED_SINCE = 'If-Modified-Since'
REFRESH_COOLDOWN = 300  # in Seconds

# Filename stuff
CSV_EXT = '.csv'
XLS_EXT = '.xls'
XLSX_EXT = '.xlsx'
ZIP_EXT = '.zip'
GZ_EXT = '.gz'


DEFAULTS = dict(connect_timeout=CONNECT_TIMEOUT, user_agent=USER_AGENT,
                request_timeout=REQUEST_TIMEOUT)


def init_logger(name, log_file, additional_logs=None,
                log_level=DEFAULT_LOG_LEVEL):
    """Initialize a logger using some defaults.

    Args:
        obj (object): the object from which we'll get a log name.
        log_file (string): file name to which the logger will save the log
            entries.
        additional_logs (list): strings of additional loggers that should be
            captured in this log as well (e.g. 'tornado.access').

    Returns: a Logger object.

    """
    logs = additional_logs or []
    logs.append('tornado.application')
    msgformat = LOG_MSG_FORMAT.format(name)
    logs.append(name)
    fmt = logging.Formatter(msgformat, LOG_DATE_TIME_FORMAT)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(fmt)
    for log_name in logs:
        log = logging.getLogger(log_name)
        log.propagate = False
        log.setLevel(log_level)
        log.addHandler(file_handler)
    return logging.getLogger(name)


def decompress_response(response, target=str()):
    """Decompress file, and return `target` or the only file from it.

    This method now first tests if the file is a ZipFile, and then falls back
    to gzip.

    Args:
        response (BytesIO): raw response from CacheClient.
        target (string, optional): the optional name of a file from within the
            archive to be fetched. The `target` value will be compared to the
            filenames in the archive and hopefully returned.

    Returns:
        target (BytesIO): a file-like object of the target.
    """
    if not zipfile.is_zipfile(response):
        response.seek(0)
        gzfile = gzip.GzipFile(fileobj=response)
        return gzfile
    zfile = zipfile.ZipFile(response, 'r')
    target = target.lower()
    namelist = zfile.namelist()
    for name in namelist:
        lower = name.lower().replace(' ', '_')
        if not ((target in lower and CSV_EXT in lower) or len(namelist) == 1):
            continue
        buf = BytesIO()
        with zfile.open(name) as zip_file:
            for line in zip_file:
                buf.write(line)
        buf.seek(0)
        return buf
    else:
        response.seek(0)
        return response


class CacheClient(object):

    def __init__(self, ioloop):
        self._log = init_logger(type(self).__name__, LOG_FILE)
        self.cache = Cache()
        self._client = AsyncHTTPClient()
        self._client.initialize(ioloop, defaults=DEFAULTS,
                                max_clients=MAX_CLIENTS)
        self.ioloop = ioloop

    @gen.coroutine
    def fetch(self, target, refresh=False, cache=True, delay=None,
              follow=True, extract=None, **kwargs):
        """Fetch a URL from the wild, but first check the Cache.

        Args:
            target (str or HTTPRequest): to be fetched.
            refresh (bool, optional): should the CacheClient ask the remote
                source to refresh cached files?  Defaults to False.
            cache (bool, optional): should results be cached? Defaults to True.
            delay (int, optional): a period, in seconds, for which the client
                should delay before sending the next request after a successful
                fetch.
            follow (bool, optional): should redirects be followed? If False,
                the Response object will only contain a string to the redirect
                url target. Defaults to True.
            extract (str, optional): if supplied, the Client will try to
                extract a filename of `extract` from any resulting compressed
                file. 
            **kwargs (misc., optional): any additional keyword arguments that
                should be passed when a new HTTPRequest is initialized. 

        Returns (/ Raises):
            response (cache.Response or None): a named tuple containing values:
                - `url` (string): the url of the fetch/cache load.
                - `buffer` (BytesIO): the body of the fetch result.
                - `fresh` (bool): True if the Response object is the result of
                    a fresh response from the target server.
                or None if an error occurred (which is logged).
        """
        request = self._cached_http_request(target, follow_redirects=follow,
                                            **kwargs)
        self._log.debug("Fetching file @ {}".format(request.url))
        if not refresh and IF_MODIFIED_SINCE in request.headers:
            self._log.debug("Have cached file, not asking for a refresh.")
            response = self.cache.load(request.url)
            raise gen.Return(response)
        elif IF_MODIFIED_SINCE in request.headers:
            last_mod = request.headers[IF_MODIFIED_SINCE]
            age = datetime.datetime.now() - last_mod
            if age.seconds < REFRESH_COOLDOWN:
                self._log.debug("Have recent cached file, not refreshing.")
                raise gen.Return(self.cache.load(request.url))
            else:
                request.headers[IF_MODIFIED_SINCE] = format_timestamp(last_mod)
        try:
            response = yield self._client.fetch(request)
        except HTTPError as err:
            if err.code == FILE_UNCHANGED:
                self._log.debug("File unchanged, using cached version.")
                raise gen.Return(self.cache.load(request.url))

            # If we get a 302, and we're expecting it, return the location and
            # fresh to indicate that the destination is a new one (since we
            # had to reach out to the server.
            elif err.code == SOFT_REDIRECT and not follow:
                loc = err.response.headers[LOCATION_HEADER]
                self._log.debug('Redirected to {}, not following'.format(loc))
                response = Response(BytesIO(loc), request.url, True)
                raise gen.Return(response)
            else:
                self._log.error(
                    "{0} ({1}) fetching {2}".format(err, err.code, request.url))
                raise gen.Return(None)

        except Exception as excp:
            self._log.exception(excp)
            raise gen.Return(None)
        else:
            self._log.debug("Got fresh file @ {0}".format(request.url))
            if extract:
                response.buffer = decompress_response(response.buffer, extract)

            if cache:
                self._log.debug("Caching {0}".format(request.url))
                self.cache_response(response, overwrite=True)
            response = Response(response.buffer, request.url, True)
            raise gen.Return(response)
        finally:
            if delay:
                self._log.debug("Pausing @ {0} for {1} sec(s)".format(
                    self.ioloop.time(), delay))
                yield gen.sleep(delay)

    def cache_response(self, response, overwrite=False):
        self.cache.add(response.request.url, response.buffer, overwrite, True)

    def _cached_http_request(self, target, **kwargs):
        """Create an HTTPRequest object with a timestamp.

        Args:
            target (string or HTTPRequest): a url or HTTPRequest instance.

        Returns:
            request (HTTPRequest): fully-populated with an If-Modified-Since
                header based on the timestamp of the cached url.
        """
        if not isinstance(target, HTTPRequest):
            target = HTTPRequest(target, **kwargs)
        last_mod = self.cache.last_modified(target.url)
        if last_mod:
            target.headers[IF_MODIFIED_SINCE] = last_mod
        return target
