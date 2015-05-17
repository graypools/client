"""Graypools' CacheClient Cache module.

This module provides an sqlite3 back-end, allowing for easy transport and
storage of historical HTTP-fetched results. Sqlite3 has anecdotally proven to
be significantly faster in terms of read/write forcaching when compared with
other filesystem schemes.

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
import sqlite3
import datetime
import threading
import os
from io import BytesIO
from collections import namedtuple


# Caching constants
CACHE_FILE = 'cache.db'
CACHE_TABLE = 'locations'
FIELDS = ','.join(['url TEXT PRIMARY KEY', 'content BLOB',
                   "last_modified TIMESTAMP DEFAULT (datetime('now', 'localtime'))"])

LAST_MOD = "SELECT last_modified FROM {} WHERE url=?".format(CACHE_TABLE)
INSERT = 'INSERT INTO {} (content, url) VALUES (?, ?)'.format(CACHE_TABLE)
UPDATE = 'UPDATE {} SET content=?, last_modified=? WHERE url=?'.format(
    CACHE_TABLE)
SELECT = 'SELECT content FROM {} WHERE url=?'.format(CACHE_TABLE)
CREATE = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(CACHE_TABLE, FIELDS)
CLEAR = 'DELETE FROM LOCATIONS'


def _get_buffer(buf):
    val = buf
    if not isinstance(buf, (basestring)):
            line = buf.tell()
            val = buf.read()
            buf.seek(line)
    return val


Response = namedtuple('Response', ('buffer', 'url', 'fresh'))

def block_and_execute(meth):
    """Wrapper method acquires lock and tries operation until DB is open.

    Args:
        meth (method): to operate on the database.
    """

    def _method(self, *args, **kwargs):
        while True:
            try:
                self._lock.acquire()
                result = meth(self, *args, **kwargs)
            except sqlite3.OperationalError:
                pass
            except Exception:
                raise
            else:
                break
            finally:
                self._lock.release()
        return result
    return _method


class Cache(object):

    """Lightweight sqlite3 interface for CRUD transactions into a URL cache."""

    def __init__(self):
        self.conn = sqlite3.connect(CACHE_FILE,
                                    detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.text_factory = BytesIO
        self.cursor = self.conn.cursor()
        self._lock = threading.Lock()
        self.create()

    @block_and_execute
    def create(self):
        """Create a cache table."""
        self.conn.execute(CREATE, ())

    def close(self):
        """Close the database connection and release the cursor."""
        self.conn.close()

    @block_and_execute
    def last_modified(self, url):
        """Retrive the last modification datetime for a given url.

        Args:
            url (string): the url to retrieve.

        Returns:
            last_mod (datetime.datetime): last updated. Sqlite3 automatically
                converts to datetime.datetime because detect_types is set.
        """
        self.cursor.execute(LAST_MOD, (url,))
        last_mod = self.cursor.fetchone()
        if last_mod and last_mod[0]:
            return last_mod[0]

    @block_and_execute
    def add(self, url, buf, overwrite=False, commit=True):
        """Add a location to the cache.

        Args:
            url (string): the url to save the object to.
            buf (misc., sqlite3-adapted): the buffer to be saved to the cache.
                Note that this argument must have been adapted for sqlite3
                insertion. BytesIO, strings, unicode and core Python types are
                all acceptable.
            overwrite (bool): if True, the cache will overwrite pre-existing
                entries for the same url.
            commit (bool): if True, the Cache will commit the insert after.
                This should be false where a large group of inserts is
                anticipated, to improve performance. In that case, a manual
                commit should be issued after.
        """
        val = _get_buffer(buf)
        try:
            self.cursor.execute(INSERT, (val, url))
            if commit:
                self.conn.commit()
        except sqlite3.IntegrityError:
            if overwrite:
                val = _get_buffer(buf)
                now = datetime.datetime.now()
                self.cursor.execute(UPDATE, (val, now, url))
                if commit:
                    self.conn.commit()

    @block_and_execute
    def load(self, url):
        """Load a cached location from the cache.

        Args:
            url (string): the url of the location to be loaded.

        Returns:
            response (Response): a Response object containing the url (string)
                and buffer (BytesIO).
        """
        self.cursor.execute(SELECT, (url,))
        content = self.cursor.fetchone()
        if content:
            content = content[0]
            if isinstance(content, buffer):
                content = BytesIO(content)
            return Response(content, url, False)

    @block_and_execute
    def clear(self):
        """Empty the cache."""
        self.cursor.execute(CLEAR)
        self.conn.commit()
