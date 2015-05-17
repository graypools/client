#!/usr/bin/env python

from __future__ import absolute_import, division, print_function, with_statement

from io import BytesIO
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import Application, RequestHandler, url
from client import CacheClient


class HelloWorldHandler(RequestHandler):
    def get(self):
        name = self.get_argument("name", "world")
        self.set_header("Content-Type", "text/plain")
        self.finish("Hello %s!" % name)


class PostHandler(RequestHandler):
    def post(self):
        self.finish("Post arg1: %s, arg2: %s" % (
            self.get_argument("arg1"), self.get_argument("arg2")))


class RedirectHandler(RequestHandler):
    def prepare(self):
        self.redirect(self.get_argument("url"),
                      status=int(self.get_argument("status", "302")))


class CountdownHandler(RequestHandler):
    def get(self, count):
        count = int(count)
        if count > 0:
            self.redirect(self.reverse_url("countdown", count - 1))
        else:
            self.write("Zero")


class CacheClientCommonTestCase(AsyncHTTPTestCase):

    def tearDown(self):
        self.http_server.stop()
        self.http_client.cache.clear()
        self.http_client.cache.close()
        super(AsyncHTTPTestCase, self).tearDown()

    def get_http_client(self):
        return CacheClient(ioloop=self.io_loop)

    def get_app(self):
        return Application([
            url("/hello", HelloWorldHandler),
            url("/post", PostHandler),
            url("/redirect", RedirectHandler),
            url("/countdown/([0-9]+)", CountdownHandler, name="countdown"),
        ], gzip=True)

    def fetch(self, path, **kwargs):
        return self.http_client.fetch(self.get_url(path), **kwargs)

    @gen_test
    def test_hello_world_no_cache(self):
        response = yield self.fetch("/hello", cache=False)
        self.assertEqual(response.buffer.read(), b"Hello world!")
        self.assertEqual(response.fresh, True)

        response = yield self.fetch("/hello?name=Drew", cache=False)
        self.assertEqual(response.buffer.read(), b"Hello Drew!")
        self.assertEqual(response.fresh, True)

    @gen_test
    def test_hello_world_cached(self):
        response = yield self.fetch("/hello")  # will add it to the cache
        self.assertEqual(response.buffer.read(), b"Hello world!")
        self.assertEqual(response.fresh, True)
        response = yield self.fetch("/hello")  # will pull it from the cache
        self.assertEqual(response.buffer.read(), b"Hello world!")
        self.assertEqual(response.fresh, False)
       
    @gen_test
    def test_hello_world_cached_with_params(self):
        response = yield self.fetch("/hello?name=Drew")
        self.assertEqual(response.buffer.read(), b"Hello Drew!")
        self.assertEqual(response.fresh, True)

        response = yield self.fetch("/hello?name=Drew")
        self.assertEqual(response.buffer.read(), b"Hello Drew!")
        self.assertEqual(response.fresh, False)

    @gen_test
    def test_post(self):
        response = yield self.fetch("/post", method="POST",
                              body="arg1=foo&arg2=bar")
        self.assertEqual(response.buffer.read(), b"Post arg1: foo, arg2: bar")

    @gen_test
    def test_follow_redirect(self):
        response = yield self.fetch("/countdown/2", follow=False)
        self.assertTrue(response.buffer.read().endswith("/countdown/1"))

        response = yield self.fetch("/countdown/2")

        # Note that the CacheClient deviates from Tornado's typical performance
        # here because it return the ~requested~ url.
        self.assertTrue(response.url.endswith("/countdown/2"))
        self.assertEqual(response.buffer.read(), b"Zero")
