CacheCliene for Tornado
==================

CacheClient is a simple HTTPClient wrapper for the `Tornado 
<http://www.tornadoweb.org>`_ web framework. CacheClient is used by `Graypools 
<https://www.graypools.com>`_ to fetch remote data sources and cache the
results. It uses an sqlite3 backend for quick read/writes and a very basic
thread-locking mechanism for managing multiple asynchronous fetches.

The CacheClient is under ongoing development. Please feel free to submit any
pull requests or issues, and we'll be happy to address them.

Hello, world
------------

Here is the CacheClient interacting with Tornado's "Hello, world" example:
    
.. code-block:: python

        import tornado.ioloop
        import tornado.web
        from tornado import gen
        from client import CacheClient

        class MainHandler(tornado.web.RequestHandler):
            def get(self):
                self.write("Hello, world")

        application = tornado.web.Application([
            (r"/", MainHandler),
        ])

        loop = tornado.ioloop.IOLoop.instance()
        client = CacheClient(loop)

        @gen.coroutine
        def _fetch_it():
            response = yield client.fetch('http://localhost/') # Fetch live.
            print(response.buffer.read())
            assert response.fresh == True
            response = yield client.fetch('http://localhost/') # fetch cached
            print(response.buffer.read())
            assert response.fresh == False  # See?
            client.cache.clear()  # Clean up
            loop.stop()

        if __name__ == "__main__":
            application.listen(80)  # Will need to be root.
            loop.add_callback(_fetch_it)
            loop.start()

