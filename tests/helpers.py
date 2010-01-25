from melkman.green import green_init
green_init()

from datetime import datetime, timedelta
from eventlet.green import socket
from eventlet.support.greenlets import GreenletExit
from eventlet.wsgi import server as wsgi_server
import os
import time
from urlparse import urlsplit
from urllib import quote_plus
from webob import Request, Response

from melk.util.dibject import Dibject, dibjectify
from melk.util.hash import melk_id    
from melk.util.nonce import nonce_str

from melkman.context import Context

__all__ = ['make_db', 'fresh_context', 'data_path', 'test_yaml_file', 'random_id', 'rfc3339_date', 'melk_ids_in', 'random_atom_feed',
           'make_atom_feed', 'dummy_atom_entries', 'make_atom_entry', 'dummy_news_item', 'epeq_datetime',
           'append_param', 'no_micro', 'TestHTTPServer', 'FileServer', 'contextual']


def data_path():
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(here, 'data')

def test_yaml_file():
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(here, 'test.yaml')
        
def make_db():
    ctx = fresh_context()
    return ctx.db

def fresh_context():
    ctx = Context.from_yaml(test_yaml_file())
    ctx.bootstrap(purge=True)
    return ctx

def random_id():
    return melk_id(nonce_str())

def rfc3339_date(timestamp):
    """
    accepts datetime
    returns RFC 3339 date
    """
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', timestamp.timetuple())

def melk_ids_in(content, url):
    from melkman.parse import parse_feed
    fp = parse_feed(content, url)
    return [x.melk_id for x in fp.entries]

def random_atom_feed(feed_id, nentries, base_timestamp=None, **kw):
    if base_timestamp is None:
        base_timestamp = datetime.utcnow()
    entries = dummy_atom_entries(nentries, base_timestamp)
    return make_atom_feed(feed_id, entries, timestamp=base_timestamp + timedelta(seconds=nentries), **kw)

def make_atom_feed(feed_id, entries,
                    title='Some Dummy Feed',
                    timestamp=None,
                    link='http://example.org/feed',
                    author='Jane Dough',
                    hub_urls=None):
    if timestamp is None:
        timestamp = datetime.utcnow()
    updated_str = rfc3339_date(timestamp)

    doc = """<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <id>%s</id>
      <title>%s</title>
      <link rel="self" href="%s"/>
      <updated>%s</updated>
      <author>
        <name>%s</name>
      </author>
    """ % (feed_id, title, link, updated_str, author)

    if hub_urls is not None:
        for hub_url in hub_urls:
            doc += '<link rel="hub" href="%s" />' % hub_url

    for entry in entries:
        doc += entry

    doc += "</feed>"

    return doc

def dummy_atom_entries(n, base_timestamp=None):
    if base_timestamp is None:
        base_timestamp = datetime.utcnow()

    entries = []
    for i in range(n):
        iid = random_id()
        timestamp = base_timestamp + timedelta(seconds=i)
        entries.append(make_atom_entry(iid, timestamp=timestamp))
    entries.reverse()
    return entries

def make_atom_entry(id, title='This is the title',
                    author='Jane Dough',
                    link='http://example.com/link',
                    timestamp=None,
                    summary='Some Text.'):
    if timestamp is None:
        timestamp = datetime.utcnow()
    updated_str = rfc3339_date(timestamp)

    return """<entry>
           <id>%s</id>
           <title>%s</title>
           <link rel="alternate" href="%s"/>
           <author><name>%s</name></author>
           <updated>%s</updated>
           <summary>%s</summary>
           </entry>
           """ % (id, title, link, author, updated_str, summary)

class DummyItem(Dibject):

    def load_full_item(self, db):
        return self
        
def dummy_news_item(d):
    di = DummyItem(dibjectify(d))
    di.setdefault('author', 'Whoever T. Merriweather')
    di.setdefault('item_id', random_id())
    di.setdefault('timestamp', datetime.utcnow())
    di.setdefault('title', 'The News Title')
    di.setdefault('link', 'http://example.org/blagosphere?id=12')
    di.setdefault('source_title', 'The Blags')
    di.setdefault('source_url', 'http://example.org/blagosphere')
    di.setdefault('summary', 'abaraljsrs sjrkja rsj klrjewori ew rwa riojweroiwer iowr wre')
    di.setdefault('details', Dibject())

    return di

class TestHTTPServer(object):

    def __init__(self, port=9291):
        self.port = port

    def run(self):
        try:
            server = socket.socket()
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', self.port))
            server.listen(50)
            wsgi_server(server, self)
        except GreenletExit:
            pass

    def __call__(self, environ, start_response):
        res = Response()
        res.status = 404
        return res(environ, start_response)

class FileServer(TestHTTPServer):
    """
    little file server for testing
    """

    def __init__(self, www_dir, port=9292):
        TestHTTPServer.__init__(self, port)
        self.requests = 0
        self.www_dir = os.path.abspath(www_dir)

    def url_for(self, path):
        return 'http://localhost:%d/%s' % (self.port, path)

    def __call__(self, environ, start_response):
        self.requests += 1
        req = Request(environ)
        res = Response()

        filename = req.path_info.lstrip('/')
        filename = os.path.abspath(os.path.join(self.www_dir, filename))

        if filename.startswith(self.www_dir) and os.path.isfile(filename):
            res.status = 200
            res.body = open(filename).read()
        else:
            res.status = 404

        return res(environ, start_response)

def epeq_datetime(t1, t2):
    return abs(t1 - t2) < timedelta(seconds=1)

def no_micro(dt):
    return dt.replace(microsecond=0)
    
def append_param(url, k, v):
    if len(urlsplit(url)[3]) > 0:
        return '%s&%s=%s' % (url, quote_plus(k), quote_plus(v))
    else: 
        return '%s?%s=%s' % (url, quote_plus(k), quote_plus(v))

def contextual(t):
    from eventlet import sleep
    from greenamqp.client_0_8 import connection
    connection.DEBUG_LEAKS = True
    def inner():
        start_connections = connection.connection_count
        ctx = fresh_context()
        with ctx:
            rc = t(ctx)
        sleep(0)
        assert len(ctx._locals_by_greenlet) == 0, 'Leaked %d greenlet storages' % len(ctx._locals_by_greenlet)
        assert ctx._broker is None, 'Broker connection was not closed.'
        end_connections = connection.connection_count
        assert start_connections == end_connections, 'Leaked %d amqp connections (%d leaked in total)' % (end_connections - start_connections, end_connections)
        return rc
    inner.__name__ = t.__name__
    return inner