from melkman.green import green_init
green_init()

from datetime import datetime, timedelta
import logging
import helpers
from threading import Thread
import BaseHTTPServer
import SimpleHTTPServer
import os
import traceback

log = logging.getLogger(__name__)

class FetchServer(Thread):
    """HTTP Server that runs in a thread and handles a predetermined number of requests"""

    TEST_PORT = 9292

    def __init__(self, requests, www_dir):
        Thread.__init__(self)
        self.requests = requests
        self.ready = 0
        self.www_dir = www_dir

    def url_for(self, path):
        return 'http://localhost:%d/%s' % (self.TEST_PORT, path)

    def run(self):
        os.chdir(self.www_dir)
        Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
        self.httpd = BaseHTTPServer.HTTPServer(('', self.TEST_PORT), Handler)
        self.ready = 1
        while self.requests:
            self.httpd.handle_request()
            self.requests -= 1
        self.ready = 0

def test_index_request():
    from melkman.db.remotefeed import RemoteFeed
    from melkman.fetch import request_feed_index
    from melkman.fetch.worker import FeedIndexer
    from melkman.green import consumer_loop
    from eventlet.api import sleep
    from eventlet.proc import spawn

    
    ctx = helpers.fresh_context()
    
    # start a feed indexer
    indexer = spawn(consumer_loop, FeedIndexer, ctx)
    
    #start a web server...
    www = os.path.join(helpers.data_path(), 'www')
    ts = FetchServer(1, www)
    ts.setDaemon(True)
    ts.start()
    
    try:
        test_url = ts.url_for('good.xml')
    
        assert RemoteFeed.lookup_by_url(ctx.db, test_url) is None
    
        # make an index request...
        request_feed_index(test_url, ctx)
        sleep(1)
    
        rf = RemoteFeed.lookup_by_url(ctx.db, test_url)
        assert rf is not None
        assert len(rf.entries.keys()) == 2
        
    finally:
        indexer.kill()
    
        ts.join(2)
        if ts.isAlive():
            # make a request to kill it off
            from urllib2 import urlopen
            urlopen(test_url)
            ts.join()
            assert 0, 'Fetch test server did not recieve request!'

def test_schedule_index():
    from melkman.db.remotefeed import RemoteFeed
    from melkman.fetch import schedule_feed_index
    from melkman.fetch.worker import FeedIndexer
    from melkman.scheduler.worker import ScheduledMessageService
    from melkman.green import consumer_loop
    from eventlet.api import sleep
    from eventlet.proc import spawn
    
    ctx = helpers.fresh_context()
    
    # start a feed indexer
    indexer = spawn(consumer_loop, FeedIndexer, ctx)
    
    # scheduled message service
    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)

    #start a web server...
    www = os.path.join(helpers.data_path(), 'www')
    ts = FetchServer(1, www)
    ts.setDaemon(True)
    ts.start()

    test_url = ts.url_for('good.xml')
    # schedule an index request...
    try:
        assert RemoteFeed.lookup_by_url(ctx.db, test_url) is None

        # make an index request...
        when = datetime.utcnow() + timedelta(seconds=2)

        schedule_feed_index(test_url, when, ctx)
        sleep(3)

        rf = RemoteFeed.lookup_by_url(ctx.db, test_url)
        assert rf is not None
        assert len(rf.entries.keys()) == 2
    except:
        log.error("error: %s" % traceback.format_exc())
    finally:
        indexer.kill()
        sched.kill()
    
        ts.join(2)
        if ts.isAlive():
            # make a request to kill it off
            from urllib2 import urlopen
            urlopen(test_url)
            ts.join()
            assert 0, 'Fetch test server did not recieve request!'