from melkman.green import green_init
green_init()

from datetime import datetime, timedelta
import logging
from helpers import *
import os
import traceback

log = logging.getLogger(__name__)


def test_index_request():
    from melkman.db.remotefeed import RemoteFeed
    from melkman.fetch import request_feed_index
    from melkman.fetch.worker import FeedIndexer
    from melkman.green import consumer_loop
    from eventlet.api import sleep
    from eventlet.proc import spawn

    
    ctx = fresh_context()
    
    # start a feed indexer
    indexer = spawn(consumer_loop, FeedIndexer, ctx)
    
    #start a web server...
    www = os.path.join(data_path(), 'www')
    ts = FileServer(www)
    ts_proc = spawn(ts.run)

    try:
        test_url = ts.url_for('good.xml')
    
        assert RemoteFeed.lookup_by_url(ctx.db, test_url) is None
    
        # make an index request...
        request_feed_index(test_url, ctx)
        sleep(.2)
    
        rf = RemoteFeed.lookup_by_url(ctx.db, test_url)
        assert rf is not None
        assert len(rf.entries.keys()) == 2
        
    finally:
        indexer.kill()
        ts_proc.kill()


def test_schedule_index():
    from melkman.db.remotefeed import RemoteFeed
    from melkman.fetch import schedule_feed_index
    from melkman.fetch.worker import FeedIndexer
    from melkman.scheduler.worker import ScheduledMessageService
    from melkman.green import consumer_loop
    from eventlet.api import sleep
    from eventlet.proc import spawn
    
    ctx = fresh_context()
    
    # start a feed indexer
    indexer = spawn(consumer_loop, FeedIndexer, ctx)
    
    # scheduled message service
    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)

    #start a web server...
    www = os.path.join(data_path(), 'www')
    ts = FileServer(www)
    ts_proc = spawn(ts.run)

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
        ts_proc.kill()
            
def test_push_index():
    from melkman.db.remotefeed import RemoteFeed
    from melkman.fetch import push_feed_index
    from melkman.fetch.worker import FeedIndexer
    from melkman.green import consumer_loop
    from eventlet.api import sleep
    from eventlet.proc import spawn
    
    ctx = fresh_context()
    
    # start a feed indexer
    indexer = spawn(consumer_loop, FeedIndexer, ctx)
    
    url = 'http://www.example.com/feeds/2'
    content = random_atom_feed(url, 10)
    ids = melk_ids_in(content, url)
    
    push_feed_index(url, content, ctx)
    sleep(.2)
    
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    for iid in ids:
        assert iid in rf.entries

    indexer.kill()

def test_push_index_digest():
    from melk.util.nonce import nonce_str
    from melkman.db.remotefeed import RemoteFeed
    from melkman.fetch import push_feed_index
    from melkman.fetch.worker import FeedIndexer
    from melkman.green import consumer_loop
    from eventlet.api import sleep
    from eventlet.proc import spawn
    from sha import new as sha1

    ctx = fresh_context()

    # start a feed indexer
    indexer = spawn(consumer_loop, FeedIndexer, ctx)

    url = 'http://www.example.com/feeds/2'
    rf = RemoteFeed.create_from_url(url)
    rf.save(ctx)
    
    secret = nonce_str()
    
    content = random_atom_feed(url, 10)
    ids = melk_ids_in(content, url)

    hasher = sha1()
    hasher.update(secret)
    hasher.update(content)
    correct_digest = 'sha1=%s' % hasher.hexdigest()
    wrong_digest = 'wrong digest'

    #
    # no hub secret is specified on the feed
    #
    push_feed_index(url, content, ctx, digest=wrong_digest)
    sleep(.2)
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    for iid in ids:
        assert iid not in rf.entries
    push_feed_index(url, content, ctx, digest=None)
    sleep(.2)
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    for iid in ids:
        assert iid not in rf.entries
    # even the correct digest fails as no digest has been set 
    push_feed_index(url, content, ctx, digest=correct_digest)
    sleep(.2)
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    for iid in ids:
        assert iid not in rf.entries

    #
    # now set the hub secret
    #
    rf.hub_info.secret = secret
    rf.save(ctx)

    push_feed_index(url, content, ctx, digest=wrong_digest)
    sleep(.2)
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    for iid in ids:
        assert iid not in rf.entries
    push_feed_index(url, content, ctx, digest=None)
    sleep(.2)
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    for iid in ids:
        assert iid not in rf.entries
    
    # finally, the correct digest should work now...
    push_feed_index(url, content, ctx, digest=correct_digest)
    sleep(.2)
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    for iid in ids:
        assert iid in rf.entries
    
    indexer.kill()
    
