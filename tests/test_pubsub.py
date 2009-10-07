from melkman.green import green_init
green_init()


import logging 
from helpers import *

log = logging.getLogger(__name__)

def test_sub_verify():
    from httplib2 import Http
    from eventlet.proc import spawn
    from melk.util.nonce import nonce_str
    from melkman.db import RemoteFeed
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for
    
    import logging
    logging.basicConfig(level=logging.WARN)
    
    ctx = fresh_context()
    
    w = WSGISubClient(ctx)
    client = spawn(w.run)
    
    http = Http()
    url = 'http://example.org/feed/0'
    challenge = nonce_str()
    verify_token = nonce_str()
    secret = nonce_str()
    
    cb = callback_url_for(url, ctx)
    cb += '&hub.mode=subscribe'
    cb += '&hub.topic=%s' % url
    cb += '&hub.challenge=%s' % challenge
    cb += '&hub.verify_token=%s' % verify_token
    
    # try verifying something that doesn't exist
    r, c = http.request(cb, 'GET')
    assert r.status == 404, 'Expected 404, got %d' % r.status
    
    # now create it
    rf = RemoteFeed.create_from_url(url)
    rf.feed_info = {"links": [{"rel": "self", "href": url}]}
    rf.save(ctx)
    
    # still should not verify
    r, c = http.request(cb, 'GET')
    assert r.status == 404, 'Expected 404, got %d' % r.status

    
    # now set appropriate fields on the feed object
    rf.hub_info.enabled = True
    rf.hub_info.verify_token = verify_token
    rf.hub_info.secret = secret
    rf.save(ctx)
    
    # now it should accept verification...
    for i in range(3):
        r, c = http.request(cb, 'GET')
        assert r.status == 200, 'Expected 200, got %d' % r.status
        assert c == challenge, 'expected %s, got %s' % (challence, c)
        
    # create unsubscribe callback...
    cb = callback_url_for(url, ctx)
    cb += '&hub.mode=unsubscribe'
    cb += '&hub.topic=%s' % url
    cb += '&hub.challenge=%s' % challenge
    cb += '&hub.verify_token=%s' % verify_token

    # currently it should fail, we are not unsubscribed
    r, c = http.request(cb, 'GET')
    assert r.status == 404, 'Expected 404, got %d' % r.status
        
    # after disabling, the unsub verify should be okay
    rf.hub_info.enabled = False
    rf.save(ctx)
    r, c = http.request(cb, 'GET')
    assert r.status == 200, 'Expected 200, got %d' % r.status
    assert c == challenge, 'expected %s, got %s' % (challence, c)

    
    # now destroy the feed entirely,
    # unsub request for stuff that 
    # does not exist should also 
    # verify.
    del ctx.db[rf.id]    
    r, c = http.request(cb, 'GET')
    assert r.status == 200, 'Expected 200, got %d' % r.status
    assert c == challenge, 'expected %s, got %s' % (challence, c)
    
    client.kill()

def test_sub_push():
    from httplib2 import Http
    from eventlet.api import sleep
    from eventlet.proc import spawn
    from melk.util.nonce import nonce_str
    from melkman.green import consumer_loop
    from melkman.db import RemoteFeed
    from melkman.fetch.worker import FeedIndexer
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for
    from sha import new as sha1
    
    import logging
    logging.basicConfig(level=logging.WARN)
    
    ctx = fresh_context()
    
    w = WSGISubClient(ctx)
    client = spawn(w.run)
    indexer = spawn(consumer_loop, FeedIndexer, ctx)
    
    http = Http()
    url = 'http://example.org/feed/0'
    content = random_atom_feed(url, 10)
    secret = nonce_str()
    
    hasher = sha1()
    hasher.update(secret)
    hasher.update(content)
    digest = "sha1=%s" % hasher.hexdigest()

    
    cb = callback_url_for(url, ctx)
    
    assert RemoteFeed.lookup_by_url(ctx.db, url) == None
    
    # try posting something that is not subscribed
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(1)
    # nothing should happen...
    assert RemoteFeed.lookup_by_url(ctx.db, url) == None

    # set up the feed, but don't subscribe
    rf = RemoteFeed.create_from_url(url)
    rf.save(ctx)
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(1)
    # nothing should happen...
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    assert len(rf.entries) == 0
    
    # now set it up 
    rf.hub_info.enabled = True
    rf.hub_info.secret = secret
    rf.save(ctx)

    # try with wrong digest...
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': 'wrong'})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.2)
    # nothing should happen...
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    assert len(rf.entries) == 0
    
    # try with no digest
    r, c = http.request(cb, 'POST', body=content)
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.2)
    # nothing should happen...
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    assert len(rf.entries) == 0
    
    # finally, try with correct digest
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.2)
    # nothing should happen...
    rf = RemoteFeed.lookup_by_url(ctx.db, url)
    assert len(rf.entries) == 10
    for iid in melk_ids_in(content, url):
        assert iid in rf.entries 
    
    client.kill()
    indexer.kill()
    
def test_sub_to_hub():
    """
    test make_sub_request and make_unsub_request
    """
    
    from httplib2 import Http
    from eventlet.api import sleep
    from eventlet.proc import spawn
    from melk.util.nonce import nonce_str
    from sha import new as sha1
    import traceback
    from webob import Request, Response

    from melkman.green import consumer_loop
    from melkman.db import RemoteFeed
    from melkman.fetch.worker import FeedIndexer
    from melkman.fetch.pubsubhubbub import WSGISubClient
    from melkman.fetch.pubsubhubbub import callback_url_for
    from melkman.fetch.pubsubhubbub import hubbub_sub
    from melkman.fetch.pubsubhubbub import hubbub_unsub

    
    import logging
    logging.basicConfig(level=logging.WARN)
    
    ctx = fresh_context()
    
    class TestHub(TestHTTPServer):

        def __init__(self, port=9299):
            TestHTTPServer.__init__(self, port=port)
            self._verified = {}

        def __call__(self, environ, start_response):
            log.debug("TestHub got request: %s" % environ)

            req = Request(environ)
            res = Response()
            
            try:
                if req.method != 'POST':
                    res.status = 400
                    return
            
                cb = req.POST['hub.callback']
                mode = req.POST['hub.mode']
                topic = req.POST['hub.topic']
                verify_token = req.POST.get('hub.verify_token', None)            

                if not mode in ('subscribe', 'unsubscribe'):
                    res.status = 400
                    return
                
                # if the request already reflects the current state, return 204
                if (((cb, topic) in self._verified and mode == 'subscribe') or
                    ((cb, topic) not in self._verified and mode == 'unsubscribe')):
                    res.status = 204
                    return

                # do a verification...
                challenge = nonce_str()
            
                vurl = cb
                vurl = append_param(vurl, 'hub.mode', mode)
                vurl = append_param(vurl, 'hub.topic', topic)
                vurl = append_param(vurl, 'hub.challenge', challenge)
                if verify_token:
                    vurl = append_param(vurl, 'hub.verify_token', verify_token)

                http = Http()
                r, c = http.request(vurl, 'GET')
                
                if r.status != 200 or c != challenge:
                    log.warn("Request did not validate :/ %s" % vurl)
                    res.status = 400
                    return
                
                # okay it was fine...
                if mode == 'subscribe':
                    secret = req.POST.get('hub.secret', None)
                    self._verified[(cb, topic)] = secret
                else:
                    try:
                        del self._verified[(cb, topic)]
                    except KeyError:
                        pass
    
                res.status = 202
                
            except:
                log.error("Error handling hub request: %s" % traceback.format_exc())
                res.status = 500
            finally:
                log.debug("Returning w/ status=%s" % res.status)
                return res(environ, start_response)
        
        def is_verified(self, cb, topic_url):
            return (cb, topic_url) in self._verified

        def secret_for(self, cb, topic_url):
            return self._verified.get((cb, topic_url), None)

    w = WSGISubClient(ctx)
    client = spawn(w.run)
    indexer = spawn(consumer_loop, FeedIndexer, ctx)

    hub = TestHub()
    hub_proc = spawn(hub.run)
    
    hub_url = 'http://localhost:%d/' % hub.port
    
    feed_url = 'http://example.org/feeds/99'
    rf = RemoteFeed.create_from_url(feed_url)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url},
                              {'rel': 'hub', 'href': hub_url}]}
    rf.save(ctx)
    
    cb = callback_url_for(feed_url, ctx)
    
    assert not hub.is_verified(cb, feed_url)
    r, c = hubbub_sub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.2)
    assert hub.is_verified(cb, feed_url)
    secret = hub.secret_for(cb, feed_url)
    
    http = Http()

    # try a push (should work)
    content = random_atom_feed(feed_url, 10, link=feed_url)
    hasher = sha1()
    hasher.update(secret)
    hasher.update(content)
    digest = "sha1=%s" % hasher.hexdigest()    
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.2)
    # nothing should happen...
    rf = RemoteFeed.lookup_by_url(ctx.db, feed_url)
    assert len(rf.entries) == 10
    for iid in melk_ids_in(content, feed_url):
        assert iid in rf.entries
    
    # unsub
    r, c = hubbub_unsub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.2)
    assert not hub.is_verified(cb, feed_url)
    
    # try a push (should fail)
    content = random_atom_feed(feed_url, 10, link=feed_url)
    hasher = sha1()
    hasher.update(secret)
    hasher.update(content)
    digest = "sha1=%s" % hasher.hexdigest()    
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.2)
    # nothing should happen...
    rf = RemoteFeed.lookup_by_url(ctx.db, feed_url)
    assert len(rf.entries) == 10
    for iid in melk_ids_in(content, feed_url):
        assert not iid in rf.entries
        
    client.kill()
    indexer.kill()
    hub_proc.kill()

