from melkman.green import green_init
green_init()

from httplib2 import Http
import logging 
from melk.util.nonce import nonce_str
import traceback
from webob import Request, Response
from helpers import *

log = logging.getLogger(__name__)

class FakeHub(TestHTTPServer):
    """
    This is a test fixture that responds to requests 
    like a pubsubhubbub hub and tracks certain info.
    
    Only supports subscribe / unsubscribe workflows. callbacks
    are simulated directly in tests.
    """
    
    def __init__(self, port=9299, lease_seconds=999999):
        TestHTTPServer.__init__(self, port=port)
        self._verified = {}
        self.lease_seconds = lease_seconds
        self._renewals = {}

    def __call__(self, environ, start_response):
        log.debug("FakeHub got request: %s" % environ)

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
            
            # subscribe when already subscribed
            if (cb, topic) in self._verified and mode == 'subscribe':
               res.status = 204
               # track lease renewals
               self._renewals.setdefault((cb, topic), 0)
               self._renewals[(cb, topic)] += 1
               return
                
            # unsubscribe and not currently subscribed
            if (cb, topic) not in self._verified and mode == 'unsubscribe':
                res.status = 204
                return

            # do a verification...
            challenge = nonce_str()
        
            vurl = cb
            vurl = append_param(vurl, 'hub.mode', mode)
            vurl = append_param(vurl, 'hub.topic', topic)
            vurl = append_param(vurl, 'hub.challenge', challenge)
            vurl = append_param(vurl, 'hub.lease_seconds', '%d' % self.lease_seconds)
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
                    del self._renewals[(cb, topic)]
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

    def renewals(self, cb, topic_url):
        return self._renewals.get((cb, topic_url), 0)

@check_leaks
def test_sub_verify():
    from httplib2 import Http
    from eventlet import spawn
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
    cb += '?hub.mode=subscribe'
    cb += '&hub.topic=%s' % url
    cb += '&hub.challenge=%s' % challenge
    cb += '&hub.verify_token=%s' % verify_token
    
    # try verifying something that doesn't exist
    r, c = http.request(cb, 'GET')
    assert r.status == 404, 'Expected 404, got %d' % r.status
    
    # now create it
    rf = RemoteFeed.create_from_url(url, ctx)
    rf.feed_info = {"links": [{"rel": "self", "href": url}]}
    rf.save()
    
    # still should not verify
    r, c = http.request(cb, 'GET')
    assert r.status == 404, 'Expected 404, got %d' % r.status

    
    # now set appropriate fields on the feed object
    rf.hub_info.enabled = True
    rf.hub_info.verify_token = verify_token
    rf.hub_info.secret = secret
    rf.save()
    
    # now it should accept verification...
    for i in range(3):
        r, c = http.request(cb, 'GET')
        assert r.status == 200, 'Expected 200, got %d' % r.status
        assert c == challenge, 'expected %s, got %s' % (challence, c)
        
    # create unsubscribe callback...
    cb = callback_url_for(url, ctx)
    cb += '?hub.mode=unsubscribe'
    cb += '&hub.topic=%s' % url
    cb += '&hub.challenge=%s' % challenge
    cb += '&hub.verify_token=%s' % verify_token

    # currently it should fail, we are not unsubscribed
    r, c = http.request(cb, 'GET')
    assert r.status == 404, 'Expected 404, got %d' % r.status
        
    # after disabling, the unsub verify should be okay
    rf.reload()
    rf.hub_info.enabled = False
    rf.save()
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
    client.wait()
    ctx.close()

@check_leaks
def test_sub_push():
    from httplib2 import Http
    from eventlet import sleep, spawn
    from melk.util.nonce import nonce_str
    from melkman.db import RemoteFeed
    from melkman.fetch.worker import run_feed_indexer
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for, psh_digest
    
    import logging
    logging.basicConfig(level=logging.WARN)
    
    ctx = fresh_context()
    
    w = WSGISubClient(ctx)
    client = spawn(w.run)
    indexer = spawn(run_feed_indexer, ctx)
    
    http = Http()
    url = 'http://example.org/feed/0'
    content = random_atom_feed(url, 10)
    secret = nonce_str()
    
    digest = 'sha1=%s' % psh_digest(content, secret)
    cb = callback_url_for(url, ctx)
    
    assert RemoteFeed.get_by_url(url, ctx) == None
    
    # try posting something that is not subscribed
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(1)
    # nothing should happen...
    assert RemoteFeed.get(url, ctx) == None

    # set up the feed, but don't subscribe
    rf = RemoteFeed.create_from_url(url, ctx)
    rf.save()
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(1)
    # nothing should happen...
    rf = RemoteFeed.get_by_url(url, ctx)
    assert len(rf.entries) == 0
    
    # now set it up 
    rf.hub_info.enabled = True
    rf.hub_info.subscribed = True
    rf.hub_info.secret = secret
    rf.save()

    # try with wrong digest...
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': 'wrong'})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.5)
    # nothing should happen...
    rf = RemoteFeed.get_by_url(url, ctx)
    assert len(rf.entries) == 0
    
    # try with no digest
    r, c = http.request(cb, 'POST', body=content)
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.5)
    # nothing should happen...
    rf = RemoteFeed.get_by_url(url, ctx)
    assert len(rf.entries) == 0
    
    # finally, try with correct digest
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.5)
    # nothing should happen...
    rf = RemoteFeed.get_by_url(url, ctx)
    assert len(rf.entries) == 10
    for iid in melk_ids_in(content, url):
        assert iid in rf.entries 
    
    client.kill()
    client.wait()
    indexer.kill()
    indexer.wait()
    ctx.close()

@check_leaks
def test_sub_to_hub():
    """
    test make_sub_request and make_unsub_request
    """
    
    from httplib2 import Http
    from eventlet import sleep, spawn
    from melk.util.nonce import nonce_str
    import traceback

    from melkman.db import RemoteFeed
    from melkman.fetch.worker import run_feed_indexer
    from melkman.fetch.pubsubhubbub import WSGISubClient
    from melkman.fetch.pubsubhubbub import callback_url_for
    from melkman.fetch.pubsubhubbub import hubbub_sub
    from melkman.fetch.pubsubhubbub import hubbub_unsub
    from melkman.fetch.pubsubhubbub import psh_digest
    
    import logging
    logging.basicConfig(level=logging.WARN)
    
    ctx = fresh_context()
    

    w = WSGISubClient(ctx)
    client = spawn(w.run)
    indexer = spawn(run_feed_indexer, ctx)

    hub = FakeHub()
    hub_proc = spawn(hub.run)
    
    hub_url = 'http://localhost:%d/' % hub.port
    
    feed_url = 'http://example.org/feeds/99'
    rf = RemoteFeed.create_from_url(feed_url, ctx)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url},
                              {'rel': 'hub', 'href': hub_url}]}
    rf.save()
    
    cb = callback_url_for(feed_url, ctx)
    
    # subscribe to the hub
    assert not hub.is_verified(cb, feed_url)
    r, c = hubbub_sub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.5)
    assert hub.is_verified(cb, feed_url)
    secret = hub.secret_for(cb, feed_url)
    
    http = Http()

    # simulate hub posting to callback URL
    content = random_atom_feed(feed_url, 10, link=feed_url, hub_urls=[hub_url])
    digest = 'sha1=%s' % psh_digest(content, secret)
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.5)
    
    # since we are subscribed, new items should be in the feed now
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    assert len(rf.entries) == 10
    for iid in melk_ids_in(content, feed_url):
        assert iid in rf.entries
    
    # unsubscribe from hub
    r, c = hubbub_unsub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.5)
    assert not hub.is_verified(cb, feed_url)
    
    # simulate another POST to the callback URL 
    # this time it should fail (we are not subscribed)
    content = random_atom_feed(feed_url, 10, link=feed_url, hub_urls=[hub_url])
    digest = "sha1=%s" % psh_digest(content, secret)
    r, c = http.request(cb, 'POST', body=content, headers={'X-Hub-Signature': digest})
    assert r.status == 200, 'Expected 200, got %d' % r.status
    sleep(0.5)
    # items should be the same as before (not subscribed)
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    assert len(rf.entries) == 10
    for iid in melk_ids_in(content, feed_url):
        assert not iid in rf.entries
        
    client.kill()
    client.wait()
    indexer.kill()
    indexer.wait()
    hub_proc.kill()
    hub_proc.wait()
    ctx.close()

@check_leaks
def test_auto_sub():
    # tests autosubscription when feeds are indexed 
    # with <link rel="hub" /> entries. 
    from datetime import datetime
    from eventlet import sleep, spawn
    from melkman.db import RemoteFeed
    from melkman.fetch import push_feed_index
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for
    from melkman.fetch.worker import run_feed_indexer

    ctx = fresh_context()
    
    w = WSGISubClient(ctx)
    client = spawn(w.run)
    indexer = spawn(run_feed_indexer, ctx)

    hub = FakeHub()
    hub_proc = spawn(hub.run)    
    hub_url = 'http://localhost:%d/' % hub.port

    feed_url = 'http://www.example.org/feeds/12'

    content = """<?xml version="1.0" encoding="utf-8"?>
      <feed xmlns="http://www.w3.org/2005/Atom">
      <id>%s</id>
      <title>Blah</title>
      <link rel="self" href="%s"/>
      <link rel="hub" href="%s" />
      <updated>%s</updated>
      <author>
        <name>Joop Doderer</name>
      </author>
      </feed>
    """ %  (feed_url, feed_url, hub_url,
            rfc3339_date(datetime.utcnow()))

    # push content in...
    push_feed_index(feed_url, content, ctx)
    sleep(.5)
    
    # check for automatic subscription...
    cb = callback_url_for(feed_url, ctx)
    assert hub.is_verified(cb, feed_url)

    rf = RemoteFeed.get_by_url(feed_url, ctx)
    assert rf.hub_info.enabled
    assert rf.hub_info.hub_url == hub_url

    client.kill()
    client.wait()
    indexer.kill()
    indexer.wait()
    hub_proc.kill()
    hub_proc.wait()
    ctx.close()
    
@check_leaks
def test_push_index_digest():
    from melk.util.nonce import nonce_str
    from melkman.db.remotefeed import RemoteFeed
    from melkman.fetch import push_feed_index
    from melkman.fetch.worker import run_feed_indexer
    from eventlet import sleep, spawn
    from melkman.fetch.pubsubhubbub import psh_digest

    ctx = fresh_context()

    # start a feed indexer
    indexer = spawn(run_feed_indexer, ctx)

    url = 'http://www.example.com/feeds/2'
    rf = RemoteFeed.create_from_url(url, ctx)
    rf.hub_info.enabled = True
    rf.hub_info.subscribed = True
    rf.save()

    secret = nonce_str()

    content = random_atom_feed(url, 10)
    ids = melk_ids_in(content, url)

    correct_digest = 'sha1=%s' % psh_digest(content, secret)
    wrong_digest = 'wrong digest'

    #
    # no hub secret is specified on the feed
    #
    push_feed_index(url, content, ctx, digest=wrong_digest, from_hub=True)
    sleep(.5)
    rf = RemoteFeed.get_by_url(url, ctx)
    for iid in ids:
        assert iid not in rf.entries
    push_feed_index(url, content, ctx, digest=None, from_hub=True)
    sleep(.5)
    rf = RemoteFeed.get_by_url(url, ctx)
    for iid in ids:
        assert iid not in rf.entries
    # even the correct digest fails as no digest has been set 
    push_feed_index(url, content, ctx, digest=correct_digest, from_hub=True)
    sleep(.5)
    rf = RemoteFeed.get_by_url(url, ctx)
    for iid in ids:
        assert iid not in rf.entries

    #
    # now set the hub secret
    #
    rf.hub_info.secret = secret
    rf.save()

    push_feed_index(url, content, ctx, digest=wrong_digest, from_hub=True)
    sleep(.5)
    rf = RemoteFeed.get_by_url(url, ctx)
    for iid in ids:
        assert iid not in rf.entries
    push_feed_index(url, content, ctx, digest=None, from_hub=True)
    sleep(.5)
    rf = RemoteFeed.get_by_url(url, ctx)
    for iid in ids:
        assert iid not in rf.entries

    # finally, the correct digest should work now...
    push_feed_index(url, content, ctx, digest=correct_digest, from_hub=True)
    sleep(.5)
    rf = RemoteFeed.get_by_url(url, ctx)
    for iid in ids:
        assert iid in rf.entries

    indexer.kill()
    indexer.wait()
    ctx.close()

@check_leaks
def test_disabled_unsubscribes():
    """
    tests that if pubsub is disabled for a 
    feed, it becomes unsubscribed from it's 
    hub.
    """
    from eventlet import sleep, spawn
    from melkman.db import RemoteFeed
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for 
    from melkman.fetch.pubsubhubbub import hubbub_sub, update_pubsub_state

    ctx = fresh_context()
    
    w = WSGISubClient(ctx)
    client = spawn(w.run)

    hub = FakeHub()
    hub_proc = spawn(hub.run)    
    hub_url = 'http://localhost:%d/' % hub.port

    feed_url = 'http://example.org/feeds/99'
    rf = RemoteFeed.create_from_url(feed_url, ctx)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url},
                              {'rel': 'hub', 'href': hub_url}]}
    rf.save()
    
    # subscribe to the feed on the hub
    cb = callback_url_for(feed_url, ctx)
    assert not hub.is_verified(cb, feed_url)
    r, c = hubbub_sub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.5)
    assert hub.is_verified(cb, feed_url)
    
    # disable pubsub for the feed
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    assert rf.hub_info.enabled == True and rf.hub_info.subscribed == True
    rf.hub_info.enabled = False
    rf.save()
    
    # trigger an update
    update_pubsub_state(rf, ctx)
    
    # check that it is now unsubscribed.
    sleep(.5)
    assert not hub.is_verified(cb, feed_url)
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    assert rf.hub_info.enabled == False and rf.hub_info.subscribed == False
    
    client.kill()
    client.wait()
    hub_proc.kill()
    hub_proc.wait()
    ctx.close()

@check_leaks
def test_hub_invalidation():
    """
    tests that if a currently subscribed hub is 
    no longer listed by a feed unsubscription is 
    performed.
    """
    from eventlet import sleep, spawn
    from melkman.db import RemoteFeed
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for 
    from melkman.fetch.pubsubhubbub import hubbub_sub, update_pubsub_state

    ctx = fresh_context()
    
    w = WSGISubClient(ctx)
    client = spawn(w.run)

    hub = FakeHub()
    hub_proc = spawn(hub.run)    
    hub_url = 'http://localhost:%d/' % hub.port

    feed_url = 'http://example.org/feeds/99'
    rf = RemoteFeed.create_from_url(feed_url, ctx)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url},
                              {'rel': 'hub', 'href': hub_url}]}
    rf.save()
    
    # subscribe to the feed on the hub
    cb = callback_url_for(feed_url, ctx)
    assert not hub.is_verified(cb, feed_url)
    r, c = hubbub_sub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.5)
    assert hub.is_verified(cb, feed_url)

    # remove the hub from the list of hubs
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url}]}
    rf.save()

    # trigger an update
    update_pubsub_state(rf, ctx)
    
    # check that it is now unsubscribed.
    sleep(.5)
    assert not hub.is_verified(cb, feed_url)
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    assert rf.hub_info.enabled == True and rf.hub_info.subscribed == False
    
    client.kill()
    client.wait()
    hub_proc.kill()
    hub_proc.wait()
    ctx.close()
    
@check_leaks
def test_hub_invalidation_resub():
    """
    tests that if a currently subscribed hub is 
    no longer listed, we subscribe to a different
    hub if any are listed.
    """
    from eventlet import sleep, spawn
    from melkman.db import RemoteFeed
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for 
    from melkman.fetch.pubsubhubbub import hubbub_sub, update_pubsub_state

    ctx = fresh_context()
    
    w = WSGISubClient(ctx)
    client = spawn(w.run)

    # create two hubs
    hub = FakeHub()
    hub_proc = spawn(hub.run)    
    hub_url = 'http://localhost:%d/' % hub.port

    hub2 = FakeHub(port=9298)
    hub2_proc = spawn(hub2.run)    
    hub2_url = 'http://localhost:%d/' % hub2.port


    feed_url = 'http://example.org/feeds/99'
    rf = RemoteFeed.create_from_url(feed_url, ctx)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url},
                              {'rel': 'hub', 'href': hub_url}]}
    rf.save()
    
    # subscribe to the feed on the hub
    cb = callback_url_for(feed_url, ctx)
    assert not hub.is_verified(cb, feed_url)
    r, c = hubbub_sub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.5)
    assert hub.is_verified(cb, feed_url)

    # remove the hub from the list of hubs, but replace it with another
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url}, 
                              {'rel': 'hub', 'href': hub2_url}]}
    rf.save()

    # trigger an update
    update_pubsub_state(rf, ctx)
    
    # check that it is now unsubscribed from the original hub, and 
    # is now subscribed to the new hub.
    sleep(2)
    assert not hub.is_verified(cb, feed_url)
    assert hub2.is_verified(cb, feed_url)
    
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    assert rf.hub_info.enabled == True and rf.hub_info.subscribed == True
    
    client.kill()
    client.wait()
    hub_proc.kill()
    hub_proc.wait()
    hub2_proc.kill()
    hub2_proc.wait()
    ctx.close()
    
@check_leaks
def test_hub_lease_renew():
    """
    tests that we resubscribe with a hub 
    within the hub specified lease window.
    """
    from eventlet import sleep, spawn
    from melkman.db import RemoteFeed
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for 
    from melkman.fetch.pubsubhubbub import hubbub_sub, update_pubsub_state

    ctx = fresh_context()
    
    w = WSGISubClient(ctx)
    client = spawn(w.run)

    # create a hub with a very short lease time.
    hub = FakeHub(lease_seconds=2)
    hub_proc = spawn(hub.run)    
    hub_url = 'http://localhost:%d/' % hub.port

    feed_url = 'http://example.org/feeds/99'
    rf = RemoteFeed.create_from_url(feed_url, ctx)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url},
                              {'rel': 'hub', 'href': hub_url}]}
    rf.save()
    
    # subscribe to the feed on the hub
    cb = callback_url_for(feed_url, ctx)
    assert not hub.is_verified(cb, feed_url)
    r, c = hubbub_sub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.5)
    assert hub.is_verified(cb, feed_url)
    rf = RemoteFeed.get_by_url(feed_url, ctx)

    assert hub.renewals(cb, feed_url) == 0
    
    sleep(2)
    
    update_pubsub_state(rf, ctx)
    
    # make sure we triggered a lease renewal
    assert hub.renewals(cb, feed_url) == 1

    client.kill()
    client.wait()
    hub_proc.kill()
    hub_proc.wait()
    ctx.close()

@check_leaks    
def test_hub_lease_renew_failover():
    """
    tests that if we fail to renew a lease with a hub 
    we will failover to a different hub if one is available.
    """
    from eventlet import sleep, spawn
    from melkman.db import RemoteFeed
    from melkman.fetch.pubsubhubbub import WSGISubClient, callback_url_for 
    from melkman.fetch.pubsubhubbub import hubbub_sub, update_pubsub_state

    ctx = fresh_context()

    w = WSGISubClient(ctx)
    client = spawn(w.run)

    # create a hub with a very short lease time.
    hub = FakeHub(lease_seconds=2)
    hub_proc = spawn(hub.run)    
    hub_url = 'http://localhost:%d/' % hub.port

    hub2 = FakeHub(port=9298)
    hub2_proc = spawn(hub2.run)
    hub2_url = 'http://localhost:%d/' % hub2.port

    feed_url = 'http://example.org/feeds/99'
    rf = RemoteFeed.create_from_url(feed_url, ctx)
    rf.feed_info = {'links': [{'rel': 'self', 'href': feed_url},
                              {'rel': 'hub', 'href': hub_url}, 
                              {'rel': 'hub', 'href': hub2_url}]}
    rf.save()

    # subscribe to the feed on the hub
    cb = callback_url_for(feed_url, ctx)
    assert not hub.is_verified(cb, feed_url)
    r, c = hubbub_sub(rf, ctx)
    assert r.status == 202, 'Expected 202, got %d' % r.status
    sleep(.5)
    
    rf = RemoteFeed.get_by_url(feed_url, ctx)
    assert rf.hub_info.subscribed == True
    assert rf.hub_info.hub_url == hub_url
    assert hub.is_verified(cb, feed_url)
    assert not hub2.is_verified(cb, feed_url)

    assert hub.renewals(cb, feed_url) == 0
    sleep(2)
    
    # kill the first hub so that when we update, 
    # the renewal will fail...
    hub_proc.kill()

    # when this update is triggered, renewal should fail and 
    # we should instead subscribe to the alternate hub.
    update_pubsub_state(rf, ctx)
    
    assert hub.renewals(cb, feed_url) == 0
    rf = RemoteFeed.get_by_url(feed_url, ctx)    
    assert rf.hub_info.subscribed == True
    assert rf.hub_info.hub_url == hub2_url
    assert hub2.is_verified(cb, feed_url)

    client.kill()
    client.wait()
    hub2_proc.kill()
    hub2_proc.wait()
    ctx.close()
