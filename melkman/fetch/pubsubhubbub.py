import logging
from giblets import Component, implements
from datetime import datetime, timedelta
from eventlet.green import socket
from eventlet.wsgi import server as wsgi_server
from eventlet.support.greenlets import GreenletExit
import hmac
from httplib2 import Http
import traceback
from urllib import quote_plus, unquote_plus, urlencode
from urlparse import urljoin
from webob import Request, Response

try:
    from hashlib import sha1 # python > 2.5
except ImportError:
    from sha import new as sha1 # python <= 2.5

from melkman.context import IContextConfigurable
from melkman.db import RemoteFeed
from melkman.db.util import backoff_save
from melkman.fetch.api import push_feed_index
from melkman.fetch.api import IndexRequestFilter
from melkman.fetch.api import PostIndexAction
from melkman.worker import IWorkerProcess
from melk.util.nonce import nonce_str

log = logging.getLogger(__name__)

DEFAULT_LEASE = 604800

def callback_url_for(feed_url, context):
    """
    for a given feed url, determine the callback url that 
    a hub should post to update our subscription to 
    the feed.
    """
    base_url = context.config.pubsubhubbub_client.callback_url
    if not base_url.endswith('/'):
        base_url += '/'
    return urljoin(base_url, quote_plus(feed_url))

def topic_url_for(feed):
    """
    for a given RemoteFeed, determine the 'topic url' that
    is used to identify the feed to it's hubs.
    """
    for link in feed.feed_info.get('links', []):
        if link.rel == 'self':
            return link.href
    return None

def _determine_feed_url(req):
    """
    determine which feed url a callback from a hub
    refers to.
    """
    url = unquote_plus(req.path)
    if url.startswith('/'):
        url = url[1:]
    return url

def hubbub_sub(feed, context, hub_url=None):
    """
    subscribe to feed on the pubsubhubbub hub 
    specified or the first hub listed in the 
    feed if none is given.
    """
    topic_url = topic_url_for(feed)
    
    if topic_url is None:
        raise ValueError('No self link found in feed, cannot subscribe via pubsubhubub.')

    if hub_url is None:
        hub_urls = feed.find_hub_urls()
        if len(hub_urls) == 0:
            raise ValueError("Cannot subscribe, no hubs were specified.")
        hub_url = hub_urls[0]
        log.warn("Guessing hub %s for %s" % (hub_url, feed.url))

    feed.hub_info.enabled = True
    feed.hub_info.hub_url = hub_url
    feed.hub_info.verify_token = nonce_str()
    feed.hub_info.secret = nonce_str()
    feed.save()

    cb = callback_url_for(feed.url, context)
    req = [
        ('hub.callback', cb),
        ('hub.mode', 'subscribe'),
        ('hub.topic', topic_url),
        ('hub.verify', 'sync'),
        ('hub.verify', 'async'),
        ('hub.verify_token', feed.hub_info.verify_token),
        ('hub.secret', feed.hub_info.secret)
    ]
    body = urlencode(req)
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    return Http().request(feed.hub_info.hub_url, method="POST", body=body, headers=headers)
        
def hubbub_unsub(feed, context):
    """
    unsubscribe from feed on the current pubsubhubbub hub 
    for the feed.
    """
    topic_url = topic_url_for(feed)
    
    if topic_url is None:
        raise ValueError('No self link found in feed, cannot unsubscribe via pubsubhubub.')

    # immediately mark the feed as unsubscribed.
    if feed.hub_info.subscribed:
        feed.hub_info.subscribed = False
        feed.save()

    # hub unknown, skip POST
    if feed.hub_info.hub_url is None:
        return

    cb = callback_url_for(feed.url, context)
    req = [
        ('hub.callback', cb),
        ('hub.mode', 'unsubscribe'),
        ('hub.topic', topic_url),
        ('hub.verify', 'sync'),
        ('hub.verify', 'async'),
        ('hub.verify_token', feed.hub_info.verify_token)
    ]
    body = urlencode(req)
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    return Http().request(feed.hub_info.hub_url, method="POST", body=body, headers=headers)


class WSGISubClient(object):
    """
    A wsgi application which handles subscription verification 
    and content push requests from a hub.
    """
    def __init__(self, context):
        self.context = context

    def run(self):
        try:
            host = self.context.config.pubsubhubbub_client.host
            port = int(self.context.config.pubsubhubbub_client.port)
            log.info("WSGISubClient starting on %s:%d" % (host, port))
            server = socket.socket()
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((host, port))
            server.listen(50)
            wsgi_server(server, self)
        except GreenletExit: 
            pass
        except: 
            log.error("Unexpected error running WSGISubClient: %s" % traceback.format_exc())
        finally: 
            self.context.close()

    def __call__(self, environ, start_response):
        try:
            req = Request(environ)
            
            if req.method == 'POST':
                res = self.handle_callback(req)
            elif req.method == 'GET':
                res = self.handle_sub_verification(req)
            else:
                res = Response()
                res.status = 400
        except:
            log.error("Error handling PubSubHubBub request: %s" % traceback.format_exc())
            res = Response()
            res.status = 500
        finally:
            try:
                self.context.close()
            except:
                log.error("Error closing context: %s" % traceback.format_exc())
            return res(environ, start_response)
        
    def handle_sub_verification(self, req):
        res = Response()
        mode = req.GET.get('hub.mode', None)
        topic = req.GET.get('hub.topic', None)

        if self._validate_sub_request(req):
            log.info("Got valid '%s' req for '%s'" % (mode, topic))
            res.status = 200
            res.body = req.GET['hub.challenge']
            return res
        else:
            log.warn("Got invalid '%s' req for '%s'" % (mode, topic))
            res.status = 404
        return res

    def _validate_sub_request(self, req):
        mode = req.GET.get('hub.mode', None)
        topic = req.GET.get('hub.topic', None)
        verify_token = req.GET.get('hub.verify_token', None)
        url = _determine_feed_url(req)

        if topic is None or mode is None or verify_token is None or url is None:
            return False

        rf = RemoteFeed.get_by_url(url, self.context)
        if rf is None:
            # for feeds we don't know about, confirm unsubscribes
            # and reject subscribes.
            return mode == 'unsubscribe'

        if topic != topic_url_for(rf):
            log.warn("hub sent mismatched feed / topic: (%s, %s)" % (topic, topic_url_for(rf)))
            return False

        if mode == 'subscribe':
            if (rf.hub_info.enabled and
                rf.hub_info.verify_token == verify_token):
                
                try:
                    lease_time = int(req.GET.get('hub.lease_seconds', DEFAULT_LEASE))
                except:
                    lease_time = DEFAULT_LEASE
                     
                lease_time = min(lease_time, 604800)
                next_sub_time = datetime.utcnow() + timedelta(seconds=lease_time/2)
                
                # mark the feed as subscribed only when we have recieved 
                # a proper subscription verification from the hub.
                rf.hub_info.subscribed = True
                rf.hub_info.next_sub_time = next_sub_time
                rf.save()
                return True
            else:
                return False

        elif mode == 'unsubscribe':
            ps = rf.hub_info
            if ps.enabled and ps.subscribed and verify_token == ps.verify_token:
                """
                deny any valid unsubscribe requests for enabled feeds
                that we believe should be subscribed.
                """
                return False
            # anything else we approve, invalid, disabled, unsubscribed.
            return True


        else:
            log.warn("hub sent unknown sub mode: %s" % mode)
            return False

    def handle_callback(self, req):
        self._handle_callback(req)
        res = Response()
        res.status = 200
        return res

    def _handle_callback(self, req):
        url = _determine_feed_url(req)
        digest = req.headers.get('X-Hub-Signature', None)
        content = req.body
        push_feed_index(url, content, self.context, digest=digest, from_hub=True)

class WSGISubClientProcess(Component):
    implements(IWorkerProcess)
    
    def run(self, context):
        w = WSGISubClient(context)
        w.run()


class HubAutosubscriber(Component):
    """
    Hook that runs after each feed index to try to keep 
    feeds subscribed to appropriate pubsubhubbub hubs
    when enabled.
    """
    implements(PostIndexAction)

    def feed_reindexed(self, feed, context):
        update_pubsub_state(feed, context)


def update_pubsub_state(feed, context):
    """
    perform any (un/re)subscription needed based on the 
    state of the feed given and currently listed 
    hubs.
    """
    hubs = feed.find_hub_urls()
    ps = feed.hub_info

    # if pubsub is disabled for this feed
    if not ps.enabled:
        if ps.subscribed: 
            try:
                hubbub_unsub(feed, context)
            except:
                log.warn("Error unsubscribing from hub %s for feed %s: %s" % 
                         (ps.hub_url, feed.url, traceback.format_exc()))  
        return

    # if the currently subscribed hub is no longer 
    # listed in the feed, unsubscribe from it.
    if ps.subscribed and ps.hub_url not in hubs:
        try:
            hubbub_unsub(feed, context)
        except:
            log.warn("Error unsubscribing from current hub: %s" % 
                     traceback.format_exc())
        feed = RemoteFeed.get(feed.id, context) # refresh

    # if it is time to resubscribe to the current hub, try to 
    # resubscribe
    elif ps.subscribed and datetime.utcnow() > ps.next_sub_time: 
        log.info('resubscribe %s to hub %s' % (feed.url, feed.hub_info.hub_url))
        if not _sub_any(feed, [feed.hub_info.hub_url], context):
            log.warn("Failed to resubscribe to %s for feed %s." % (ps.hub_url, feed.url))
            try:
                hubbub_unsub(feed, context)
            except:
                log.warn("Error unsubscribing from hub %s for feed %s: %s" % 
                         (ps.hub_url, feed.url, traceback.format_exc()))  
                feed = RemoteFeed.get(feed.id, context) # refresh

    # if it is not subscribed, subscribe to first thing 
    # that works.
    if not ps.subscribed:
        _sub_any(feed, hubs, context)                


def _sub_any(feed, hubs, context):
    """
    suscribe to any hub in the list of hubs given that
    works. 
    """
    for hub in hubs:
        try:
            # use exponential backoff method to avoid
            # conflicts when setting up.
            log.debug("Trying to subscribe to %s at hub %s" % (feed.url, hub))
            def try_sub(tries):
                if tries > 1:
                    ff = RemoteFeed.load(context.db, feed.id)
                else:
                    ff = feed
                return hubbub_sub(ff, context, hub_url=hub)
            r, c = backoff_save(try_sub, pass_count=True)
            
            if r.status >= 200 and r.status < 300:
                log.info("Subscribed to %s at hub %s (%d)" % (feed.url, hub, r.status))
                return True
            else: 
                log.info("Failed to subscribe to %s at hub %s (%d)" % (feed.url, hub, r.status))
        except: 
            log.error("Failed to subscribe to %s at hub %s: %s" % (feed.url, hub, traceback.format_exc()))
            
    return False

class HubPushValidator(Component):
    """
    validates requests that are pushed from 
    a pubsubhubbub hub.
    """
    implements(IndexRequestFilter)

    def accepts_request(self, feed, request, context):
        # only validate requests marked as from a hub
        if not request.get('from_hub', False) == True:
            return True

        content = request.get('content', '')
        if not feed.hub_info.enabled or feed.hub_info.subscribed == False:
            log.warn("Ignoring hub push for unsubscribed feed.")
            return False

        if 'digest' in request:
            if not _digest_matches(request['digest'], content, feed.hub_info.secret):
                log.warn("Rejecting content push: digest (%s) did not match!" % request['digest'])
                return False

        return True


def psh_digest(content, secret):
    """
    compute digest of content and secret
    according to pubsubhubbub spec
    """
    return hmac.new(secret.encode('utf-8'), 
                    content.encode('utf-8'),
                    sha1).hexdigest()

def _digest_matches(digest, content, secret):

    if not secret or not digest or not content:
        return False

    if not digest.startswith("sha1="):
        return False

    digest = digest[5:] # digest from server
    
    return digest == psh_digest(content, secret)