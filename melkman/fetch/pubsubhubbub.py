import logging
from giblets import Component, implements
from eventlet.api import tcp_listener
from eventlet.wsgi import server as wsgi_server
from httplib2 import Http
import traceback
from urllib import quote_plus, urlencode
from webob import Request, Response

try:
    from hashlib import sha1 # python > 2.5
except ImportError:
    from sha import new as sha1 # python <= 2.5


from melkman.db import RemoteFeed
from melkman.fetch.api import push_feed_index
from melkman.fetch.api import IndexRequestFilter
from melkman.fetch.api import PostIndexAction
from melk.util.nonce import nonce_str

log = logging.getLogger(__name__)

def callback_url_for(feed_url, context):
    base_url = context.config.pubsubhubbub_client.callback_url
    melk_id = RemoteFeed.id_for_url(feed_url)
    return '%s?url=%s' % (base_url, quote_plus(feed_url))

def topic_url_for(feed):
    for link in feed.feed_info.get('links', []):
        if link.rel == 'self':
            return link.href
    return None

def hubbub_sub(feed, context, hub_url=None):
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
    feed.save(context)

    cb = callback_url_for(feed.url, context)
    req = {
        'hub.callback': cb,
        'hub.mode': 'subscribe',
        'hub.topic': topic_url,
        'hub.verify': 'sync',
        'hub.verify': 'async',
        'hub.verify_token': feed.hub_info.verify_token,
        'hub.secret': feed.hub_info.secret
    }
    body = urlencode(req)
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    return Http().request(feed.hub_info.hub_url, method="POST", body=body, headers=headers)
        
def hubbub_unsub(feed, context):

    topic_url = topic_url_for(feed)
    
    if topic_url is None:
        raise ValueError('No self link found in feed, cannot unsubscribe via pubsubhubub.')

    if feed.hub_info.enabled == True:
        feed.hub_info.enabled = False
        feed.save(context)

    cb = callback_url_for(feed.url, context)
    req = {
        'hub.callback': cb,
        'hub.mode': 'unsubscribe',
        'hub.topic': topic_url,
        'hub.verify': 'sync',
        'hub.verify': 'async',
        'hub.verify_token': feed.hub_info.verify_token
    }
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
        host = self.context.config.pubsubhubbub_client.host
        port = int(self.context.config.pubsubhubbub_client.port)
        log.info("WSGISubClient starting on %s:%d" % (host, port))
        wsgi_server(tcp_listener((host, port)), self)

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

        if self._is_valid_sub_request(req):
            log.info("Got valid '%s' req for '%s'" % (mode, topic))
            res.status = 200
            res.body = req.GET['hub.challenge']
            return res
        else:
            log.warn("Got invalid '%s' req for '%s'" % (mode, topic))
            res.status = 404
        return res

    def _is_valid_sub_request(self, req):
        mode = req.GET.get('hub.mode', None)
        topic = req.GET.get('hub.topic', None)
        verify_token = req.GET.get('hub.verify_token', None)
        url = req.GET.get('url', None)

        if topic is None or mode is None or verify_token is None or url is None:
            return False

        rf = RemoteFeed.lookup_by_url(self.context.db, url)
        if rf is None:
            # confirm unsubscribes for feeds
            # we don't have...
            return mode == 'unsubscribe'

        if topic != topic_url_for(rf):
            log.warn("hub sent mismatched feed / topic: (%s, %s)" % (topic, topic_url_for(rf)))
            return False

        if mode == 'subscribe':
            return (rf.hub_info.enabled and
                    rf.hub_info.verify_token == verify_token)

        elif mode == 'unsubscribe':
            return (not rf.hub_info.enabled and
                    rf.hub_info.verify_token == verify_token)

    def handle_callback(self, req):
        self._handle_callback(req)
        res = Response()
        res.status = 200
        return res

    def _handle_callback(self, req):
        url = req.GET.get('url', None)
        digest = req.headers.get('X-Hub-Signature', None)
        content = req.body
        push_feed_index(url, content, self.context, digest=digest, from_hub=True)  

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
        if not feed.hub_info.enabled:
            log.warn("Ignoring hub push for unsubscribed feed.")
            return False

        if 'digest' in request:
            if not _digest_matches(request['digest'], content, feed.hub_info.secret):
                log.warn("Rejecting content push: digest (%s) did not match!" % request['digest'])
                return False

        return True

def _digest_matches(digest, content, secret):

    if not secret or not digest or not content:
        return False

    if not digest.startswith("sha1="):
        return False

    digest = digest[5:]

    hasher = sha1()
    hasher.update(secret)
    hasher.update(content)

    return hasher.hexdigest() == digest
