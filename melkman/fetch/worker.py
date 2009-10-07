from datetime import datetime, timedelta
from eventlet.api import spawn
from httplib2 import Http
import logging
import traceback
try:
    from hashlib import sha1 # python > 2.5
except ImportError:
    from sha import new as sha1 # python <= 2.5


from melkman.db import RemoteFeed
from melkman.fetch.api import schedule_feed_index, FeedIndexerConsumer

__all__ = ['FeedIndexer', 'index_feed_polling']

log = logging.getLogger(__name__)

###############################
# pushing update
###############################
METHOD_PUSH = 'push'
def index_feed_push(url, content, context, **kw):
    feed = RemoteFeed.lookup_by_url(context.db, url)

    updated_docs = []
    if feed is None:
        feed = RemoteFeed.create_from_url(url)
    
    if 'digest' in kw:
        if not _digest_matches(kw['digest'], content, feed.hub_info.secret):
            log.warn("Rejecting content push: digest (%s) did not match!" % kw['digest'])
            return

    # 200 status code, not from cache, do update...
    updated_docs += feed.update_from_feed(content, context.db, method=METHOD_PUSH)
    updated_docs.append(feed)
    context.db.update(updated_docs)

    # now we need to signal that the bucket was updated 
    # like it would have if we called save instead of using 
    # a bulk load...
    feed._send_modified(context)

    log.info("Updated feed %s success: %s, %d new items" % 
      (feed.url, feed.update_history[0].success, feed.update_history[0].updates))

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

###############################
# polling update
###############################
METHOD_POLL = 'poll'
def index_feed_polling(url, context, http_cache=None, timeout=15, reschedule=False):
    """
    poll the feed at the url given and index it immediately on 
    the calling thread. 
    """
    # fetch
    http = Http(cache=http_cache, timeout=timeout)
    http.force_exception_to_status_code = True
    response, content = http.request(url, 'GET')

    updated_docs = []
    feed = RemoteFeed.lookup_by_url(context.db, url)
    if feed is None:
        feed = RemoteFeed.create_from_url(url)

    if response.fromcache:
        feed.record_update_info(success=True, updates=0, method=METHOD_POLL)
    elif response.status != 200:
        feed.record_update_info(success=False, updates=0, 
                           reason=response.reason, method=METHOD_POLL)
    else:
        # 200 status code, not from cache, do update...
        updated_docs += feed.update_from_feed(content, context.db, method=METHOD_POLL)

    # compute the next time to check...
    next_interval = compute_next_fetch_interval(feed.update_history)
    log.debug("next update interval for %s = %s" % (feed.url, next_interval))
    feed.next_poll_time = datetime.utcnow() + next_interval
    feed.poll_in_progress = False

    updated_docs.append(feed)
    context.db.update(updated_docs)

    # now we need to signal that the bucket was updated 
    # like it would have if we called save instead of using 
    # a bulk load...
    feed._send_modified(context)

    log.info("Updated feed %s success: %s, %d new items" % 
      (feed.url, feed.update_history[0].success, feed.update_history[0].updates))

    # whee... request at the next time !
    if reschedule:
        message_id = 'periodic_index_%s' % RemoteFeed.id_for_url(feed.url)
        schedule_feed_index(feed.url, feed.next_poll_time, context, message_id=message_id)

def compute_next_fetch_interval(update_history):
    # XXX plug-in ?
    return compute_next_fetch_interval_aimd(update_history)

INITIAL_FETCH_INTERVAL = timedelta(hours=1)
MIN_FETCH_INTERVAL = timedelta(minutes=15)
MAX_FETCH_INTERVAL = timedelta(days=1)
INCREASE_SUMMAND = timedelta(minutes=30)
DECREASE_DIVISOR = 2
TARGET_UPDATES_PER_FETCH = 1
def compute_next_fetch_interval_aimd(update_history):
    """
    Compute the time when this feed should next be fetched based
    on historical update data.
    """

    #
    # this performs Additive Increase / Multiplicative Decrease based on 
    # the last two updates only.
    #
    # if an update occurred in the last interval, the next interval is 
    # half as long as the last. If no update was found, the interval is 
    # increased by a fixed amount.
    #

    if len(update_history) < 2:
        return INITIAL_FETCH_INTERVAL

    h1 = update_history[0]
    h2 = update_history[1]

    last_interval = h1.timestamp - h2.timestamp

    if h1.success == False or h1.updates == 0:
        new_interval = last_interval + INCREASE_SUMMAND
    elif h1.updates > TARGET_UPDATES_PER_FETCH :
        new_interval = last_interval // DECREASE_DIVISOR
    else:
        new_interval = last_interval

    if new_interval > MAX_FETCH_INTERVAL:
        new_interval = MAX_FETCH_INTERVAL
    if new_interval < MIN_FETCH_INTERVAL:
        new_interval = MIN_FETCH_INTERVAL

    return new_interval

class FeedIndexer(FeedIndexerConsumer):
    """
    Implements an asynchronous feed indexer process.
    """

    def __init__(self, context):
        FeedIndexerConsumer.__init__(self, context.broker)
        self.context = context

    def receive(self, message_data, message):
        spawn(self.handle_message, message_data, message)

    def handle_message(self, message_data, message):
        try:
            url = message_data.get('url', None)
            if url is None:
                log.error("malformed index_feed message, no url: %s" % message)
                return
            
            if 'content' in message_data:
                self.handle_push(url, message_data, message)
            else:
                self.handle_poll(url, message_data, message)

            log.info('Completed index of %s' % url)
        finally:
            try:
                message.ack()
            except:
                log.error("Failed to acknowledge message: %s" % traceback.format_exc())
            log.debug("completed handling message.")

    def handle_poll(self, url, message_data, message):
        log.info('Recieved poll index request for %s' % url)
        reschedule = not message_data.get('skip_reschedule', False)
        http_cache = self.context.config.get('http', {}).get('cache', None)
        index_feed_polling(url, self.context, http_cache=http_cache, reschedule=reschedule)
    
    def handle_push(self, url, message_data, message):
        log.info('Recieved push index request for %s' % url)
        content = message_data['content']
        kw = {}
        if 'digest' in message_data:
            kw['digest'] = message_data['digest']
        index_feed_push(url, content, self.context, **kw)