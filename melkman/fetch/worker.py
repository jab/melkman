from datetime import datetime, timedelta
from giblets import Component, ExtensionPoint, implements
from eventlet import spawn
from eventlet.support.greenlets import GreenletExit
from httplib2 import Http
import logging
import traceback

from melkman.db import RemoteFeed
from melkman.fetch.api import INDEX_FEED_COMMAND
from melkman.fetch.api import schedule_feed_index
from melkman.fetch.api import PostIndexAction, IndexRequestFilter
from melkman.green import Pool
from melkman.messaging import MessageDispatch, always_ack, pooled
from melkman.worker import IWorkerProcess

__all__ = ['run_feed_indexer', 'index_feed_polling']

log = logging.getLogger(__name__)


class IndexerPlugins(Component):
    
    request_filters = ExtensionPoint(IndexRequestFilter)
    post_index = ExtensionPoint(PostIndexAction)
    
    def accepts_request(self, feed, request, context):
        for filt in self.request_filters:
            if not filt.accepts_request(feed, request, context):
                return False
        return True
    
    def feed_reindexed(self, feed, context):
        for action in self.post_index:
            try:
                action.feed_reindexed(feed, context)
            except:
                log.error("Error running post-update hook: %s" % traceback.format_exc())

def run_post_index_hooks(feed, context):
    hooks = IndexerPlugins(context.component_manager)
    hooks.feed_reindexed(feed, context)

def check_request_approved(feed, request_info, context):
    hooks = IndexerPlugins(context.component_manager)
    return hooks.accepts_request(feed, request_info, context)
    
###############################
# pushing update
###############################
METHOD_PUSH = 'push'
def index_feed_push(url, content, context, request_info=None):
    if request_info is None:
        request_info = {}

    feed = RemoteFeed.get_by_url(url, context)

    updated_docs = []
    if feed is None:
        feed = RemoteFeed.create_from_url(url, context)

    if check_request_approved(feed, request_info, context) == False:
        log.warn("Rejected index request for %s" % url)
        return 


    # 200 status code, not from cache, do update...
    feed.update_from_feed(content, method=METHOD_PUSH)
    feed.save()

    log.info("Updated feed %s success: %s, %d new items" % 
      (feed.url, feed.update_history[0].success, feed.update_history[0].updates))

    run_post_index_hooks(feed, context)


###############################
# polling update
###############################
METHOD_POLL = 'poll'
def index_feed_polling(url, context, timeout=15, request_info=None):
    """
    poll the feed at the url given and index it immediately on 
    the calling thread. 
    """
    if request_info is None:
        request_info = {}

    feed = RemoteFeed.get_by_url(url, context)
    if feed is None:
        feed = RemoteFeed.create_from_url(url, context)

    if check_request_approved(feed, request_info, context) == False:
        log.warn("Rejected index request for %s" % url)
        return

    reschedule = not request_info.get('skip_reschedule', False)
    http_cache = context.config.get('http', {}).get('cache', None)

    # fetch
    http = Http(cache=http_cache, timeout=timeout)
    http.force_exception_to_status_code = True
    response, content = http.request(url, 'GET')

    updated_docs = []
    if response.fromcache:
        feed.record_update_info(success=True, updates=0, method=METHOD_POLL)
    elif response.status != 200:
        feed.record_update_info(success=False, updates=0, 
                           reason=response.reason, method=METHOD_POLL)
    else:
        # 200 status code, not from cache, do update...
        feed.update_from_feed(content, method=METHOD_POLL)

    # compute the next time to check...
    next_interval = compute_next_fetch_interval(feed.update_history)
    log.debug("next update interval for %s = %s" % (feed.url, next_interval))
    feed.next_poll_time = datetime.utcnow() + next_interval
    feed.poll_in_progress = False
    feed.save()

    log.info("Updated feed %s success: %s, %d new items" % 
      (feed.url, feed.update_history[0].success, feed.update_history[0].updates))

    # whee... request at the next time !
    if reschedule:
        message_id = 'periodic_index_%s' % RemoteFeed.id_for_url(feed.url)
        schedule_feed_index(feed.url, feed.next_poll_time, context, message_id=message_id)

    run_post_index_hooks(feed, context)


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


#####################
# Message handling
#####################s

def handle_message(message_data, message, context):
    """
    """
    try:
        url = message_data.get('url', None)
        if url is None:
            log.error("malformed index_feed message, no url: %s" % message)
            return
    
        if 'content' in message_data:
            _handle_push(url, message_data, message, context)
        else:
            _handle_poll(url, message_data, message, context)

        log.info('Completed index of %s' % url)
        log.debug("completed handling message.")
    except: 
        log.error('Error handling feed indexer command (%s): %s' % 
                  (message_data, traceback.format_exc()))

def _handle_poll(url, message_data, message, context):
    log.info('Received poll index request for %s' % url)
    try:
        index_feed_polling(url, context, request_info=message_data)
    except:
        log.error("Error indexing %s during poll request: %s" % (url, traceback.format_exc()))

def _handle_push(url, message_data, message, context):
    log.info('Received push index request for %s' % url)
    try:
        content = message_data['content']
        index_feed_push(url, content, context, request_info=message_data)
    except:
        log.error("Error pushing %s: %s" % (message_data, traceback.format_exc()))


def run_feed_indexer(context):
    try:
        worker_pool = Pool()
        dispatch = MessageDispatch(context)
    
        @pooled(worker_pool)
        @always_ack
        def cb(message_data, message):
            try:
                return handle_message(message_data, message, context)
            except GreenletExit:
                pass
            except: 
                log.error("Unexpected error handling feed indexer message: %s" % traceback.format_exc())
            finally:
                context.close()

        proc = dispatch.start_worker(INDEX_FEED_COMMAND, cb)
        proc.wait()
    except GreenletExit:
        pass
    except: 
        log.error("Unexpected error running feed indexer: %s" % traceback.format_exc())
    finally:
        # stop accepting work
        proc.kill()
        proc.wait()
        # stop working on existing work
        worker_pool.killall()
        worker_pool.waitall()

class FeedIndexerProcess(Component):
    implements(IWorkerProcess)
    
    def run(self, context):
        run_feed_indexer(context)