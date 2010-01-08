from copy import deepcopy
from couchdb import ResourceConflict, ResourceNotFound
from eventlet.api import spawn, sleep
from eventlet.pool import Pool
from eventlet.proc import spawn as spawn_proc, waitall, killall, ProcExit
from giblets import Component, implements
import logging
import traceback

from melkman.aggregator.api import *
from melkman.db.bucket import NewsBucket, NewsItemRef
from melkman.db.composite import Composite, view_composites_by_subscription
from melkman.db.remotefeed import RemoteFeed
from melkman.db.util import batched_view_iter
from melkman.fetch.api import request_feed_index
from melkman.messaging import MessageDispatch, always_ack, pooled
from melkman.worker import IWorkerProcess

log = logging.getLogger(__name__)

def _handle_bucket_modified(message_data, message, context):
    """
    main aggregator handler for the 'bucket_modified' message
    """

    # the 'bucket' was a Composite, and there are there are new
    # subscriptions to update.
    if ('Composite' in message_data.get('bucket_types', []) and 
        len(message_data.get('new_subscriptions', [])) > 0):
        _handle_new_subscriptions(message_data, message, context)

    # there are new items that have been put into the bucket, 
    # notify anyone who is subscribed to this bucket.
    if len(message_data.get('updated_items', [])) > 0:
        _notify_subscribers(message_data, message, context)

def _handle_new_subscriptions(message_data, message, context):
    """
    helper handler called when new subscriptions are added to 
    a composite.
    """
    try:
        new_subscriptions = message_data.get('new_subscriptions', [])
        if len(new_subscriptions) == 0:
            log.warn("Ignoring init_subscription with no new subscriptions ...")
            return

        cid = message_data.get('bucket_id', None)
        if cid is None:
            log.error("Ignoring init_subscription with no bucket_id: %s" % message_data)
            return

        composite = Composite.get(cid, context)
        if composite is None or not 'Composite' in composite.document_types:
            log.error("Ignoring subscription update for non-existent composite %s" % cid)
            return
        
        new_feeds = []
        updates = 0
        for sub in new_subscriptions:
            if not sub in composite.subscriptions:
                log.warn("ignoring subscription %s -> %s, not in composite" % (sub, cid))
                continue
            
            bucket = NewsBucket.get(sub, context)
            if bucket is None:
                log.warn("Ignoring init subscription to unknown object (%s)" % composite.subscriptions[sub])
                continue

            #  try 'casting' to a RemoteFeed
            if 'RemoteFeed' in bucket.document_types:
                rf = RemoteFeed.from_doc(bucket.unwrap(), context)
                # mark as needing immediate fetch if 
                # there is no history for this feed. 
                if len(rf.update_history) == 0:
                    new_feeds.append(rf.url)
                    continue

            try:
                log.debug("init subscription %s -> %s" % (sub, cid))
                updates += composite.init_subscription(sub)
                sleep(0) # yeild control
            except:
                log.error("Error initializing subscription %s -> %s: %s" % (sub, cid, traceback.format_exc()))
        if updates > 0:
            try:
                composite.save()
            except ResourceConflict: 
                # not a big deal in this case. This basically means
                # our timestamp did not become the latest -- we 
                # have made no alterations other than adding items.
                # Our additions succeed/fail independently of this as they
                # are separate documents.
                pass

        # request that we start indexing anything new...
        for url in new_feeds:
            request_feed_index(url, context)
    except:
        log.error("Error handling init_subscrition %s: %s" % (message_data, traceback.format_exc()))
        raise

def _notify_subscribers(message_data, message, context):
    """
    helper handler called to notify subscribers to a bucket
    of an update.
    """
    try:
        bucket_id = message_data.get('bucket_id', None)
        if bucket_id is None:
            log.warn("Ignorning malformed bucket_modified message... (no bucket_id)")
            message.ack()
            return

        out_message = deepcopy(message_data)
        out_message['command'] = 'update_subscription'

        publisher = MessageDispatch(context)
        # iterate subscribed composites.
        query = {
            'startkey': bucket_id,
            'endkey': bucket_id,
            'include_docs': False
        }
        # send a message for each subscribed composite that indicates the
        # need to update from the changed bucket.
        for r in batched_view_iter(context.db, view_composites_by_subscription, 100, **query):
            log.debug("notify %s of update to %s" % (r.id, out_message['bucket_id']))
            out_message['composite_id'] = r.id
            publisher.send(out_message, UPDATE_SUBSCRIPTION)
    except:
        log.error("Error dispatching composite updates: %s" % traceback.format_exc())
        raise


def _handle_update_subscription(message_data, message, context):
    """
    main aggregator handler for the 'update_subscription' message
    """
    try:
        updated_items = message_data.get('updated_items', [])
        if len(updated_items) == 0:
            log.debug('Ignoring subscription update with no updated items...')
            return

        cid = message_data.get('composite_id', None)
        if cid is None:
            log.debug('Ignoring subscription update with no composite id...')
            return
        
        bid = message_data.get('bucket_id', None)
        if bid is None:
            log.debug('Ignoring subscription update to %s with no bucket id...' % cid)
            return

        composite = Composite.get(cid, context)
        if composite is None or not 'Composite' in composite.document_types:
            log.error("Ignoring subscription update for non-existent composite %s" % cid)                
            return

        # check source 
        if not bid in composite.subscriptions:
            log.warn('Ignoring subscription update to %s for non-subscribed bucket %s' % (cid, bid))
        
        log.debug("updating %s (from bucket %s)" % (cid, bid))
        updated_refs = []
        for item in updated_items:
            ref = dict([(str(k), v) for k, v in item.items()])
            updated_refs.append(NewsItemRef.from_doc(ref, context))
        count = composite.filtered_update(updated_refs)
        if count > 0:
            try:
                composite.save()
            except ResourceConflict:
                # not a big deal in this case. This basically means
                # our timestamp did not become the latest -- we 
                # have made no alterations other than adding items.
                # Our additions succeed/fail independently of this as they
                # are separate documents.
                pass
    except:
        log.error("Error updating composite subscription %s: %s" % 
                  (message_data, traceback.format_exc()))
        raise


def run_aggregator(context):
    dispatcher = MessageDispatch(context)
    
    procs = []
    worker_pool = Pool(max_size=10000)

    @pooled(worker_pool)
    @always_ack
    def bucket_modified_handler(message_data, message):
        try:
            _handle_bucket_modified(message_data, message, context)
        finally:
            context.close()
        
    @pooled(worker_pool)
    @always_ack
    def update_subscription_handler(message_data, message):
        try:
            _handle_update_subscription(message_data, message, context)
        finally:
            context.close()
    
    procs.append(dispatcher.start_worker(BUCKET_MODIFIED, bucket_modified_handler))
    procs.append(dispatcher.start_worker(UPDATE_SUBSCRIPTION, update_subscription_handler))
    
    try:
        waitall(procs)
    except ProcExit:
        killall(procs)
        raise

class AggregatorProcess(Component):
    implements(IWorkerProcess)

    def run(self, context):
        run_aggregator(context)