from copy import deepcopy
from couchdb import ResourceNotFound
from eventlet.api import spawn, sleep
import logging
import traceback

from melkman.db.bucket import NewsBucket, NewsItemRef
from melkman.db.composite import Composite, view_composites_by_subscription
from melkman.db.remotefeed import RemoteFeed
from melkman.db.util import batched_view_iter
from melkman.fetch.api import request_feed_index
from melkman.aggregator.api import *

log = logging.getLogger(__name__)

class CompositeUpdateDispatcher(CompositeDispatchConsumer):
    """
    SubscriptionDispatchConsumer translates
    bucket_modified messages into subscription 
    update messages on subscribed composites.
    """
    
    def __init__(self, context):
        BucketModifiedConsumer.__init__(self, context.broker)
        self.context = context

    def receive(self, message_data, message):
        spawn(self.handle_message, message_data, message)

    def handle_message(self, message_data, message):
        try:
            try:
                if ('Composite' in message_data.get('bucket_types', []) and 
                    len(message_data.get('new_subscriptions', [])) > 0):
                    self.send_init_subscription(message_data, message)
            except:
                pass
            
            try:
                if len(message_data.get('updated_items', [])) > 0:
                    self.notify_subscribers(message_data, message)
            except: 
                pass    
        finally:
            message.ack()
            self.context.close()

    def send_init_subscription(self, message_data, message):
        try:
            bucket_id = message_data.get('bucket_id', None)
            if bucket_id is None:
                log.warn("Ignorning malformed bucket_modified message... (no bucket_id)")
                message.ack()
                return
        
            out_message = deepcopy(message_data)
            out_message['command'] = 'init_subscription'
            publisher = SubscriptionUpdatePublisher(self.context.broker)
            publisher.send(out_message)
        except:
            log.error("Error dispatching composite subscription init: %s" % traceback.format_exc())
            raise

    def notify_subscribers(self, message_data, message):
        try:
            bucket_id = message_data.get('bucket_id', None)
            if bucket_id is None:
                log.warn("Ignorning malformed bucket_modified message... (no bucket_id)")
                message.ack()
                return

            out_message = deepcopy(message_data)
            out_message['command'] = 'update_subscription'
            publisher = SubscriptionUpdatePublisher(self.context.broker)
            # iterate subscribed composites.
            query = {
                'startkey': bucket_id,
                'endkey': bucket_id,
                'include_docs': False
            }
            # send a message 'to' each subscribed composite.
            for r in batched_view_iter(self.context.db, view_composites_by_subscription, 100, **query):
                log.debug("notify %s of update to %s" % (r.id, out_message['bucket_id']))
                out_message['composite_id'] = r.id
                publisher.send(out_message)
        except:
            log.error("Error dispatching composite updates: %s" % traceback.format_exc())
            raise


class CompositeUpdater(SubscriptionUpdateConsumer):
    
    def __init__(self, context):
        SubscriptionUpdateConsumer.__init__(self, context.broker)
        self.context = context
        self._handlers = {}
        self._handlers['update_subscription'] = self.handle_sub_update
        self._handlers['init_subscription'] = self.handle_init_subscription

    def receive(self, message_data, message):
        command = message_data.get('command', None)
        handler = self._handlers.get(command, None)
        if handler is not None:
            spawn(handler, message_data, message)
        else:
            log.warn("ignoring unknown message: %s" % message)
            message.ack()

    def handle_sub_update(self, message_data, message):
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

            composite = Composite.get(cid, self.context)
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
                updated_refs.append(NewsItemRef.from_doc(ref, self.context))
            count = composite.filtered_update(updated_refs)
            if count > 0:
                composite.save()
        except:
            log.error("Error updating composite subscription %s: %s" % 
                      (message_data, traceback.format_exc()))
            # XXX should signal failure to process, back off and not 
            # ack.. or reschedule with defer queue... (!!)
            # this could be a temporary failure like database downtime 
            # or a conflict, in which case, we should just try later...
            raise
        finally:
            message.ack()
            self.context.close()
            
    def handle_init_subscription(self, message_data, message):
        try:
            new_subscriptions = message_data.get('new_subscriptions', [])
            if len(new_subscriptions) == 0:
                log.warn("Ignoring init_subscription with no new subscriptions ...")
                return

            cid = message_data.get('bucket_id', None)
            if cid is None:
                log.error("Ignoring init_subscription with no bucket_id: %s" % message_data)
            composite = Composite.get(cid, self.context)
            if composite is None or not 'Composite' in composite.document_types:
                log.error("Ignoring subscription update for non-existent composite %s" % cid)
                return
            
            new_feeds = []
            updates = 0
            for sub in new_subscriptions:
                if not sub in composite.subscriptions:
                    log.warn("ignoring subscription %s -> %s, not in composite" % (sub, cid))
                    continue
                
                bucket = NewsBucket.get(sub, self.context)
                if bucket is None:
                    log.warn("Ingoring init subscription to unknown object (%s)" % composite.subscriptions[sub])
                    continue
    
                #  try 'casting' to a RemoteFeed
                if 'RemoteFeed' in bucket.document_types:
                    rf = RemoteFeed.from_doc(bucket.unwrap(), self.context)
                    # mark as needing immediate fetch if 
                    # there is no history for this feed. 
                    if len(rf.update_history) == 0:
                        new_feeds.append(rf.url)
                        continue
    
                try:
                    log.debug("init subscription %s -> %s" % (sub, cid))
                    updates += composite.init_subscription(sub)
                    sleep(0)
                except:
                    log.error("Error initializing subscription %s -> %s: %s" % (sub, cid, traceback.format_exc()))
            if updates > 0:
                composite.save()
                
            # request that we start indexing anything new...
            for url in new_feeds:
                request_feed_index(url, self.context)
        except:
            # XXX handle the non-ack...
            log.error("Error handling init_subscrition %s: %s" % (message_data, traceback.format_exc()))
            raise
        finally:
            message.ack()
            self.context.close()