from carrot.messaging import Consumer, Publisher
from giblets import Component, implements
import logging
from melkman.context import IRunDuringBootstrap

log = logging.getLogger(__name__)

__all__ = ['notify_bucket_modified', 'notify_subscription_update',
           'BucketModifiedPublisher', 'BucketModifiedConsumer',
           'SubscriptionUpdatePublisher', 'SubscriptionUpdateConsumer',
           'CompositeDispatchConsumer']

BUCKET_MODIFIED_EXCHANGE = 'melkman.direct'
BUCKET_MODIFIED_KEY = 'bucket_modified'
class BucketModifiedPublisher(Publisher):
    exchange = BUCKET_MODIFIED_EXCHANGE
    routing_key = BUCKET_MODIFIED_KEY
    delivery_mode = 2
    mandatory = True

class BucketModifiedConsumer(Consumer):
    exchange = BUCKET_MODIFIED_EXCHANGE
    routing_key = BUCKET_MODIFIED_KEY

COMPOSITE_DISPATCH_QUEUE = 'composite_dispatch'
class CompositeDispatchConsumer(BucketModifiedConsumer):
    queue = COMPOSITE_DISPATCH_QUEUE

SUBSCRIPTION_UPDATE_EXCHANGE = 'melkman.direct'
SUBSCRIPTION_UPDATE_KEY = 'update_subscription'
SUBSCRIPTION_UPDATE_QUEUE = 'update_subscription'
class SubscriptionUpdatePublisher(Publisher):
    exchange = SUBSCRIPTION_UPDATE_EXCHANGE
    routing_key = SUBSCRIPTION_UPDATE_KEY
    delivery_mode = 2
    mandatory = True

class SubscriptionUpdateConsumer(Consumer):
    exchange = SUBSCRIPTION_UPDATE_EXCHANGE
    routing_key = SUBSCRIPTION_UPDATE_KEY
    queue = SUBSCRIPTION_UPDATE_QUEUE
    durable = True
    
def notify_bucket_modified(bucket, context, **kw):
    message = {
        'bucket_id': bucket.id,
        'bucket_types': bucket.document_types,
    }
    message.update(kw)

    publisher = BucketModifiedPublisher(context.broker)
    publisher.send(message)

def notify_subscription_update(composite, bucket, context, **kw):
    updated_items = kw.get('updated_items', [])
    removed_items = kw.get('removed_items', [])

    message = {
        'command': 'update_subscription',
        'composite_id': composite.id,
        'bucket_id': bucket.id,
        'bucket_types': bucket.document_types,
    }
    message.update(kw)
    
    publisher = SubscriptionUpdatePublisher(context.broker)
    publisher.send(message)
    
class AggregatorSetup(Component):
    implements(IRunDuringBootstrap)

    def bootstrap(self, context, purge=False):

        log.info("Setting up aggregator queues...")
        types = (CompositeDispatchConsumer, SubscriptionUpdateConsumer)
        for cls in types:
            c = cls(context.broker)
            c.close()
            context.broker.close()

        if purge == True:
            log.info("Clearing feed indexing queues...")
            cnx = context.broker
            backend = cnx.create_backend()
            for cls in types:
                backend.queue_purge(cls.queue)
        
        context.close()
