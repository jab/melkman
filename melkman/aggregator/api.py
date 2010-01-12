from giblets import Component, implements
import logging
from melkman.context import IRunDuringBootstrap
from melkman.messaging import MessageDispatch


log = logging.getLogger(__name__)

__all__ = ['BUCKET_MODIFIED', 'UPDATE_SUBSCRIPTION',
           'notify_bucket_modified', 'update_subscription', 
           'AggregatorSetup']

# message types
BUCKET_MODIFIED = 'melkman.bucket_modified'
UPDATE_SUBSCRIPTION = 'melkman.update_subscription'

def notify_bucket_modified(bucket, context, **kw):
    message = {
        'bucket_id': bucket.id,
        'bucket_types': bucket.document_types,
    }
    message.update(kw)
    MessageDispatch(context).send(message, BUCKET_MODIFIED)

def update_subscription(composite, bucket, context, **kw):
    updated_items = kw.get('updated_items', [])
    removed_items = kw.get('removed_items', [])

    message = {
        'command': 'update_subscription',
        'composite_id': composite.id,
        'bucket_id': bucket.id,
        'bucket_types': bucket.document_types,
    }
    message.update(kw)
    
    MessageDispatch(context).send(message, SUBSCRIPTION_UPDATE)

class AggregatorSetup(Component):
    implements(IRunDuringBootstrap)

    def bootstrap(self, context, purge=False):

        types = (BUCKET_MODIFIED, UPDATE_SUBSCRIPTION)
        
        log.info("Setting up aggregator queues...")
        dispatch = MessageDispatch(context)
        for t in types:
            dispatch.declare(t)
            if purge == True:
                dispatch.clear(t)
        
        context.close()