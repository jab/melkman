from couchdb.design import ViewDefinition
from couchdb.schema import *
from datetime import datetime, timedelta
import logging

from melkman.aggregator.api import notify_bucket_modified
from melkman.db.bucket import NewsBucket, NewsItemRef, view_bucket_entries_by_timestamp
from melkman.db.util import DocumentHelper, DibjectField, MappingField


log = logging.getLogger(__name__)

REJECT_ITEM = 'reject'
ACCEPT_ITEM = 'accept'

class FilterConfig(Schema):
    op = TextField()
    negative = BooleanField(default=False)
    config = DibjectField()
    action = TextField()
    title = TextField()

class Subscription(Schema):
    bucket_id = TextField()
    title = TextField()
    url = TextField()

class Composite(NewsBucket):

    document_types = ListField(TextField(), default=['NewsBucket', 'Composite'])
    
    subscriptions = MappingField(DictField(Subscription))
    filters = ListField(DictField(FilterConfig))
    
    rejected_ref = TextField(default=None)

    def __init__(self, *args, **kw):
        NewsBucket.__init__(self, *args, **kw)
        self._rejected = None
        self._added_subs = set()
        self._removed_subs = set()

    def subscribe(self, bucket, title=None, url=None):
        sub = Subscription()
        if isinstance(bucket, basestring):        
            sub.bucket_id = bucket
            sub.title = title or bucket
            sub.url = url or ''
        else:
            sub.bucket_id = bucket.id
            sub.title = title or bucket.title
            sub.url = url or bucket.url or ''
        
        if not sub.bucket_id in self.subscriptions:
            self._added_sub(sub.bucket_id)
            
        self.subscriptions[sub.bucket_id] = sub

    def unsubscribe(self, bucket):
        if not isinstance(bucket, basestring):
            bucket = bucket.id
        try:
            del self.subscriptions[bucket]
            self._removed_sub(bucket)
        except KeyError:
            pass

    def init_subscription(self, bucket_id, context):
        sub_info = self.subscriptions.get(bucket_id, None)
        
        if sub_info is None:
            return 0 # not subscribed.
        
        stop_date = datetime.utcnow() - timedelta(days=1)
        query = {
            'startkey': [bucket_id, {}],
            'endkey': [bucket_id, DateTimeField()._to_json(stop_date)],
            'limit': 50,
            'descending': True
        }
        initial_items = [NewsItemRef.wrap(r.value) for r in 
                         view_bucket_entries_by_timestamp(context.db, **query)]

        if len(initial_items) > 0:
            return self.filtered_update(initial_items, context)
        else:
            return 0

    def filtered_update(self, news_items, context):
        return _filtered_update(self, news_items, context)

    def get_rejected(self, db):
        if self._rejected is not None and self._rejected.id == self.rejected_ref:
            return self._rejected
        elif self.rejected_ref:
            self._rejected = NewsBucket.load(db, self.rejected_ref)
            return self._rejected
        else:
            return None

    def save(self, context):
        # XXX not transactional
        NewsBucket.save(self, context)
        if self._rejected is not None:
            self._rejected.save(self, context)

    def _send_modified(self, context):
        notify_bucket_modified(self, context,
                               updated_items=self._added.values(),
                               removed_items=self._removed.values(),
                               new_subscriptions=list(self._added_subs),
                               removed_subscriptions=list(self._removed_subs))
        self._added = {}
        self._removed = {}
        self._added_subs = set()
        self._removed_subs = set()
    

    def _added_sub(self, bucket_id):
        try:
            self._removed_subs.remove(bucket_id)
        except KeyError:
            pass
            
        self._added_subs.add(bucket_id)

    def _removed_sub(self, bucket_id):
        try:
            self._added_subs.remove(bucket_id)
        except KeyError:
            pass
            
        self._removed_subs.add(bucket_id)

view_composites_by_subscription = ViewDefinition('composite_indices', 'composites_by_subscription', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("Composite") != -1) {
        for (sub_id in doc.subscriptions) {
            emit(sub_id, null);
        }
    }
}
''')

view_composite_subscriptions_by_title = ViewDefinition('composite_indices', 'composite_subscriptions_by_title',
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("Composite") != -1) {
        for (sub_id in doc.subscriptions) {
            var sub = doc.subscriptions[sub_id];
            emit([doc._id, sub.title], sub);
        }
    }
}
''')

def bootstrap(db):
    view_composites_by_subscription.sync(db)
    view_composite_subscriptions_by_title.sync(db)


def _filtered_update(composite, news_items, ctx):
    from melkman.filters import NewsItemFilterFactory, ACCEPT_ITEM, REJECT_ITEM
    filter_factory = NewsItemFilterFactory(ctx.component_manager)
    filt = filter_factory.create_chain(composite.filters)

    accepts = []
    rejects = []
    for item in news_items:
        result = filt(item)
        if result == ACCEPT_ITEM:
            accepts.append(item)
        else:
            if result != REJECT_ITEM:
                log.warn('Unsupported filter action: %s -- rejecting' % result)
            rejects.append(item)

    updated_items = 0
    for item in accepts:
        if composite.add_news_item(item):
            updated_items += 1

    if len(rejects) > 0:
        reject_bucket = composite.get_rejected(ctx.db)
        if reject_bucket is not None:
            for item in rejects:
                reject_bucket.add_news_item(item)

    log.info("filtered update to %s accepted %d (%d new), rejected %d" % (composite.id, len(accepts), updated_items, len(rejects)))
    return updated_items