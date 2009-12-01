# Copyright (C) 2009 The Open Planning Project
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the
# Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor,
# Boston, MA  02110-1301
# USA

from couchdb import ResourceConflict
from couchdb.design import ViewDefinition
from couchdb.schema import *
from datetime import datetime
from functools import wraps
import logging
from melk.util.nldict import nldict
from melk.util.nonce import nonce_str
from melk.util.hash import melk_id

from melkman.db.util import DocumentHelper, MappingField, DibjectField
from melkman.aggregator.api import notify_bucket_modified

__all__ = ['NewsItem', 'NewsBucket',
           'immediate_add',
           'view_entries',
           'view_entries_by_timestamp',
           'view_entries_by_add_time']

log = logging.getLogger(__name__)


class NewsItem(DocumentHelper):

    document_types = ListField(TextField(), default=['NewsItem'])

    @property
    def item_id(self):
        return self.id

    timestamp = DateTimeField(default=datetime.utcnow)
    title = TextField()
    author = TextField()
    link = TextField()
    source_title = TextField()
    source_url = TextField()
    summary = TextField()

    details = DibjectField()

    def load_full_item(self):
        return self

_REPLICATE_FIELDS = ('item_id', 'timestamp', 'title', 'author', 'link', 
                     'source_title', 'source_url', 'summary')

class NewsItemRef(DocumentHelper):
    """
    A trimmed down version of a NewsItem held inside
    a container.
    """
    document_types = ListField(TextField(), default=['NewsItemRef'])

    item_id = TextField()
    bucket_id = TextField()
    timestamp = DateTimeField(default=datetime.utcnow)
    add_time = DateTimeField(default=datetime.utcnow)
    title = TextField()
    author = TextField()
    link = TextField()
    source_title = TextField()
    source_url = TextField()
    summary = TextField()
    
    def load_full_item(self):
        return NewsItem.lookup_by_id(self.item_id, self._context)

    def update_from(self, other_item):
        for field in _REPLICATE_FIELDS:
            val = getattr(other_item, field)
            setattr(self, field, val)

    @classmethod
    def create_from_info(cls, context, bucket_id, **kw):
        kwa = dict(kw)
        kwa['id'] = cls.dbid(bucket_id, kw['item_id'])
        kwa['bucket_id'] = bucket_id
        instance = cls.create(context, **kwa)
        return instance

    @classmethod
    def create_from_item(cls, context, bucket_id, item):
        kw = dict()
        kw['id'] = cls.dbid(bucket_id, item.item_id)
        kw['bucket_id'] = bucket_id

        instance = cls.create(context, **kw)
        
        for field in _REPLICATE_FIELDS:
            instance[field] = item[field]

        return instance

    @classmethod
    def dbid(cls, bucket_id, item_id):
        return '%s_%s' % (bucket_id, item_id)


class NewsBucket(DocumentHelper):

    document_types = ListField(TextField(), default=['NewsBucket'])

    title = TextField(default='')
    url = TextField(default='')

    last_modification_date = DateTimeField()
    creation_date = DateTimeField(default=datetime.utcnow)

    def __init__(self, *args, **kw):
        if len(args) == 0 and not 'id' in kw:
            log.warn("assigning random id to bucket...")
            args = [melk_id(nonce_str())]
        
        DocumentHelper.__init__(self, *args, **kw)
        self._entries = None # lazy load
        self._removed = {}
        self._updated = {}
        self._maxlen = kw.get('maxlen')
        self._sortkey = kw.get('sortkey')

    def __len__(self):
        return len(self.entries)

    def _maxlen_get(self):
        return self._maxlen

    def _maxlen_set(self, value):
        if self._maxlen == value:
            return
        self._maxlen = value
        if self._entries is not None:
            self._entries.maxlen = value

    maxlen = property(_maxlen_get, _maxlen_set)

    @classmethod
    def create(cls, context, *args, **kw):
        if len(args) == 0 and not 'id' in kw:
            log.warn("assigning random id to bucket...")
            args = [melk_id(nonce_str())]
        return super(NewsBucket, cls).create(context, *args, **kw)

    def _lazy_load_entries(method, force=False):
        @wraps(method, ('__name__', '__doc__'))
        def wrapper(self, *args, **kwds):
            if force or self._entries is None:
                self._entries = nldict(self._maxlen, self._sortkey)
                self._entries.observers.append(self)
                
                # saved in db
                if not (self.id is None or self.rev is None):
                    query = {
                        'startkey': self.id,
                        'endkey': self.id + '0',
                        'include_docs': True,
                    }
                    for r in view_entries(self._context.db, **query):
                        ref = NewsItemRef.from_doc(r.doc, self._context)
                        self._entries[ref.item_id] = ref

            return method(self, *args, **kwds)

        return wrapper

    @property
    @_lazy_load_entries
    def entries(self):
        return self._entries

    def mapping_set(self, map, key, val):
        assert map is self._entries
        self._updated_item(val)

    def mapping_deleted(self, map, key, val):
        assert map is self._entries
        self._removed_item(val)

    @_lazy_load_entries
    def add_news_item(self, item):
        if isinstance(item, basestring):
            item = NewsItemRef.create_from_info(self._context, self.id, item_id=item)
        elif isinstance(item, NewsItem) or isinstance(item, NewsItemRef):
            item = NewsItemRef.create_from_item(self._context, self.id, item)
        else:
            item = NewsItemRef.create_from_info(self._context, self.id, **item)

        # if this item has already been added,
        # only consider it an update if the 
        # timestamp is strictly greater than the 
        # timestamp we already have for it.
        if item.item_id in self._entries:
            current_item = self._entries[item.item_id]
            if item.timestamp is None or item.timestamp <= current_item.timestamp:
                return False
            current_item.update_from(item)
            self._updated_item(current_item)
            return True

        # if it's currently in the trash, we remove it from 
        # the trash and unconditionally update it
        if item.item_id in self._removed:
            current_item = self._removed[item.item_id]
            current_item.update_from(item)
            self._entries[item.item_id] = current_item
            return True

        # a new item
        self._entries[item.item_id] = item
        return True

    @_lazy_load_entries
    def remove_news_item(self, item):
        try:
            if isinstance(item, basestring):
                del self._entries[item]
                return True
            if hasattr(item, 'item_id'):
                del self._entries[item.item_id]
                return True
            return False
        except KeyError:
            return False

    @_lazy_load_entries
    def has_news_item(self, item):
        if isinstance(item, basestring):
            return item in self._entries
        if hasattr(item, 'item_id'):
            return item.item_id in self._entries
        return False

    @_lazy_load_entries
    def filter_entries(self, predicate):
        kill_keys = []
        for (k, v) in self._entries.iteritems():
            if not predicate(v):
                kill_keys.append(k)
        for k in kill_keys:
            del self._entries[k]
        return kill_keys

    @_lazy_load_entries
    def clear(self):
        self._entries.clear()

    def save(self):
        self.last_modification_date = datetime.utcnow()
        
        updates = [self]
        
        try_update = list(self._updated.values())
        try_delete = list(self._removed.values())

        updates += try_update
        
        for item in try_delete:
            updates.append({'_id': item.id, '_rev': item.rev, '_deleted': True})
        
        results = self._context.db.update(updates)

        (main_doc_saved, main_doc_id, main_doc_result) = results.pop(0)
        if main_doc_saved:
            self._data.update({"_id": main_doc_id, "_rev": main_doc_result})

        conflicts = False

        successful_updates = []
        for item in try_update:
            (ref_saved, ref_id, ref_rev) = results.pop(0)
            if ref_saved:
                item._data.update({"_id": ref_id, "_rev": ref_rev})
                successful_updates.append(item)
            else:
                conflicts = True

        successful_deletes = []
        for item in try_delete:
            (ref_saved, ref_id, ref_rev) = results.pop(0)
            if ref_saved:
                successful_deletes.append(item)
            else:
                conflicts = True
        
        kw = {}
        if len(successful_updates) > 0:
            kw['updated_items'] = [x.unwrap() for x in successful_updates]
        if len(successful_deletes) > 0:
            kw['removed_items'] = [x.unwrap() for x in successful_deletes]
        self._send_modified_event(**kw)
        self._updated = {}
        self._removed = {}

        if conflicts:
            # require reload
            self._entries = None

        if not main_doc_saved:
            raise main_doc_result


    def _send_modified_event(self, *args, **kw):
        notify_bucket_modified(self, self._context, **kw)

    def _updated_item(self, item):
        try:
            del self._removed[item.item_id]
        except KeyError:
            pass
        self._updated[item.item_id] = item

    def _removed_item(self, item):
        updated = self._updated.get(item.item_id, None)
        if updated is not None:
            del self._updated[item.item_id]
            # only keep previously saved items in the
            # removed list.
            if updated.rev is not None:
                self._removed[item.item_id] = item
        else:
            self._removed[item.item_id] = item
    
    def _clobber(self, item):
        if self._entries is not None:
            self._entries[item.item_id] = item

        try:
            del self._removed[item.item_id]
        except KeyError: 
            pass
            
        try:
            del self._updated[item.item_id]
        except KeyError:
            pass

    def delete(self):
        dels = [{'_id': self.id, '_rev': self.rev, '_deleted': True}]
        for e in self._entries.values():
            dels.append({'_id': e.id, '_rev': e.rev, '_deleted': True})
        self._context.db.update(dels)
    delete = _lazy_load_entries(delete, force=True)


def immediate_add(bucket, item, context, notify=True):
    """
    immediately commit the addition of an item to the 
    specified bucket to the database.
    
    if notify is False, it is the caller's responsibility to 
    issue notification of *successful* additions.
    
    note, this will clobber any unsaved changes in the 
    bucket for this item.
    """
    if isinstance(item, basestring):
        item = NewsItemRef.create_from_info(context, bucket.id, item_id=item)
    elif isinstance(item, NewsItem) or isinstance(item, NewsItemRef):
        item = NewsItemRef.create_from_item(context, bucket.id, item)
    else:
        item = NewsItemRef.create_from_info(context, bucket.id, **item)

    current_item = NewsItemRef.get(item.id, context)
    if current_item is None:
        current_item = item
    elif item.timestamp is None or item.timestamp <= current_item.timestamp:
        return False
    else:
        current_item.update_from(item)
    
    try:
        current_item.save()
        bucket._clobber(current_item)        
        
        if notify == True:
            notify_bucket_modified(bucket, context, updated_items=[current_item.unwrap()])

        return True
    except ResourceConflict:
        return False
    

#####################################################################
# this is a view that indexes the entries in a bucket by timestamp
#####################################################################

view_entries = ViewDefinition('bucket_indices', 'entries', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("NewsItemRef") != -1) {
        emit(doc.bucket_id, doc.item_id);
    }
}
''')

view_entries_by_timestamp = ViewDefinition('bucket_indices', 'entries_by_timestamp', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("NewsItemRef") != -1) {
        emit([doc.bucket_id, doc.timestamp], doc.item_id);
    }
}
''')

view_entries_by_add_time = ViewDefinition('bucket_indices', 'entries_by_add_time', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("NewsItemRef") != -1) {
        emit([doc.bucket_id, doc.add_time], doc.item_id);
    }
}
''')

def bootstrap(db):
    view_entries.sync(db)
    view_entries_by_timestamp.sync(db)
    view_entries_by_add_time.sync(db)
