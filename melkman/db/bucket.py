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

from couchdb.design import ViewDefinition
from couchdb.schema import *
from datetime import datetime
import logging

from melkman.db.util import DocumentHelper, MappingField, DibjectField
from melkman.aggregator.api import notify_bucket_modified

__all__ = ['NewsItem', 'NewsBucket', 'view_bucket_entries_by_timestamp']

log = logging.getLogger(__name__)

class NewsItem(DocumentHelper):

    document_types = ListField(TextField(), default=['NewsItem'])

    timestamp = DateTimeField(default=datetime.utcnow())
    title = TextField()
    author = TextField()
    link = TextField()
    source_title = TextField()
    source_url = TextField()
    summary = TextField()

    details = DibjectField()

    def load_full_item(self, db):
        return self

class NewsItemRef(Schema):
    """
    A trimmed down version of a NewsItem held inside
    a container.
    """
    id = TextField()
    timestamp = DateTimeField(default=datetime.utcnow())
    add_time = DateTimeField(default=datetime.utcnow())
    title = TextField()
    author = TextField()
    link = TextField()
    source_title = TextField()
    source_url = TextField()
    summary = TextField()
    
    def load_full_item(self, db):
        return NewsItem.load(db, self.id)

class NewsBucket(DocumentHelper):

    document_types = ListField(TextField(), default=['NewsBucket'])

    title = TextField(default='')
    url = TextField(default='')

    entries = MappingField(DictField(NewsItemRef))

    last_modification_date = DateTimeField()
    creation_date = DateTimeField(default=datetime.utcnow)

    def __init__(self, *args, **kw):
        DocumentHelper.__init__(self, *args, **kw)
        self._added = {}
        self._removed = {}

    def add_news_item(self, item):
        if isinstance(item, basestring):
            item = NewsItemRef(id=item)
        elif not isinstance(item, NewsItemRef):
            item = NewsItemRef(**item)

        # if this item has already been added,
        # only consider it an update if the 
        # timestamp is strictly greater than the 
        # timestamp we already have for it.
        if item.id in self.entries:
            current_item = self.entries[item.id]
            if item.timestamp is None or item.timestamp <= current_item.timestamp:
                return False

        self.entries[item.id] = item
        self._added_item(self.entries[item.id])
        return True

    def remove_news_item(self, item):
        try:
            if isinstance(item, basestring):
                self._removed_item(self.entries[item])
                del self.entries[item]
                return True
            elif hasattr(item_id, 'id'):
                self._removed_item(self.entries[item.id])
                del self.entries[item.id]
                return True
            else:
                return False
        except KeyError:
            return False

    def has_news_item(self, item):
        if isinstance(item, basestring):
            return item in self.entries
        elif hasattr(item, 'id'):
            return item.id in self.entries
        else:
            return False

    def filter_entries(self, predicate):
        kill_keys = []
        for (k, v) in self.entries.iteritems():
            if not predicate(v):
                kill_keys.append(k)
        for k in kill_keys:
            self._removed_item(self.entries[k])
            del self.entries[k]

        return kill_keys

    def clear(self):
        for item in self.entries.values():
            self._removed_item(item)
        self.entries = {}

    def store(self, db):
        raise NotImplementedError("Use bucket.save(context) instead.")

    def save(self, context):
        self.last_modification_date = datetime.utcnow()
        DocumentHelper.store(self, context.db)
        self._send_modified(context)

    def _send_modified(self, context):
        notify_bucket_modified(self, context,
                               updated_items=self._added.values(),
                               removed_items=self._removed.values())
        self._added = {}
        self._removed = {}

    def _added_item(self, item):
        try:
            del self._removed[item.id]
        except KeyError:
            pass
        self._added[item.id] = item

    def _removed_item(self, item):
        try:
            del self._added[item.id]
        except KeyError:
            pass
        self._removed[item.id] = item


#####################################################################
# this is a view that indexes the entries in a bucket by timestamp
#####################################################################

view_bucket_entries_by_timestamp = ViewDefinition('bucket_indices', 'entries_by_timestamp', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("NewsBucket") != -1) {
        for (item_id in doc.entries) {
            var item = doc.entries[item_id];
            emit([doc._id, item.timestamp, item.id], item);
        }
    }
}
''')

view_bucket_entries_by_add_time = ViewDefinition('bucket_indices', 'entries_by_add_time', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("NewsBucket") != -1) {
        for (item_id in doc.entries) {
            var item = doc.entries[item_id];
            emit([doc._id, item.add_time, item.id], item);
        }
    }
}
''')


def bootstrap(db):
    view_bucket_entries_by_timestamp.sync(db)
    view_bucket_entries_by_add_time.sync(db)

# def bucket_entries_by_timestamp_query(bucket_id, limit,
#                                     start_time=None, end_time=None,
#                                     descending=True):
#     query = dict(descending=descending, limit=limit)
#     if descending:
#         pass
#     else:
#         pass
#     
# def get_bucket_entries_by_timestamp(bucket_id, limit, start_time=None, end_time=None, descending=True):
#     """
#     helper for view_bucket_entries_by_timestamp. 
#     return NewsItemRefs
#     
#     :param limit: return at most this many entries
#     :param start_time: datetime, if specified, no entries before this date will be returned
#     :param end_time: datetime, if specified, no entries after this date will be returned
#     :param descending: if True, return newest entries first, else oldest first
#     """
#     query = bucket_entries_by_timestamp_query(bucket_id, limit,
#                                             start_time=start_time, end_time=end_time,
#                                             descending=descending)
#     return [NewsItemRef.wrap(r.value) for r in view_entries_by_timestamp(query)]
# 
# def iter_bucket_entries_by_timestamp(bucket_id, limit, start_time=None, end_time=None, descending=True):
#     pass

# views needed:
# lastest entries
# random entries ?