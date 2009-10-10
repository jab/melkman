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

from melkman.db.util import DocumentHelper, Schema, MappingField, DibjectField
from melkman.aggregator.api import notify_bucket_modified

__all__ = ['NewsItem', 'NewsBucket', 'view_entries_by_timestamp']

log = logging.getLogger(__name__)

class NewsItem(DocumentHelper):

    document_types = ListField(TextField(), default=['NewsItem'])

    @property
    def item_id(self):
        return self.id

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
    item_id = TextField()
    timestamp = DateTimeField(default=datetime.utcnow())
    add_time = DateTimeField(default=datetime.utcnow())
    title = TextField()
    author = TextField()
    link = TextField()
    source_title = TextField()
    source_url = TextField()
    summary = TextField()
    
    
    @classmethod
    def from_doc(cls, doc, context):
        return cls.wrap(doc)

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
            item = NewsItemRef(item_id=item)
        elif not isinstance(item, NewsItemRef):
            item = NewsItemRef(**item)

        # if this item has already been added,
        # only consider it an update if the 
        # timestamp is strictly greater than the 
        # timestamp we already have for it.
        if item.item_id in self.entries:
            current_item = self.entries[item.item_id]
            if item.timestamp is None or item.timestamp <= current_item.timestamp:
                return False

        self.entries[item.item_id] = item
        self._added_item(self.entries[item.item_id])
        return True

    def remove_news_item(self, item):
        try:
            if isinstance(item, basestring):
                self._removed_item(self.entries[item])
                del self.entries[item]
                return True
            elif hasattr(item_id, 'item_id'):
                self._removed_item(self.entries[item.item_id])
                del self.entries[item.item_id]
                return True
            else:
                return False
        except KeyError:
            return False

    def has_news_item(self, item):
        if isinstance(item, basestring):
            return item in self.entries
        elif hasattr(item, 'item_id'):
            return item.item_id in self.entries
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

    def save(self):
        self.last_modification_date = datetime.utcnow()
        DocumentHelper.save(self)
        self._send_modified_event(updated_items=self._added.values(),
                                 removed_items=self._removed.values())
        self._added = {}
        self._removed = {}


    def _send_modified_event(self, **kw):
        notify_bucket_modified(self, self._context, **kw)

    def _added_item(self, item):
        try:
            del self._removed[item.item_id]
        except KeyError:
            pass
        self._added[item.item_id] = item

    def _removed_item(self, item):
        try:
            del self._added[item.item_id]
            return
        except KeyError:
            pass
        self._removed[item.item_id] = item


#####################################################################
# this is a view that indexes the entries in a bucket by timestamp
#####################################################################

view_entries_by_timestamp = ViewDefinition('bucket_indices', 'entries_by_timestamp', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("NewsBucket") != -1) {
        for (item_id in doc.entries) {
            var item = doc.entries[item_id];
            emit([doc._id, item.timestamp, item.item_id], item);
        }
    }
}
''')

view_entries_by_add_time = ViewDefinition('bucket_indices', 'entries_by_add_time', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("NewsBucket") != -1) {
        for (item_id in doc.entries) {
            var item = doc.entries[item_id];
            emit([doc._id, item.add_time, item.item_id], item);
        }
    }
}
''')


def bootstrap(db):
    view_entries_by_timestamp.sync(db)
    view_entries_by_add_time.sync(db)
