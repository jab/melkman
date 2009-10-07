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

from couchdb import ResourceNotFound, ResourceConflict
from couchdb.design import ViewDefinition
from couchdb.schema import *
from copy import deepcopy
from datetime import datetime, timedelta

from httplib2 import Http
import logging

from melk.util.dibject import dibjectify
from melk.util.hash import melk_id
from melk.util.urlnorm import canonical_url

from melkman.parse import parse_feed, item_trace, find_best_timestamp, InvalidFeedError
from melkman.db.bucket import NewsBucket, NewsItem, NewsItemRef
from melkman.db.util import DibjectField, MappingField

log = logging.getLogger(__name__)


class HistoryItem(Schema):
    """
    An entry in the history of updates for
    a RemoteFeed
    """
    timestamp = DateTimeField(default=datetime.utcnow)
    updates = IntegerField(default=0)
    success = BooleanField(default=True)
    reason = TextField(default='')
    method = TextField(default='')

class HubInfo(Schema):
    hub_url = TextField()
    verify_token = TextField(default='')
    secret = TextField(default='')
    enabled = BooleanField(default=False)


MAX_HISTORY = 10
class RemoteFeed(NewsBucket):
    """
    This class is a NewsBucket that represents a remote
    news source available via a feed.
    """

    document_types = ListField(TextField(), default=['NewsBucket', 'RemoteFeed'])

    @classmethod
    def create_from_url(self, url, **kw):
        feed_id = self.id_for_url(url)
        return RemoteFeed(feed_id, url=url, **kw)

    feed_info = DibjectField()

    # next time to poll feed
    next_poll_time = DateTimeField()
    poll_in_progress = BooleanField(default=False)
    poll_start_time = DateTimeField()

    # pubsubhubbub info
    hub_info = DictField(HubInfo)
    
    update_history = ListField(DictField(schema=HistoryItem))

    def record_update_info(self, **info):
        self.update_history.insert(0, HistoryItem(**info))
        while len(self.update_history) > MAX_HISTORY:
            self.update_history.pop()
            
    def update_from_feed(self, content, db, method):
        """
        updates this feed from the unparsed feed content given. 
        n.b. this does NOT save changes to the database immediately!

        returns a list of new/updated NewsItems that should also 
        be saved if these changes are committed.
        """
        return _update_feed(self, content, db, method)

    @classmethod
    def id_for_url(cls, url):
        nurl = canonical_url(url).lower()
        return melk_id(nurl)

    @classmethod
    def lookup_by_url(cls, db, url):
        fid = cls.id_for_url(url)
        return cls.load(db, fid)

    @classmethod
    def lookup_by_urls(cls, db, urls):
        return cls.lookup_by_ids(db, [cls.id_for_url(u) for u in urls])




view_remote_feeds_by_next_poll_time = ViewDefinition('remote_feed_indices', 'remote_feeds_by_next_poll_time', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("RemoteFeed") != -1) {
        if ('poll_in_progress' in doc && doc.poll_in_progress) {
            emit([true, doc.poll_start_time, doc._id], null);
        }
        else {
            emit([false, doc.next_poll_time, doc._id], null)
        }
    }
}
''')

def bootstrap(db):
    view_remote_feeds_by_next_poll_time.sync(db)


def _find_updates(db_feed, parsed_feed):
    """
    db_feed - RemoteFeed to check
    parsed_feed - current parse of the feed

    return a list of new/updated items items according 
    to the RemoteFeed given and the parse.
    """

    # locate any new or updated items
    updated_items = []
    for e in parsed_feed.entries:
        existing_item = db_feed.entries.get(e.melk_id)
        if existing_item is None:
            updated_items.append(e)
        else:
            # if there is a timestamp specified, we compare.
            # if none was specified, we assume it was not updated.
            item_timestamp = find_best_timestamp(e)
            if item_timestamp is not None and item_timestamp > existing_item.timestamp:
                updated_items.append(e)

    return updated_items


def _update_feed(feed, content, db, method):
    """
    feed - RemoteFeed instance
    updates feed and returns a list of new/updated NewsItems
    
    no changes are pushed to the database.
    """
        
    try:
        fp = parse_feed(content, feed.url)
    except:
        feed.record_update_info(success=False, updates=0,
                            reason='Feed could not be parsed',
                            method=method)
        import traceback
        log.error("unable to parse feed %s: %s" % (feed.url, traceback.format_exc()))
        return []

    updated_items = _find_updates(feed, fp)

    # update feed metadata
    feed.feed_info = deepcopy(fp.feed)
    feed.title = fp.feed.get('title', '')

    # add item to RemoteFeed, gather ids
    iids = []
    traces = {}
    for item in updated_items:
        trace = item_trace(item)
        traces[item.melk_id] = trace
        ref = {'id': item.melk_id}
        ref.update(trace)
        feed.add_news_item(ref)
    feed.record_update_info(success=True, updates=len(updated_items), method=method)

    # grab any existing entries (need revisions to push update)...
    save_items = {}
    for r in db.view('_all_docs', keys=traces.keys(), include_docs=True).rows:
        if 'doc' in r:
            save_items[r.key] = NewsItem.wrap(r.doc)

    for item in updated_items:
        iid = item.melk_id
        trace = traces[iid]
        
        news_item = save_items.get(iid, None)
        if news_item is None:
            # if it is a new entry, create it
            news_item = NewsItem(iid, **trace)
            save_items[item.melk_id] = news_item
        else:
            # otherwise, just update fields
            for k, v in trace.items():
                setattr(news_item, k, v)
        news_item.details = item

    return save_items.values()