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

from datetime import datetime, timedelta
import time

from helpers import *


def test_update_feed_repeat_index():
    """
    test that indexing the same content twice has no effect
    """
    from melkman.db import RemoteFeed
    from melkman.parse import parse_feed
    ctx = fresh_context()
    db = ctx.db
    
    # create a document with a 10 entry feed
    feed_url = 'http://example.org/%s' % random_id()
    content = random_atom_feed(feed_url, 10)
    
    ids = melk_ids_in(content, feed_url)
    assert len(ids) == 10

    feed = RemoteFeed.create_from_url(feed_url)

    # update a remote feed with the content.
    updated = feed.update_from_feed(content, db, method='test')
    # this list should be ready to push to the db with no probs
    db.update(updated)
    # make sure all the items come back as new/updated
    assert len(updated) == 10
    uids = [x.id for x in updated]
    assert len(uids) == 10
    for uid in uids:
        assert uid in ids
    # make sure all the items are in the feed
    for iid in ids:
        assert feed.has_news_item(iid)
    
    last_mod = feed.last_modification_date

    # update again with identical content,
    # should have no effect.
    updated = feed.update_from_feed(content, db, method='test')
    assert len(updated) == 0
    assert last_mod == feed.last_modification_date

def test_update_feed_partial_repeat():
    """
    test that indexing the some of same content twice only 
    updates new things.
    """
    from melkman.db import RemoteFeed
    from melkman.parse import parse_feed
    ctx = fresh_context()
    db = ctx.db

    # create a document with a 10 entry feed
    feed_url = 'http://example.org/%s' % random_id()
    entries1 = dummy_atom_entries(10)
    entries2 = dummy_atom_entries(10)
    
    content = make_atom_feed(feed_url, entries1)

    # extract the ids from the document
    expect_ids = melk_ids_in(content, feed_url)
    assert len(expect_ids) == 10

    feed = RemoteFeed.create_from_url(feed_url)

    # update a remote feed with the content.
    updated = feed.update_from_feed(content, db, method='test')
    # this list should be ready to push to the db with no probs
    db.update(updated)
    # make sure all the items come back as new/updated
    assert len(updated) == 10
    uids = [x.id for x in updated]

    assert len(uids) == 10
    for iid in uids:
        assert iid in expect_ids
    # make sure all the items are in the feed
    for iid in expect_ids:
        assert feed.has_news_item(iid)

    last_mod = feed.last_modification_date

    # add some additional entries
    content = make_atom_feed(feed_url, entries2 + entries1)
    expect_ids = set([x for x in melk_ids_in(content, feed_url) if not x in expect_ids])
    assert len(expect_ids) == 10
    
    updated = feed.update_from_feed(content, db, method='test')
    # this list should be ready to push to the db with no probs
    db.update(updated)
    assert len(updated) == 10
    uids = [x.id for x in updated]
    assert len(uids) == 10
    for iid in uids:
        assert iid in expect_ids
    # make sure all the items are in the feed
    for iid in expect_ids:
        assert feed.has_news_item(iid)

def test_item_trace_update():
    from melkman.db import NewsItem, RemoteFeed
    ctx = fresh_context()
    db = ctx.db
    
    def _check_item(item, info):
        for k, v in info.items():
            if k != 'id':
                assert getattr(item, k) == v, "Key %s: Expected %s, got %s" % (k, v, getattr(item, k))
    def check_item(f, iid, info):
        _check_item(f.entries[iid], info)
        _check_item(NewsItem.load(db, iid), info)

    feed_url = 'http://example.org/feed'
    feed = RemoteFeed.create_from_url(feed_url)
    
    atom_id = 'http://example.org/articles.php?id=1'
    time1 = no_micro(datetime.utcnow())
    info1 = {'id': atom_id,
             'title': 'Title1',
             'author': 'author1',
             'link': 'http://example.org/link1',
             'summary': 'summary text 1', 
             'timestamp': time1}


    feed_v1 = make_atom_feed(feed_url, [make_atom_entry(**info1)])
    updated = feed.update_from_feed(feed_v1, db, method='test')
    melk_id = updated[0].id
    db.update(updated)
    feed.save(ctx)
    check_item(feed, melk_id, info1)

    # change the info, but not the timestamp, should stay the same
    info2 = dict(info1)
    info2['title'] = 'Title 2'
    feed_v2 = make_atom_feed(feed_url, [make_atom_entry(**info2)])
    updated = feed.update_from_feed(feed_v2, db, method='test')
    db.update(updated)
    feed.save(ctx)
    # should still match info1. (no update)
    check_item(feed, melk_id, info1)

    # now update the timestamp along with other fields
    time3 = no_micro(time1 + timedelta(seconds=1))
    info3 = {'id': atom_id,
             'title': 'Title3',
             'author': 'author3',
             'link': 'http://example.org/link3',
             'summary': 'summary text 3',
             'timestamp': time3}
    feed_v3 = make_atom_feed(feed_url, [make_atom_entry(**info3)])
    updated = feed.update_from_feed(feed_v3, db, method='test')
    db.update(updated)
    feed.save(ctx)
    check_item(feed, melk_id, info3)

def test_view_bucket_entries_by_timestamp():
    from melkman.db import NewsBucket, NewsItemRef
    from melkman.db.bucket import view_bucket_entries_by_timestamp
    from random import shuffle
    
    ctx = fresh_context()
    db = ctx.db
    
    bucket_id = 'test_bucket'
    bucket = NewsBucket(bucket_id)

    first_date = datetime.today()
    items = [(random_id(), first_date - timedelta(days=i)) for i in range(100)]
    jumbled_items = list(items)
    shuffle(jumbled_items)

    for iid, timestamp in jumbled_items:
        bucket.add_news_item({'id': iid, 'timestamp': timestamp})

    bucket.save(ctx)
    
    # make sure they're all in there
    for iid, timestamp in items:
        assert bucket.has_news_item(iid)

    # insure they come out in the right order in the index
    query = {
        'startkey': [bucket_id, {}],
        'endkey': [bucket_id],
        'limit': 200,
        'descending': True
    }
    sorted_items = [NewsItemRef.wrap(r.value) for r in 
                    view_bucket_entries_by_timestamp(db, **query)]

    assert len(sorted_items) == 100
    for i, item in enumerate(sorted_items):
        assert item.id == items[i][0]

# missing:
# test history entries