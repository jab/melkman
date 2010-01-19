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
    ctx = fresh_context()
    
    # create a document with a 10 entry feed
    feed_url = 'http://example.org/%s' % random_id()
    content = random_atom_feed(feed_url, 10)
    
    ids = melk_ids_in(content, feed_url)
    assert len(ids) == 10

    feed = RemoteFeed.create_from_url(feed_url, ctx)

    # update a remote feed with the content.
    updates = feed.update_from_feed(content, method='test')
    feed.save()
    
    assert updates == 10
    # make sure all the items are in the feed
    for iid in ids:
        assert feed.has_news_item(iid)
    
    # update again with identical content,
    # should have no effect.
    updates = feed.update_from_feed(content, method='test')
    assert updates == 0
    
    feed.save()

    # reload...
    feed.reload()
    # update again with identical content,
    # should have no effect.
    updates = feed.update_from_feed(content, method='test')
    assert updates == 0


def test_get_or_immediate_create_by_url():
    """
    test that get_or_immediate_create_by_url retrieves existing feeds by url
    if they in fact exist, and creates them if they don't.
    """
    from datetime import datetime, timedelta
    from eventlet.api import sleep
    from melkman.db.remotefeed import RemoteFeed, get_or_immediate_create_by_url
    ctx = fresh_context()

    feed_url = 'http://example.org/1'
    # make sure it doesn't exist yet
    feed = RemoteFeed.get_by_url(feed_url, ctx)
    assert feed is None

    # this should result in its immediate creation
    creationdt = datetime.utcnow()
    feed = get_or_immediate_create_by_url(feed_url, ctx)
    assert feed is not None
    assert feed.last_modification_date - creationdt < timedelta(seconds=1)

    sleep(1)
    
    # this should retrieve the existing feed, not create a new one
    now = datetime.utcnow()
    samefeed = get_or_immediate_create_by_url(feed_url, ctx)
    assert samefeed.id == feed.id
    assert samefeed.last_modification_date == feed.last_modification_date
    assert now - samefeed.last_modification_date >= timedelta(seconds=1)


def test_update_feed_partial_repeat():
    """
    test that indexing the some of same content twice only 
    updates new things.
    """
    from melkman.db import RemoteFeed
    ctx = fresh_context()

    # create a document with a 10 entry feed
    feed_url = 'http://example.org/%s' % random_id()
    entries1 = dummy_atom_entries(10)
    entries2 = dummy_atom_entries(10)
    
    content = make_atom_feed(feed_url, entries1)

    # extract the ids from the document
    expect_ids = melk_ids_in(content, feed_url)
    assert len(expect_ids) == 10

    feed = RemoteFeed.create_from_url(feed_url, ctx)

    # update a remote feed with the content.
    updates = feed.update_from_feed(content, method='test')
    feed.save()

    # make sure all the items come back as new/updated
    assert updates == 10
    # make sure all the items are in the feed
    for iid in expect_ids:
        assert feed.has_news_item(iid)

    # add some additional entries
    content = make_atom_feed(feed_url, entries2 + entries1)
    expect_ids = set([x for x in melk_ids_in(content, feed_url) if not x in expect_ids])
    assert len(expect_ids) == 10
    
    updates = feed.update_from_feed(content, method='test')
    # this list should be ready to push to the db with no probs
    feed.save()
    assert updates == 10

    # make sure all the items are in the feed
    for iid in expect_ids:
        assert feed.has_news_item(iid)



def test_item_trace_update():
    from melkman.db import NewsItem, RemoteFeed
    ctx = fresh_context()
    
    def _check_item(item, info):
        for k, v in info.items():
            if k != 'id':
                assert getattr(item, k) == v, "Key %s: Expected %s, got %s" % (k, v, getattr(item, k))
    def check_item(f, iid, info):
        _check_item(f.entries[iid], info)
        _check_item(NewsItem.get(iid, ctx), info)

    feed_url = 'http://example.org/feed'
    feed = RemoteFeed.create_from_url(feed_url, ctx)
    
    atom_id = 'http://example.org/articles.php?id=1'
    time1 = no_micro(datetime.utcnow())
    info1 = {'id': atom_id,
             'title': 'Title1',
             'author': 'author1',
             'link': 'http://example.org/link1',
             'summary': 'summary text 1', 
             'timestamp': time1}


    feed_v1 = make_atom_feed(feed_url, [make_atom_entry(**info1)])
    feed.update_from_feed(feed_v1, method='test')
    feed.save()

    melk_id = melk_ids_in(feed_v1, feed_url)[0]

    check_item(feed, melk_id, info1)

    # change the info, but not the timestamp, should stay the same
    info2 = dict(info1)
    info2['title'] = 'Title 2'
    feed_v2 = make_atom_feed(feed_url, [make_atom_entry(**info2)])
    feed.update_from_feed(feed_v2, method='test')
    feed.save()
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
    feed.update_from_feed(feed_v3, method='test')
    feed.save()
    check_item(feed, melk_id, info3)

def test_view_bucket_entries_by_timestamp():
    from melkman.db import NewsBucket, NewsItemRef
    from melkman.db.bucket import view_entries_by_timestamp
    from random import shuffle
    
    ctx = fresh_context()
    
    bucket_id = 'test_bucket'
    bucket = NewsBucket.create(ctx, bucket_id)

    first_date = datetime.today()
    items = [(random_id(), first_date - timedelta(days=i)) for i in range(100)]
    jumbled_items = list(items)
    shuffle(jumbled_items)

    for iid, timestamp in jumbled_items:
        bucket.add_news_item({'item_id': iid, 'timestamp': timestamp})

    bucket.save()
    
    # make sure they're all in there
    for iid, timestamp in items:
        assert bucket.has_news_item(iid)

    # insure they come out in the right order in the index
    query = {
        'startkey': [bucket_id, {}],
        'endkey': [bucket_id],
        'limit': 200,
        'descending': True, 
        'include_docs': True
    }
    sorted_items = [NewsItemRef.from_doc(r.doc, ctx) for r in 
                    view_entries_by_timestamp(ctx.db, **query)]

    assert len(sorted_items) == 100
    for i, item in enumerate(sorted_items):
        assert item.item_id == items[i][0]
        
def test_delete():
    from melkman.db import RemoteFeed, NewsItem, NewsItemRef
    ctx = fresh_context()

    feed_url = 'http://example.org/feeds/1'
    dummy_feed = random_atom_feed(feed_url, 25)
    items = melk_ids_in(dummy_feed, feed_url)

    rf = RemoteFeed.create_from_url(feed_url, ctx)
    rf.update_from_feed(dummy_feed, 'test')
    rf.save()
    
    bucket_id = rf.id
    ref_ids = []
    assert bucket_id in ctx.db
    for iid in items:
        assert iid in rf.entries
        ref_id = NewsItemRef.dbid(bucket_id, iid)
        ref_ids.append(ref_id)
        assert ref_id in ctx.db
        # a news item was created too...
        assert iid in ctx.db

        
    # now destroy!
    rf.delete()
    assert not bucket_id in ctx.db
    for ref_id in ref_ids:
        assert not ref_id in ctx.db
    for iid in items:
        assert not iid in ctx.db

def test_max_history_len():
    from melkman.db.remotefeed import RemoteFeed, MAX_HISTORY
    ctx = fresh_context()

    feed_url = 'http://example.org/feeds/1'
    rf = RemoteFeed.create_from_url(feed_url, ctx)

    for i in range(5*MAX_HISTORY):
        reason = 'update %d' % i
        rf.record_update_info(reason=reason)
        if i < MAX_HISTORY:
            assert len(rf.update_history) == i + 1
        else:
            assert len(rf.update_history) == MAX_HISTORY
        assert rf.update_history[0].reason == reason
