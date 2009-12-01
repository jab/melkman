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

def test_bucket_add_remove():
    """
    test adding and removing a single item from a bucket
    """
    from melkman.db import NewsBucket
    ctx = fresh_context()
    
    # create a bucket
    bucket_id = random_id()
    bucket = NewsBucket.create(ctx, bucket_id)
    item_id = random_id()

    # add a random item
    bucket.add_news_item(item_id)
    # make sure it is in the bucket before and after saving
    assert bucket.has_news_item(item_id)
    bucket.save()
    bucket = NewsBucket.get(bucket_id, ctx)
    
    assert bucket is not None
    assert bucket.has_news_item(item_id)
    
    # remove the item
    bucket.remove_news_item(item_id)
    # make sure it is not in the bucket before and after saving
    assert not bucket.has_news_item(item_id)
    bucket.save()
    bucket = NewsBucket.get(bucket_id, ctx)
    assert not bucket.has_news_item(item_id)
    
def test_bucket_silent_nodupes():
    """
    tests that calling add_news_item with the 
    same item twice only adds the item once.
    """
    from melkman.db import NewsBucket
    ctx = fresh_context()
    
    # create a bucket and add an item to it
    bucket_id = random_id()
    bucket = NewsBucket.create(ctx, bucket_id)
    item_id = random_id()
    bucket.add_news_item(item_id)
    assert bucket.has_news_item(item_id)
    
    # should be able to silently add it again
    bucket.add_news_item(item_id)
    # and it should still be in there
    assert bucket.has_news_item(item_id)
    
    # should be only one in there
    count = 0
    for iid, item in bucket.entries.iteritems():
        assert item.item_id == iid
        if iid == item_id:
            count += 1    
    assert count == 1
    
    # now get rid of it (once), it should be gone
    bucket.remove_news_item(item_id)
    assert not bucket.has_news_item(item_id)

def test_bucket_overwrite_item():
    """
    test semantics of updating an item
    """
    from melkman.db import NewsBucket, NewsItemRef
    ctx = fresh_context()
    item_id = random_id()
    
    info1 = {'item_id': item_id,
             'timestamp': datetime.utcnow(), 
             'title': 'the foo',
             'author': 'jimmy'}
    info2 = {'item_id': item_id, 
             'timestamp': datetime.utcnow() + timedelta(seconds=10), 
             'title': 'the bar'}
    
    def info_matches(item, info):
        # check that all specified info is present
        assert epeq_datetime(item.timestamp, info['timestamp'])

        for k in info:
            if k in ('timestamp'):
                continue
            if item[k] != info[k]:
                return False
        return True
    
    # create a bucket and add an item to it with 
    # extra info
    bucket_id = random_id()
    bucket = NewsBucket.create(ctx, bucket_id)

    assert bucket.add_news_item(info1) == True
    assert bucket.has_news_item(item_id)
    assert info_matches(bucket.entries[item_id], info1)
    bucket.save()
    bucket = NewsBucket.get(bucket_id, ctx)
    assert bucket.has_news_item(item_id)
    assert info_matches(bucket.entries[item_id], info1)
    
    # now add it again with different info
    assert bucket.add_news_item(info2) == True
    # and it should still be in there, but now with 
    # different info
    assert bucket.has_news_item(item_id)
    assert info_matches(bucket.entries[item_id], info2)
    bucket.save()
    bucket = NewsBucket.get(bucket_id, ctx)
    assert bucket.has_news_item(item_id)
    assert info_matches(bucket.entries[item_id], info2)

    # try adding the original info, it should be 
    # rejected because there is an item with a newer
    # timestamp.
    assert bucket.add_news_item(info1) == False
    assert bucket.has_news_item(item_id)
    assert info_matches(bucket.entries[item_id], info2)
    bucket.save()
    bucket = NewsBucket.get(bucket_id, ctx)
    assert bucket.has_news_item(item_id)
    assert info_matches(bucket.entries[item_id], info2)

    # should be only one in there once
    count = 0
    for iid, item in bucket.entries.iteritems():
        assert iid == item.item_id
        if iid == item_id:
            count += 1    
    assert count == 1
    
def test_bucket_save_twice():
    from melkman.db import NewsBucket
    ctx = fresh_context()

    bucket = NewsBucket.create(ctx, random_id())
    bucket.save()
    bucket.save()
    
def test_add_before_save_no_id():
    from melkman.db import NewsBucket
    ctx = fresh_context()

    bucket = NewsBucket.create(ctx)
    bucket.add_news_item('abc')
    bucket.save()
    bucket = NewsBucket.get(bucket.id, ctx)
    assert bucket.has_news_item('abc')

def test_add_from_ref():
    from melkman.db import NewsBucket, NewsItemRef
    ctx = fresh_context()

    bucket = NewsBucket.create(ctx)
    bucket.add_news_item('abc')
    bucket.save()
    bucket = NewsBucket.get(bucket.id, ctx)
    assert bucket.has_news_item('abc')
    
    bucket2 = NewsBucket.create(ctx)
    bucket2.add_news_item(bucket.entries['abc'])
    bucket2.save()
    bucket2 = NewsBucket.get(bucket2.id, ctx)
    assert bucket.has_news_item('abc')

def test_bucket_delete():
    from melkman.db import NewsBucket, NewsItemRef
    ctx = fresh_context()

    bucket = NewsBucket.create(ctx)
    items = [random_id() for i in range(1000)]
    for iid in items:
        bucket.add_news_item(iid)
    bucket.save()
    
    bucket_id = bucket.id
    ref_ids = []
    assert bucket_id in ctx.db
    for iid in items:
        assert iid in bucket.entries
        ref_id = NewsItemRef.dbid(bucket.id, iid)
        ref_ids.append(ref_id)
        assert ref_id in ctx.db
        
    # now destroy!
    bucket.delete()
    assert not bucket_id in ctx.db
    for ref_id in ref_ids:
        assert not ref_id in ctx.db

def test_immediate_add():
    from melkman.db.bucket import NewsBucket, immediate_add, view_entries
    ctx = fresh_context()
    
    bucket = NewsBucket.create(ctx)
    bucket.save()

    items = []
    for i in range(10):
        iid = random_id()
        immediate_add(bucket, iid, ctx)
        items.append(iid)
    
    # check what's in the database directly...
    query = {
        'startkey': bucket.id,
        'endkey': bucket.id + 'z'
    }
    bucket_items = [r.value for r in view_entries(ctx.db, **query)]
    
    for item in items:
        assert item in bucket_items
    for item in bucket_items:
        assert item in items
    
    # it should also show up in the bucket.
    for item in items:
        assert bucket.has_news_item(item)


def test_bucket_conflict_does_not_stop_item_additions():
    """
    This tests that item additions succeed independently from 
    changes to the bucket document. Workers rely on this 
    property to ignore certain conflicts and should be updated
    if it is no longer true.
    """
    from couchdb import ResourceConflict
    from melkman.db.bucket import NewsBucket
    ctx = fresh_context()
    
    bucket = NewsBucket.create(ctx)
    bucket.save()
    
    
    # create two copies of the bucket and modify them 
    # 'simultaneously' by adding items to each.
    b1 = NewsBucket.get(bucket.id, ctx)
    b2 = NewsBucket.get(bucket.id, ctx)
    
    items1 = [random_id() for i in range(10)]
    items2 = [random_id() for i in range(10)]
    
    for iid in items1:
        b1.add_news_item(iid)
        
    for iid in items2:
        b2.add_news_item(iid)
        
    
    # this should work, but cause a conflict 
    # when b2 is saved since the version will 
    # change.
    b1.save()
    
    try:
        b2.save()
    except ResourceConflict:
        pass
    else:
        assert 0, 'expected ResourceConflict'
        
        
    # nevertheless, the items added in both cases
    # should be a part of the bucket if we look it 
    # up again.
    b3 = NewsBucket.get(bucket.id, ctx)
    for iid in items1:
        assert iid in b3.entries
    for iid in items2:
        assert iid in b3.entries


def test_bucket_maxlen():
    """
    Test that bucket with maxlen behaves as expected
    """
    from melkman.db.bucket import NewsBucket, NewsItemRef
    from datetime import datetime, timedelta
    from operator import attrgetter
    ctx = fresh_context()
    
    # add 10 items spaced an hour apart to a bucket of max-length 3
    maxlen = 3
    sortkey = attrgetter('timestamp')
    bucket = NewsBucket.create(ctx, maxlen=maxlen, sortkey=sortkey)
    items = []
    timestamp = datetime.utcnow()
    for i in xrange(10):
        item = NewsItemRef.create_from_info(ctx, bucket.id,
            item_id=random_id(),
            timestamp=timestamp - timedelta(hours=i),
            )
        items.append(item)
        bucket.add_news_item(item)

    # make sure the bucket has only the maxlen latest items
    # before and after saving

    idgetter = attrgetter('item_id')
    sorteditemsids = map(idgetter, sorted(items, key=sortkey))

    def ids_by_timestamp(entries):
        return map(idgetter, sorted(entries.values(), key=sortkey))

    def check_before_and_after_save(bucket):
        bucketlen = len(bucket)
        if bucket.maxlen is not None:
            assert bucketlen <= bucket.maxlen

        assert ids_by_timestamp(bucket.entries) == sorteditemsids[-bucketlen:]
        bucket.save()

        # XXX this throws away maxlen and sortkey! persist them?
        maxlen = bucket.maxlen
        bucket = NewsBucket.get(bucket.id, ctx)
        bucket.maxlen = maxlen
        bucket._sortkey = sortkey

        assert ids_by_timestamp(bucket.entries) == sorteditemsids[-bucketlen:]

        return bucket

    bucket = check_before_and_after_save(bucket)

    # decrease maxlen and make sure bucket.entries remains consistent
    maxlen -= 1
    bucket.maxlen = maxlen
    bucket = check_before_and_after_save(bucket)

    # now increase maxlen so that the bucket is under capacity and check consistency
    maxlen += 2
    bucket.maxlen = maxlen
    bucket = check_before_and_after_save(bucket)

    # fill to capacity and check that the new maxlen is maintained
    for i in items:
        bucket.add_news_item(i)

    bucket = check_before_and_after_save(bucket)

    # now set the maxlen to None to make an unbounded bucket, add items and
    # check that they're all accommodated
    bucket.maxlen = None
    for i in items:
        bucket.add_news_item(i)

    bucket = check_before_and_after_save(bucket)


    # if you add something to bucket._entries directly rather than going
    # through the interface, make sure it's there after saving
    bucket.clear()
    bucket.save()
    assert len(bucket) == 0
    item = items[0]
    bucket.entries[item.item_id] = item
    assert bucket.has_news_item(item)
    bucket.save()
    assert bucket.has_news_item(item)
