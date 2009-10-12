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
    from melkman.db import NewsBucket
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

