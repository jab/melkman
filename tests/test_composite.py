from helpers import *

def test_create_composite():
    from melkman.db.composite import Composite
    ctx = fresh_context()
    cc = Composite()
    cc.save(ctx)

def test_composites_by_sub():
    from melkman.db.bucket import NewsBucket
    from melkman.db.composite import Composite, view_composites_by_subscription
    ctx = fresh_context()
    c1 = Composite()
    c2 = Composite()

    bucket1 = NewsBucket()
    bucket1.save(ctx)
    
    bucket2 = NewsBucket()
    bucket2.save(ctx)

    bucket3 = NewsBucket()
    bucket3.save(ctx)
    
    c1.subscribe(bucket1)
    c1.save(ctx)

    c2.subscribe(bucket1)
    c2.subscribe(bucket2)
    c2.save(ctx)

    count = 0
    seen = set()
    for r in view_composites_by_subscription(ctx.db, include_docs=True, startkey=bucket1.id, endkey=bucket1.id):
        comp = Composite.wrap(r.doc)
        seen.add(comp.id)
        count += 1
    assert count == 2
    assert c1.id in seen
    assert c2.id in seen

    count = 0
    seen = set()
    for r in view_composites_by_subscription(ctx.db, include_docs=True, startkey=bucket2.id, endkey=bucket2.id):
        comp = Composite.wrap(r.doc)
        seen.add(comp.id)
        count += 1
    assert count == 1
    assert c2.id in seen


    for r in view_composites_by_subscription(ctx.db, include_docs=True, startkey=bucket3.id, endkey=bucket3.id):
        assert False, 'unexpected subscription'

def test_composite_subs_by_title():
    from melkman.db.bucket import NewsBucket
    from melkman.db.composite import Composite, view_composite_subscriptions_by_title
    from random import shuffle
    
    ctx = fresh_context()
    cc = Composite()

    buckets = []
    for i in range(10):
        bucket = NewsBucket()
        bucket.title = 'bucket %d' % i
        bucket.save(ctx)
        buckets.append(bucket)
    
    shuffled_buckets = list(buckets)
    shuffle(shuffled_buckets)
    
    for bucket in shuffled_buckets:
        cc.subscribe(bucket)
    cc.save(ctx)
    
    # should come out in alphabetical order

    for i, row in enumerate(view_composite_subscriptions_by_title(ctx.db, startkey=[cc.id, None], endkey=[cc.id, {}])):
        assert row.value['bucket_id'] == buckets[i].id
    assert i + 1 == len(buckets)

def test_composite_filtered_update():
    from melkman.db.composite import Composite
    from random import shuffle
    
    ctx = fresh_context()
    cc = Composite()
    # a filter stack that accepts only things with the 
    # word tortoise in the title, or is tagged tortoise
    cc.filters.append({'op': 'match_title',
                       'config': {'values': ['tortoise'],
                                  'match_type': 'substring'},
                       'action': 'accept'})
    cc.filters.append({'op': 'match_tag',
                      'config': {'values': ['tortoise']},
                      'action': 'accept'})
    cc.filters.append({'op': 'match_all',
                       'config': {},
                       'action': 'reject'})
    
    ok_items = [dummy_news_item({'title': "The tortoise and the O'Hare"}),
                dummy_news_item({'details': {'tags': [{'label': 'tortoise'}, {'label': 'shells'}]}})]
    
    not_ok_items = [dummy_news_item({'title': 'Jellybirds'}),
                    dummy_news_item({'details': {'tags': [{'label': 'hare'}, {'label': 'shells'}]}})]
    
    all_items = []
    all_items += ok_items
    all_items += not_ok_items
    shuffle(all_items)
    
    cc.filtered_update(all_items, ctx)
    
    for item in ok_items:
        assert cc.has_news_item(item)

    for item in not_ok_items:
        assert not cc.has_news_item(item)
    
    
