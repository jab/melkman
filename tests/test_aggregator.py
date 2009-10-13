from melkman.green import green_init
green_init()
from helpers import *
import logging

log = logging.getLogger(__name__)

def test_modified_updates_composite():
    from eventlet.api import sleep
    from eventlet.proc import spawn, waitall
    from melkman.aggregator.worker import CompositeUpdater, CompositeUpdateDispatcher
    from melkman.db.bucket import NewsBucket
    from melkman.db.composite import Composite
    from melkman.green import consumer_loop

    ctx = fresh_context()

    dispatcher = spawn(consumer_loop, CompositeUpdateDispatcher, ctx)
    updater = spawn(consumer_loop, CompositeUpdater, ctx)
    
    b = []
    c = []
    # make some buckets and composites. 
    for i in range(3):
        bucket = NewsBucket.create(ctx)
        bucket.save()
        b.append(bucket)
     
        comp = Composite.create(ctx)
        comp.save()
        c.append(comp)
    
    # set up some subscriptions
    c[0].subscribe(b[0])
    c[1].subscribe(b[1])
    c[2].subscribe(c[0])
    c[2].subscribe(c[1])
    for i in range(3):
        c[i].save()
        
    id1 = random_id()
    b[0].add_news_item(id1)
    log.debug("updating bucket 0 (%s) with item %s..." % (b[0].id, id1))
    b[0].save()
    sleep(1)
    
    # refresh them from the db...
    for i in range(3):
        c[i] = Composite.get(c[i].id, ctx)
    
    assert c[0].has_news_item(id1)
    assert not c[1].has_news_item(id1)
    assert c[2].has_news_item(id1)
    
    id2 = random_id()
    b[1].add_news_item(id2)
    log.debug("updating bucket 1 (%s) with item %s..." % (b[1].id, id2))
    b[1].save()
    sleep(1)

    # refresh them from the db...
    for i in range(3):
        c[i] = Composite.get(c[i].id, ctx)
    
    assert not c[0].has_news_item(id2)
    assert c[1].has_news_item(id2)
    assert c[2].has_news_item(id2)
    
    id3 = random_id()
    b[2].add_news_item(id3)
    log.debug("updating bucket 2 (%s) with item %s..." % (b[2].id, id3))
    b[2].save()
    sleep(1)

    # refresh them from the db...
    for i in range(3):
        c[i] = Composite.get(c[i].id, ctx)

    assert not c[0].has_news_item(id3)
    assert not c[1].has_news_item(id3)
    assert not c[2].has_news_item(id3)

    dispatcher.kill()
    updater.kill()
    waitall([dispatcher, updater])
    ctx.close()
    
def test_sub_loop_sane():
    from eventlet.api import sleep
    from eventlet.proc import spawn, waitall
    from melkman.aggregator.worker import CompositeUpdater, CompositeUpdateDispatcher
    from melkman.db.bucket import NewsBucket
    from melkman.db.composite import Composite
    from melkman.green import consumer_loop

    ctx = fresh_context()

    dispatcher = spawn(consumer_loop, CompositeUpdateDispatcher, ctx)
    updater = spawn(consumer_loop, CompositeUpdater, ctx)

    # create two composites and subscribe them 
    # to each other... O_O
    c1 = Composite.create(ctx)
    c2 = Composite.create(ctx)
    c1.save()
    c2.save()
    
    c1.subscribe(c2)
    c2.subscribe(c1)
    
    c1.save()
    c2.save()

    for i in range(10):
        c1.add_news_item(random_id())    
        c2.add_news_item(random_id())

    c1.save()
    c2.save()

    sleep(1)

    # refresh
    c1 = Composite.get(c1.id, ctx)
    c2 = Composite.get(c2.id, ctx)

    assert len(c1.entries) == 20
    assert len(c2.entries) == 20
    
    for iid in c1.entries: 
        assert c2.has_news_item(iid)

    for iid in c2.entries: 
        assert c1.has_news_item(iid)

    sleep(1)

    dispatcher.kill()
    updater.kill()
    waitall([dispatcher, updater])

    from melkman.aggregator.api import SUBSCRIPTION_UPDATE_QUEUE
    from melkman.aggregator.api import COMPOSITE_DISPATCH_QUEUE
    backend = ctx.broker.create_backend()
    assert backend.get(SUBSCRIPTION_UPDATE_QUEUE) is None
    assert backend.get(COMPOSITE_DISPATCH_QUEUE) is None
    
    ctx.close()


def test_init_subscription():
    from eventlet.api import sleep
    from eventlet.proc import spawn, waitall
    from melkman.aggregator.worker import CompositeUpdater, CompositeUpdateDispatcher
    from melkman.db.bucket import NewsBucket
    from melkman.db.composite import Composite
    from melkman.green import consumer_loop

    ctx = fresh_context()

    dispatcher = spawn(consumer_loop, CompositeUpdateDispatcher, ctx)
    updater = spawn(consumer_loop, CompositeUpdater, ctx)

    c = Composite.create(ctx)
    c.save()

    entries = []
    bucket = NewsBucket.create(ctx)
    for i in range(5):
        eid = random_id()
        entries.append(eid)
        bucket.add_news_item(eid)
    bucket.save()
    sleep(.5)

    c.subscribe(bucket)
    c.save()
    sleep(.5)

    c = Composite.get(c.id, ctx)
    for eid in entries:
        assert c.has_news_item(eid)

    dispatcher.kill()
    updater.kill()
    waitall([dispatcher, updater])

    ctx.close()
