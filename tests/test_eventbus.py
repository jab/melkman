from helpers import *

def test_event_send_recieve():
    from eventlet.api import sleep
    from melkman.eventbus import EventBus
    ctx = fresh_context()
    event_bus = EventBus(ctx)
    
    got_events = dict()
    got_events['count'] = 0
    def got_one(event):
        if event['foo'] == 12:
            got_events['count'] += 1

    event_bus.add_listener('test', got_one)
    event_bus.send('test', {'foo': 12})
    
    sleep(.5)
    
    assert got_events['count'] == 1
    
    event_bus.kill()
    

def test_multiple_listeners():
    from eventlet.api import sleep
    from melkman.eventbus import EventBus
    ctx = fresh_context()
    event_bus = EventBus(ctx)

    got_events = dict()
    got_events['one'] = 0
    def got_one(event):
        if event['foo'] == 12:
            got_events['one'] += 1

    got_events['two'] = 0
    def got_two(event):
        if event['foo'] == 12:
            got_events['two'] += 1

    event_bus.add_listener('test', got_one)
    event_bus.add_listener('test', got_two)
    event_bus.send('test', {'foo': 12})

    sleep(.5)

    assert got_events['one'] == 1
    assert got_events['two'] == 1

    event_bus.kill()


def test_multiple_consumers():
    from eventlet.api import sleep
    from melkman.eventbus import EventBus
    ctx = fresh_context()
    event_bus1 = EventBus(ctx)

    got_events = dict()
    got_events['one'] = 0
    def got_one(event):
        if event['foo'] == 12:
            got_events['one'] += 1

    event_bus1.add_listener('test', got_one)

    event_bus2 = EventBus(ctx)

    got_events['two'] = 0
    def got_two(event):
        if event['foo'] == 12:
            got_events['two'] += 1
    
    event_bus2.add_listener('test', got_two)


    event_bus1.send('test', {'foo': 12})

    sleep(.5)
    assert got_events['one'] == 1
    assert got_events['two'] == 1

    event_bus2.send('test', {'foo': 12})

    sleep(.5)
    assert got_events['one'] == 2
    assert got_events['two'] == 2

    event_bus1.kill()
    event_bus2.kill()

def test_multiple_channels():
    from eventlet.api import sleep
    from melkman.eventbus import EventBus
    ctx = fresh_context()
    event_bus = EventBus(ctx)

    got_events = dict()
    got_events['one'] = 0
    def got_one(event):
        if event['foo'] == 12:
            got_events['one'] += 1

    got_events['two'] = 0
    def got_two(event):
        if event['foo'] == 12:
            got_events['two'] += 1

    event_bus.add_listener('test1', got_one)
    event_bus.add_listener('test2', got_two)
    event_bus.send('test1', {'foo': 12})

    sleep(.5)

    assert got_events['one'] == 1
    assert got_events['two'] == 0

    event_bus.send('test2', {'foo': 12})
    
    sleep(.5)
    
    assert got_events['one'] == 1
    assert got_events['two'] == 1

    event_bus.kill()