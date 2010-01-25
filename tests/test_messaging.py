from helpers import *

@contextual
def test_event_send_receive(ctx):
    from eventlet import sleep
    from melkman.messaging import EventBus

    try:
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
    
    finally:
        event_bus.kill()
        

@contextual
def test_event_multiple_listeners(ctx):
    from eventlet import sleep
    from melkman.messaging import EventBus
    event_bus = EventBus(ctx)

    try:
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
    finally:
        event_bus.kill()
        

@contextual
def test_event_multiple_consumers(ctx):
    from eventlet import sleep
    from melkman.messaging import EventBus

    try:
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
    finally:
        event_bus1.kill()
        event_bus2.kill()
        

@contextual
def test_event_multiple_channels(ctx):
    from eventlet import sleep
    from melkman.messaging import EventBus

    event_bus = EventBus(ctx)
    try:
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
    finally:
        event_bus.kill()
        

@contextual
def test_dispatch_send_recv(ctx):
    from eventlet import with_timeout
    from eventlet.event import Event
    from melkman.messaging import MessageDispatch, always_ack

    w = MessageDispatch(ctx)
    message_type = 'test_dispatch_send_recv'
    
    work_result = Event()
    
    @always_ack
    def handler(job, message):
        work_result.send(sum(job['values']))
    
    worker = w.start_worker(message_type, handler)
    w.send({'values': [1, 2]}, message_type)
    
    try:
        assert with_timeout(2, work_result.wait) == 3
    finally:
        worker.kill()
        worker.wait()
        

@contextual
def test_dispatch_one_receiver(ctx):
    from eventlet import sleep
    from eventlet.event import Event
    from melkman.messaging import MessageDispatch, always_ack

    w = MessageDispatch(ctx)
    message_type = 'test_dispatch_one_receiver'

    work_result = Event()

    got_events = {'count': 0}
    
    @always_ack
    def handler(job, message):
        got_events['count'] += 1

    worker1 = w.start_worker(message_type, handler)
    worker2 = w.start_worker(message_type, handler)
    try:
        w.send({}, message_type)
        sleep(2)
    
        assert got_events['count'] == 1
    finally:
        worker1.kill()
        worker1.wait()
        worker2.kill()
        worker2.wait()
        