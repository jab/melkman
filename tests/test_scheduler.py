from helpers import *

@contextual
def test_deferred_in_database(ctx):
    from datetime import datetime, timedelta
    from carrot.messaging import Consumer
    from eventlet import sleep, spawn
    import logging
    from melk.util.nonce import nonce_str
    import sys

    from melkman.context import Context
    from melkman.scheduler import defer_amqp_message, cancel_deferred
    from melkman.scheduler.worker import ScheduledMessageService
    from melkman.scheduler.worker import DeferredAMQPMessage, view_deferred_messages_by_timestamp

    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)
    
    m1 = {'hello_world': nonce_str()}
    when = no_micro(datetime.utcnow() + timedelta(hours=2))
    defer_amqp_message(when, m1, 'testq', 'testx', ctx)
    
    # give it a sec to write it out, then close it down.
    sleep(1)
    sched.kill()
    sched.wait()
    
    # check the database for the message we sent
    count = 0
    for r in view_deferred_messages_by_timestamp(ctx.db, include_docs=True):
        count += 1
        message = DeferredAMQPMessage.wrap(r.doc)
        for (k, v) in m1.items():
            assert message.message[k] == v
        assert message.timestamp == when
    assert count == 1

    



@contextual
def test_deferred_send_receive(ctx):
    from datetime import datetime, timedelta
    from carrot.messaging import Consumer
    from eventlet import sleep, spawn, with_timeout
    from eventlet.event import Event
    from eventlet.support.greenlets import GreenletExit
    import logging
    from melk.util.nonce import nonce_str
    import sys

    from melkman.context import Context
    from melkman.scheduler import defer_amqp_message, cancel_deferred
    from melkman.scheduler.worker import ScheduledMessageService

    got_message = Event()
    def got_message_cb(*args, **kw):
        got_message.send(True)

    def do_consume():
        consumer = Consumer(ctx.broker, exchange='testx', queue='testq', 
                            routing_key='testq', exclusive=True, durable=False)
        consumer.register_callback(got_message_cb)
        try:
            consumer.wait(limit=1)
        except StopIteration:
            pass
        except GreenletExit:
            pass
        finally:
            consumer.close()
            

    cons = spawn(do_consume)

    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)

    m1 = {'hello': 'world'}
    now = datetime.utcnow()
    wait = timedelta(seconds=2)
    defer_amqp_message(now + wait, m1, 'testq', 'testx', ctx)

    try:
        #sleep(1)
        with_timeout(10, got_message.wait)
        assert got_message.ready()
    finally:
        sched.kill()
        sched.wait()
        cons.kill()
        cons.wait()
        

@contextual
def test_defer_event(ctx):
    from datetime import datetime, timedelta
    from eventlet import sleep, spawn, with_timeout
    from eventlet.event import Event
    from melkman.messaging import EventBus
    from melkman.scheduler import defer_event
    from melkman.scheduler.worker import ScheduledMessageService

    CHAN = 'test_chan'

    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)

    got_message = Event()
    def got_message_cb(*args, **kw):
        got_message.send(True)
    

    eb = EventBus(ctx)
    eb.add_listener(CHAN, got_message_cb)

    now = datetime.utcnow()
    wait = timedelta(seconds=2)
    defer_event(now + wait, CHAN, {'foo': 'bar'}, ctx)

    sleep(3)

    try:
        with_timeout(10, got_message.wait)
        assert got_message.ready()
    finally:
        eb.kill()
        sched.kill()
        sched.wait()
        

@contextual
def test_defer_message_dispatch(ctx):
    from datetime import datetime, timedelta
    from eventlet import sleep, spawn, with_timeout
    from eventlet.event import Event
    from melkman.messaging import MessageDispatch, always_ack
    from melkman.scheduler import defer_message
    from melkman.scheduler.worker import ScheduledMessageService

    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)
    w = MessageDispatch(ctx)
    
    message_type = 'test_dispatch_send_recv'
    
    work_result = Event()
    
    @always_ack
    def handler(job, message):
        work_result.send(sum(job['values']))
    
    worker = w.start_worker(message_type, handler)

    try:
        now = datetime.utcnow()
        wait = timedelta(seconds=2)
        # w.send({'values': [1, 2]}, message_type)
        defer_message(now + wait, {'values': [1 ,2]}, message_type, ctx)
        sleep(3)
    
        assert with_timeout(2, work_result.wait) == 3
    finally:
        worker.kill()
        worker.wait()
        sched.kill()
        sched.wait()
        

