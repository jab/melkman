from helpers import *


def test_deferred_in_database():
    from datetime import datetime, timedelta
    from carrot.messaging import Consumer
    from eventlet.api import sleep
    from eventlet.proc import spawn
    import logging
    from melk.util.nonce import nonce_str
    import sys

    from melkman.context import Context
    from melkman.scheduler import defer_amqp_message, cancel_deferred
    from melkman.scheduler.worker import ScheduledMessageService
    from melkman.scheduler.worker import DeferredAMQPMessage, view_deferred_messages_by_timestamp


    # log = logging.getLogger('melkman.scheduler')
    # log.setLevel(level=logging.DEBUG)
    # log.addHandler(logging.StreamHandler(sys.stdout))
    ctx = fresh_context()

    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)
    
    m1 = {'hello_world': nonce_str()}
    when = no_micro(datetime.utcnow() + timedelta(hours=2))
    defer_amqp_message(when, m1, 'testq', 'testx', ctx)
    
    # give it a sec to write it out, then close it down.
    sleep(1)
    sched.kill()
    
    # check the database for the message we sent
    count = 0
    for r in view_deferred_messages_by_timestamp(ctx.db, include_docs=True):
        count += 1
        message = DeferredAMQPMessage.wrap(r.doc)
        for (k, v) in m1.items():
            assert message.message[k] == v
        assert message.timestamp == when
    assert count == 1

def test_deferred_send_receive():
    from datetime import datetime, timedelta
    from carrot.messaging import Consumer
    from eventlet.api import sleep
    from eventlet.proc import spawn
    from eventlet.coros import event
    import logging
    from melk.util.nonce import nonce_str
    import sys

    from melkman.context import Context
    from melkman.green import timeout_wait
    from melkman.scheduler import defer_amqp_message, cancel_deferred
    from melkman.scheduler.worker import ScheduledMessageService
    from melkman.scheduler.worker import DeferredAMQPMessage, view_deferred_messages_by_timestamp


    ctx = fresh_context()

    from melkman.scheduler.worker import ScheduledMessageService

    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)

    got_message = event()
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
        finally:
            consumer.close()
            ctx.close()

    cons = spawn(do_consume)

    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)
    
    m1 = {'hello': 'world'}
    now = datetime.utcnow()
    wait = timedelta(seconds=2)
    defer_amqp_message(now + wait, m1, 'testq', 'testx', ctx)

    timeout_wait(got_message, 10)
    assert got_message.ready()
    
    sched.kill()
    cons.kill()
    ctx.close()
    
def test_defer_event():
    from datetime import datetime, timedelta
    from eventlet.api import sleep
    from eventlet.coros import event
    from eventlet.proc import spawn
    from melkman.green import timeout_wait
    from melkman.messaging import EventBus
    from melkman.scheduler import defer_event
    from melkman.scheduler.worker import ScheduledMessageService

    CHAN = 'test_chan'

    ctx = fresh_context()
    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)

    got_message = event()
    def got_message_cb(*args, **kw):
        got_message.send(True)
    

    eb = EventBus(ctx)
    eb.add_listener(CHAN, got_message_cb)

    now = datetime.utcnow()
    wait = timedelta(seconds=2)
    defer_event(now + wait, CHAN, {'foo': 'bar'}, ctx)

    sleep(3)

    try:
        timeout_wait(got_message, 10)
        assert got_message.ready()
    finally:
        eb.kill()
        sched.kill()
    
def test_defer_message_dispatch():
    from datetime import datetime, timedelta
    from eventlet.api import sleep
    from eventlet.coros import event
    from eventlet.proc import spawn
    from melkman.green import timeout_wait
    from melkman.messaging import MessageDispatch, always_ack
    from melkman.scheduler import defer_message
    from melkman.scheduler.worker import ScheduledMessageService

    ctx = fresh_context()
    sms = ScheduledMessageService(ctx)
    sched = spawn(sms.run)
    w = MessageDispatch(ctx)
    
    message_type = 'test_dispatch_send_recv'
    
    work_result = event()
    
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
    
        assert timeout_wait(work_result, 2) == 3
    finally:
        worker.kill()
        sched.kill()

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    test_deferred_send_receive()