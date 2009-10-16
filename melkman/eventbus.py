from __future__ import with_statement
from carrot.messaging import Publisher, Consumer
from eventlet.coros import event, semaphore
from eventlet.proc import spawn
from uuid import uuid1
from melkman.green import resilient_consumer_loop

def exchange_for_channel(channel):
    return 'eb.%s' % channel

class EventPublisher(Publisher):
    exchange_type = 'fanout'
    delivery_mode = 1
    durable = False
    auto_delete = False

    def __init__(self, channel, context):
        Publisher.__init__(self, context.broker, exchange=exchange_for_channel(channel))

class EventConsumer(Consumer):
    exchange_type = 'fanout'
    exclusive = True
    durable = False
    no_ack = True

    def __init__(self, channel, context):
        self._callback_lock = semaphore(1)
        queue = 'eb_%s' % uuid1().hex
        Consumer.__init__(self, context.broker,
                          exchange=exchange_for_channel(channel),
                          queue=queue)

    def register_callback(self, callback):
        with self._callback_lock:
            Consumer.register_callback(self, callback)

    def remove_callback(self, callback):
        with self._callback_lock:
            try:
                self.callbacks.remove(callback)
            except ValueError:
                pass

    def receive(self, message_data, message):
        with self._callback_lock:
            for callback in self.callbacks:
                try:
                    callback(message_data, message)
                except:
                    log.error("Error during callback to message %s: %s" % 
                              (message_data, traceback.format_exc()))
                
    
class EventBus(object):
    """
    A simple broadcast transient event bus
    built on AMQP.
    """
    def __init__(self, context):
        self.context = context
        self._consumers = {}
        self._procs = {}

    def send(self, channel, event):
        pub = EventPublisher(channel, self.context)
        pub.send(event)
        pub.close()

    def add_listener(self, channel, callback):
        consumer = self._consumers.get(channel)
        if consumer is None:
            consumer, proc = self._start_consumer(channel)
            self._procs[channel] = proc
        def cb(message_data, message):
            callback(message_data)
        consumer.register_callback(cb)

    def remove_listener(self, channel, callback):
        consumer = self._consumers.get(channel)
        if consumer is not None:
            consumer.remove_callback(callback)

    def _start_consumer(self, channel):
        ready = event()
        def make_consumer(ctx):
            consumer = EventConsumer(channel, ctx)
            self._consumers[channel] = consumer
            if not ready.has_result():
                ready.send(consumer)
            return consumer
        proc = spawn(resilient_consumer_loop, make_consumer, self.context)
        consumer = ready.wait()
        return consumer, proc

    def kill(self):
        for p in self._procs.values():
            p.kill()
        waitall(self._procs.values())
