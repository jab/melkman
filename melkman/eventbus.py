from carrot.messaging import Publisher, Consumer
from eventlet.coros import event
from eventlet.proc import spawn
from uuid import uuid1
from melkman.green import resilient_consumer_loop

def exchange_for_channel(channel):
    return 'eventbus.%s' % channel

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
    no_ack = True

    def __init__(self, channel, context):
        queue = 'eb_%s' % uuid1().hex
        Consumer.__init__(self, context.broker,
                          exchange=exchange_for_channel(channel),
                          queue=queue)

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
            p.wait()