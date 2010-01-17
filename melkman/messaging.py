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

from carrot.messaging import Publisher, Consumer
from eventlet.coros import event
from eventlet.proc import spawn, waitall, ProcExit
import logging
import traceback
from uuid import uuid1

log = logging.getLogger(__name__)

__all__ = ['EventBus', 'MessageDispatch']


def consumer_loop(make_consumer, context):
    consumer = None
    try:
        consumer = make_consumer(context)
        it = consumer.iterconsume()
        while it.next():
            pass
    except ProcExit:
        log.debug("consumer_loop: killed")
    finally:
        if consumer:
            consumer.close()
        context.close()

def _exchange_for_channel(channel):
    return 'eventbus.%s' % channel

class EventPublisher(Publisher):
    exchange_type = 'fanout'
    delivery_mode = 1
    durable = False
    auto_delete = False

    def __init__(self, channel, context):
        Publisher.__init__(self, context.broker, exchange=_exchange_for_channel(channel))

class EventConsumer(Consumer):
    exchange_type = 'fanout'
    exclusive = True
    no_ack = True

    def __init__(self, channel, context):
        queue = 'eb_%s' % uuid1().hex
        Consumer.__init__(self, context.broker,
                          exchange=_exchange_for_channel(channel),
                          queue=queue)

class EventBus(object):
    """
    A simple broadcast transient event bus. 
    Events are transient, and broadcast to all 
    attached listeners.
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
        """
        add a callback that is triggered whenever a message
        is sent to the channel specified. 
        
        callback - single argument function accepting the event.
        """
        consumer = self._consumers.get(channel)
        if consumer is None:
            consumer, proc = self._start_consumer(channel)
            self._procs[channel] = proc
        def cb(message_data, message):
            try:
                callback(message_data)
            except:
                log.error("Unhandled exception in event callback for channel %s: %s" % (channel, traceback.format_exc()))

        consumer.register_callback(cb)

    def _start_consumer(self, channel):
        ready = event()
        def make_consumer(ctx):
            consumer = EventConsumer(channel, ctx)
            self._consumers[channel] = consumer
            if not ready.has_result():
                ready.send(consumer)
            return consumer
        proc = spawn(consumer_loop, make_consumer, self.context)
        consumer = ready.wait()
        return consumer, proc

    def kill(self):
        for p in self._procs.values():
            p.kill()
        waitall(self._procs.values())



class MessageDispatchPublisher(Publisher):
    exchange = 'work_dispatch'
    exchange_type = 'direct'
    delivery_mode = 2
    
    def __init__(self, message_type, context):
        Publisher.__init__(self, context.broker, routing_key=message_type)
    

def _queue_id_for(message_type):
    return '%s.%s' % (MessageDispatchPublisher.exchange, message_type) 

class MessageDispatchConsumer(Consumer):
    exchange = MessageDispatchPublisher.exchange
    durable = True
    
    def __init__(self, message_type, queue, context):
        Consumer.__init__(self, context.broker, routing_key=message_type, queue=queue)


class MessageDispatch(object):
    """
    MessageDispatch manages sending messages out to queues of 
    workers and registration of workers based on message type
    and function.  A message is delivered to queues based on 
    its type and processed by exactly one listener per queue.  
    
    If broadcast to all listeners is desired, see EventBus.
    """

    def __init__(self, context):
        self.context = context

        
    def send(self, message, message_type):
        """
        send a message to any worker queues listening to the type
        
        message - message to send
        type - type of message
        """
        pub = MessageDispatchPublisher(message_type, self.context)
        pub.send(message)
        pub.close()

    def start_worker(self, message_type, callback, queue=None):
        """
        begin a worker process handling messages of the type specified.
        callback - a function accepting a job description and a message. 
        it is the responsibility of the callback to acknowledge the 
        message.
        
        each message is delivered to at most one worker per queue. If 
        no queue is specified, a default queue for the message type is
        used.  To configure multiple responses to a message type, 
        parallel queues may be specified for the different worker types.

        returns an eventlet Proc representing the process that is actively
        consuming messages from the work queue.  This process may be killed
        to stop additional messages from being processed by the registered 
        callback.
        
        Callbacks are executed on the consumer process greenlet.
        """
        if queue is None:
            queue = _queue_id_for(message_type)
        
        def cb(message_data, message):
            try:
                callback(message_data, message)
            except:
                log.error("Unhandled exception handling work on queue %s (job=%s): %s" % (queue, message_data, traceback.format_exc()))

        def create_consumer(context):
            consumer = MessageDispatchConsumer(message_type, queue, context)
            consumer.register_callback(cb)
            return consumer

        return spawn(consumer_loop, create_consumer, self.context)


    def declare(self, message_type, queue=None):
        """
        declare a queue for the message type specified. This insures that
        messages of this type will be queued regardless of whether any 
        workers have registered.
        
        if queue is not specified, the default queue for the message type is declared.
        """
        if queue is None:
            queue = _queue_id_for(message_type)
        c = MessageDispatchConsumer(message_type, queue, self.context)
        c.close()

    def clear(self, message_type, queue=None):
        """
        clear any pending messages of the type specified from the queue specified.
        if queue is not specified, the default queue for the message type is used.
        """
        if queue is None:
            queue = _queue_id_for(message_type)

        backend = self.context.broker.create_backend()
        backend.queue_purge(queue)
        backend.close()

    def delete(self, message_type, queue=None):
        """
        clear any pending messages of the type specified from the queue specified
        and destroy the queue.  if queue is not specified, the default queue for 
        the message type is used.
        """
        if queue is None:
            queue = _queue_id_for(message_type)

        backend = self.context.broker.create_backend()
        backend.queue_purge(queue)
        backend.close()

class pooled(object):
    """
    decorator that executes the enclosed 
    callback in a pool eg: 
    
    @pooled(some_pool)
    def my_callback(md, m):
        ...
    """
    def __init__(self, pool):
        self.pool = pool
        
    def __call__(self, callback):
        def cb(message_data, message):
            return self.pool.execute(callback, message_data, message)
        return cb


def always_ack(handler):
    """
    decorator for message handlers that 
    acknowleges the message on any exit 
    condition if it has not already been 
    acknowledged.
    """
    def wrap_handler(message_data, message):
        try:
            handler(message_data, message)
        finally:
            if not message.acknowledged:
                message.ack()
    return wrap_handler
