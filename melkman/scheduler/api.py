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

from couchdb import ResourceConflict, ResourceNotFound
from couchdb.design import ViewDefinition
from couchdb.schema import *
from datetime import datetime, timedelta
import logging
import traceback

from giblets import Component, implements
from melkman.context import IRunDuringBootstrap
from melkman.messaging import MessageDispatch, MessageDispatchPublisher
from melkman.messaging import EventPublisher
from melkman.db import delete_all_in_view

__all__ = ['defer_amqp_message', 'defer_event', 'defer_message', 'cancel_deferred']

log = logging.getLogger(__name__)

SCHEDULER_COMMAND = 'melkman.scheduler'

DEFER_MESSAGE_COMMAND = 'schedule'
CANCEL_MESSAGE_COMMAND = 'cancel'

def defer_event(send_time, channel, event, context, **kw):
    """
    defer EventBus.send(channel, event) until send_time
    """
    pub = EventPublisher(channel, context)
    return defer_publish(send_time, event, pub, context, **kw)

def defer_message(send_time, message, message_type, context, **kw):
    """
    defer MessageDispatch.send(message, message_type) until send_time 
    """
    pub = MessageDispatchPublisher(message_type, context)
    return defer_publish(send_time, message, pub, context, **kw)
    
def defer_publish(send_time, message, publisher, context, **kw):
    """
    Defers the action of publisher.send(message) until the 
    time specified. 
    
    message - message to defer
    publisher - a carrot.messaging.Publisher with all 
                publication details specified.
    """
    return defer_amqp_message(send_time, message, publisher.routing_key,
                              publisher.exchange, context, 
                              exchange_type=publisher.exchange_type, 
                              delivery_mode=publisher.delivery_mode,
                              **kw)
    

def defer_amqp_message(send_time, message, routing_key, exchange, context, **kw):
    """
    This is a lower level version of defer which allows
    specification of the exact amqp exchange and routing_key 

    send_time: datetime representing when to send
    message: the message to send
    exchange: the exchange to send to
    routing_key: the routing key to use when sending
    context: current melkman context 
    
    optional kwargs:
    message_id 
    mandatory
    delivery_mode
    priority
    """

    message = {
        'command': DEFER_MESSAGE_COMMAND,
        'timestamp': DateTimeField()._to_json(send_time),
        'exchange': exchange,
        'routing_key': routing_key,
        'message': message,
    }
    message.update(**kw)

    publisher = MessageDispatch(context)
    publisher.send(message, SCHEDULER_COMMAND)



def cancel_deferred(message_id, context):
    message = {
        'command': CANCEL_MESSAGE_COMMAND,
        'message_id': message_id
    }
    publisher = MessageDispatch(context)
    publisher.send(message, SCHEDULER_COMMAND)


class SchedulerSetup(Component):
    implements(IRunDuringBootstrap)

    def bootstrap(self, context, purge=False):
        
        log.info("Syncing deferred message database views...")
        view_deferred_messages_by_timestamp.sync(context.db)

        log.info("Setting up scheduler queues...")
        dispatch = MessageDispatch(context)
        dispatch.declare(SCHEDULER_COMMAND)
        if purge == True:
            log.info("Clearing scheduler queues...")
            dispatch.clear(SCHEDULER_COMMAND)
            log.info("Destroying existing deferred messages...")
            delete_all_in_view(context.db, view_deferred_messages_by_timestamp)
        context.close()
            
class DeliveryOptions(Schema):
    exchange = TextField()
    routing_key = TextField()
    exchange_type = TextField(default='direct')
    delivery_mode = IntegerField(default=2)
    mandatory = BooleanField(default=False)
    priority = IntegerField(default=0)

class DeferredAMQPMessage(Document):
    document_types = ListField(TextField(), default=['DeferredAMQPMessage'])

    message_id = TextField()
    timestamp = DateTimeField()
    options = DictField(DeliveryOptions)
    message = DictField()

    claimed = BooleanField(default=False)
    error_count = IntegerField(default=0)


    def claim(self, db):
        """
        """
        if self.claimed == True:
            return False
        try:
            self.timestamp = datetime.utcnow()
            self.claimed = True
            self.store(db)
            return True
        except ResourceConflict:
            return False

    def unclaim(self, db, reschedule_time=None):
        self.claimed = False
        if reschedule_time is not None:
            self.timestamp = reschedule_time
        self.store(db)

    @classmethod
    def lookup_by_message_id(cls, db, message_id):
        dbid = cls.id_for_message_id(message_id)
        return cls.load(db, dbid)

    @classmethod
    def create_from_message_id(cls, message_id):
        dbid = cls.id_for_message_id(message_id)
        return cls(dbid)

    @classmethod
    def id_for_message_id(cls, message_id):
        return 'DeferredAMQPMessage:%s' % message_id

view_deferred_messages_by_timestamp = ViewDefinition('deferred_message_indices', 'by_timestamp', 
'''
function(doc) {
    if (doc.document_types && doc.document_types.indexOf("DeferredAMQPMessage") != 1) {
        emit([doc.claimed, doc.timestamp, doc._id], null);
    }
}
''')