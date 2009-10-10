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

from datetime import datetime, timedelta
from carrot.messaging import Consumer, Publisher
from couchdb.schema import DateTimeField
import logging
import traceback

from giblets import Component, ExtensionInterface, implements
from melkman.context import IRunDuringBootstrap
from melkman.scheduler import defer_message

__all__ = ['request_feed_index', 'schedule_feed_index',
           'push_feed_index', 'FeedIndexerConsumer',
           'FeedIndexerPublisher']

log = logging.getLogger(__name__)

INDEX_FEED_EXCHANGE = 'melkman.direct'
INDEX_FEED_COMMAND = 'index_feed'
INDEX_FEED_QUEUE = 'index_feed'

class FeedIndexerPublisher(Publisher):
    exchange = INDEX_FEED_EXCHANGE
    routing_key = INDEX_FEED_COMMAND
    delivery_mode = 2
    mandatory = True

class FeedIndexerConsumer(Consumer):
    exchange = INDEX_FEED_EXCHANGE
    routing_key = INDEX_FEED_COMMAND
    queue = INDEX_FEED_QUEUE
    durable = True

def request_feed_index(url, context, priority=0, skip_reschedule=False):
    message = {'url': url}
    if skip_reschedule:
        message['skip_reschedule'] = True
    publisher = FeedIndexerPublisher(context.broker)
    publisher.send({'url': url}, priority=priority)
    publisher.close()

def schedule_feed_index(url, timestamp, context, message_id=None, skip_reschedule=False):
    message = {'url': url}
    if skip_reschedule:
        message['skip_reschedule'] = True

    options = dict(delivery_mode=2,
                  mandatory=True)

    if message_id is not None:
        options['message_id'] = message_id

    defer_message(timestamp, message, INDEX_FEED_QUEUE, INDEX_FEED_EXCHANGE, context, **options)


def push_feed_index(url, content, context, **kw):
    message = {
        'url': url,
        'content': content
    }
    message.update(kw)

    publisher = FeedIndexerPublisher(context.broker)
    publisher.send(message)
    publisher.close()

class FeedIndexerSetup(Component):
    implements(IRunDuringBootstrap)
    
    def bootstrap(self, context, purge=False):

        log.info("Setting up feed indexing queues...")
        c = FeedIndexerConsumer(context.broker)
        c.close()
        context.broker.close()

        if purge == True:
            log.info("Clearing feed indexing queues...")
            cnx = context.broker
            backend = cnx.create_backend()
            backend.queue_purge(INDEX_FEED_QUEUE)

class PostIndexAction(ExtensionInterface):

    def feed_reindexed(feed, context):
        """
        called after a feed has been updated.
        """

class IndexRequestFilter(ExtensionInterface):

    def accepts_request(feed, request, context):
        """
        called before an index request is processed. 
        if this returns False, the request will 
        be rejected.
        """
