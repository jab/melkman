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
from couchdb.schema import DateTimeField
import logging
import traceback

from giblets import Component, ExtensionInterface, implements
from melkman.context import IRunDuringBootstrap
from melkman.messaging import MessageDispatch
from melkman.scheduler import defer_message

__all__ = ['request_feed_index', 'schedule_feed_index', 'push_feed_index']

log = logging.getLogger(__name__)

INDEX_FEED_COMMAND = 'index_feed'

def request_feed_index(url, context, skip_reschedule=False):
    """
    request that the url specified be fetched and indexed.
    """
    message = {'url': url}
    if skip_reschedule:
        message['skip_reschedule'] = True
    publisher = MessageDispatch(context)
    publisher.send(message, INDEX_FEED_COMMAND)

def schedule_feed_index(url, timestamp, context, message_id=None, skip_reschedule=False):
    """
    request that the url specified be fetched and indexed at a specific time
    in the future.
    """
    message = {'url': url}
    if skip_reschedule:
        message['skip_reschedule'] = True

    if message_id is not None:
        options['message_id'] = message_id

    defer_message(timestamp, message, INDEX_FEED_COMMAND, context)


def push_feed_index(url, content, context, **kw):
    """
    immediately index the content given (identified by its url)
    """
    message = {
        'url': url,
        'content': content
    }
    message.update(kw)

    publisher = MessageDispatch(context)
    publisher.send(message, INDEX_FEED_COMMAND)

class FeedIndexerSetup(Component):
    implements(IRunDuringBootstrap)
    
    def bootstrap(self, context, purge=False):

        log.info("Setting up feed indexing queues...")
        c = MessageDispatch(context)
        c.declare(INDEX_FEED_COMMAND)

        if purge == True:
            log.info("Clearing feed indexing queues...")
            c.clear(INDEX_FEED_COMMAND)

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