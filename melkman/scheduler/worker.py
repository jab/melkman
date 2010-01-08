from carrot.messaging import Publisher
from copy import deepcopy
from couchdb import ResourceConflict, ResourceNotFound
from couchdb.schema import DateTimeField
from datetime import datetime, timedelta
from eventlet.api import sleep
from eventlet.coros import event
from eventlet.proc import spawn as spawn_proc, waitall, killall, ProcExit
from giblets import Component, implements
import logging 
import traceback

from melkman.green import resilient_consumer_loop, timeout_wait
from melkman.messaging import MessageDispatch, always_ack
from melkman.scheduler.api import DeliveryOptions, DeferredAMQPMessage, view_deferred_messages_by_timestamp
from melkman.scheduler.api import SCHEDULER_COMMAND, DEFER_MESSAGE_COMMAND, CANCEL_MESSAGE_COMMAND
from melkman.worker import IWorkerProcess

log = logging.getLogger(__name__)

_COMMANDS = {}

def _handle_scheduler_command(message_data, message, context):
    """
    main message handler for the schedule message service
    """
    
    try:
        # dispatch to appropriate handler based on the 'command'
        # field of the message.
        cmd = message_data.get('command', None)
        
        if cmd is None:
            log.warn('No command specified in message request, ignoring: %s' % message_data)
            return
        
        cmd_handler = _COMMANDS.get(cmd, None)
        if cmd_handler is None:
            log.warn('Ignoring message with unknown command: %s' % cmd)
            return
        else:
            cmd_handler(message_data, message, context)

    except:
        log.error("Fatal client error handling message %s: %s" % (message_data, traceback.format_exc()))
        raise


def _handle_defer_command(message_data, message, context):
    """
    handle the DEFER_MESSAGE command.
    """
    mid = message_data.get('message_id', None)
    if mid is not None:
        deferred = DeferredAMQPMessage.lookup_by_message_id(context.db, mid)
        if deferred is None:
            # create new (with id)
            deferred = DeferredAMQPMessage.create_from_message_id(mid)
        else:
            # if it is already in progress, too late for modification..
            if deferred.claimed:
                log.warn("Ignoring update to in progress message %s" % message_data)
                return
    else:
        # create new (anonymous)
        deferred = DeferredAMQPMessage()

    try:
        # fill in data
        deferred.timestamp = DateTimeField()._to_python(message_data['timestamp'])
        deferred.options.exchange = message_data['exchange']
        deferred.options.routing_key = message_data['routing_key']
        if 'delivery_mode' in message_data:
            deferred.options.delivery_mode = int(message_data['delivery_mode'])
            if not deferred.options.delivery_mode in (1, 2):
                raise ValueError("Bad delivery mode: %s" % message_data['delivery_mode'])
        if 'mandatory' in message_data:
            deferred.options.mandatory = bool(message_data['mandatory'])
        if 'priority' in message_data:
            deferred.options.priority = int(message_data['priority'])
            if not defferred.options.priority in xrange(0, 10):
                raise ValueError("Bad priority: %s" % deferred.options.priority)
        deferred.message = deepcopy(message_data['message'])
    except:
        log.warn("Ignoring ill formatted request %s: %s" % (message_data, traceback.format_exc()))
        return

    try:
        deferred.store(context.db)
        log.info("scheduled message %s for delivery at %s" % (deferred.id, deferred.timestamp))
    except ResourceConflict:
        log.warn("Conflict re-storing message %s! Assuming not problematic..." % message_data)
    except ResourceNotFound:
        log.warn("Not found re-storing message %s! Assuming already processed..." % message_data)
_COMMANDS[DEFER_MESSAGE_COMMAND] = _handle_defer_command

def _handle_cancel_command(message_data, message, context):
    mid = message_data.get('message_id', None)
    if mid is None:
        log.warn("Ignoring cancel command with no message id: %s" % message_data)
        return

    deferred = DeferredAMQPMessage.lookup_by_message_id(mid)
    if deferred is None:
        log.warn("Ignorning cancel for missing message %s, already processed?" % message_data)
        return

    # claim it so that nobody will start it.
    if not deffered.claim(context.db):
        log.warn("Ignoring cancel for in progress message %s" % message_data)
        return

    try:
        del self.context.db[deferred._id]
    except ResourceNotFound:
        log.warn("Deferred message was destroyed by other means before cancelled: %s" % message_data)
        return
_COMMANDS[CANCEL_MESSAGE_COMMAND] = _handle_cancel_command


class ScheduledMessageService(object):

    MIN_SLEEP_TIME = timedelta(seconds=1)
    MAX_SLEEP_TIME = timedelta(minutes=60)
    MAX_CLAIM_TIME = timedelta(minutes=5)

    def __init__(self, context):
        self.context = context
        self.service_queue = event()
        self._listener = None
        self._dispatch = None

    def run(self):
        try:
            self._listener = self._start_listener()
            self._dispatcher = spawn_proc(self.run_dispatcher)
        
            procs = [self._listener, self._dispatcher]
            waitall(procs)
        except ProcExit:
            killall(procs)

    ################################################################
    # The listener consumes messages on the scheduled message queue 
    # and stores the deferred messages in the database.
    ################################################################

    def _start_listener(self):
        @always_ack
        def cb(message_data, message):
            _handle_scheduler_command(message_data, message, self.context)
            self.wakeup_dispatcher()
        
        dispatch = MessageDispatch(self.context)
        return dispatch.start_worker(SCHEDULER_COMMAND, cb)


    ##############################################################
    # The dispatcher consumes deferred messages from the database 
    # when their scheduled time arrives and spits them out 
    # to the message broker
    ##############################################################    
    def run_dispatcher(self):
        # cleanup any mess left over last time...
        self.cleanup()
        while(True):
            log.info("checking for ready messages...")
            last_time = self.send_ready_messages()
            sleep_time = self._calc_sleep(last_time)
            log.info("sleeping for %s" % sleep_time)
            
            sleep_secs = sleep_time.days*84600 + sleep_time.seconds
            timeout_wait(self.service_queue, sleep_secs)
            
            if self.service_queue.ready():
                self.service_queue.reset()

    def wakeup_dispatcher(self):
        if not self.service_queue.ready():
            self.service_queue.send(True)

    def _calc_sleep(self, after=None):
        next_time = self.find_next_send_time(after=after)
    
        if next_time is None:
            sleep_time = self.MAX_SLEEP_TIME
        else:
            sleep_time = next_time - datetime.utcnow()
            sleep_time += timedelta(seconds=1)
            sleep_time -= timedelta(microseconds=sleep_time.microseconds)

        if sleep_time < self.MIN_SLEEP_TIME:
            sleep_time = self.MIN_SLEEP_TIME
        if sleep_time > self.MAX_SLEEP_TIME:
            sleep_time = self.MAX_SLEEP_TIME        
        
        return sleep_time

    def find_next_send_time(self, after=None):
        if after is None:
            after = datetime.utcnow()
        after_str = DateTimeField()._to_json(after)

        next_query = dict(
            startkey = [False, after_str, {}],
            endkey = [True, None],
            include_docs = False,
            descending = False,
            limit = 1
        )

        next_send = None
        for r in view_deferred_messages_by_timestamp(self.context.db, **next_query):
            next_send = DateTimeField()._to_python(r.key[1])
            break

        return next_send

    def send_ready_messages(self):
        while True:
            now = datetime.utcnow()
            now_str = DateTimeField()._to_json(now)

            query = dict(
                startkey = [False, None],
                endkey = [False, now_str, {}],
                include_docs = True,
                descending = False,
                limit = 100
            )


            vr = view_deferred_messages_by_timestamp(self.context.db, **query)
            batch = []
            for r in vr:
                batch.append(DeferredAMQPMessage.wrap(r.doc))

            if len(batch) == 0:
                break
            
            dispatch_count = 0
            for message in batch:
                try:
                    if self._dispatch_message(message):
                        dispatch_count += 1
                except ProcExit:
                    # asked to stop, go ahead and quit.
                    raise
                except:
                    log.error("Unexected error dispatching message %s: %s" %
                              (message, traceback.format_exc()))
                    
            log.info("Dispatched %d messages" % dispatch_count)
            
        return now

    def _dispatch_message(self, message):
        if not message.claim(self.context.db):
            return
        
        try:
            publisher = Publisher(self.context.broker, exchange=message.options.exchange)
            publisher.send(message.message,
                           routing_key = message.options.routing_key,
                           delivery_mode = message.options.delivery_mode,
                           mandatory = message.options.mandatory,
                           priority = message.options.priority)
            publisher.close()
        except:
            log.error("Error dispatching deferred message %s: %s" % (message, traceback.format_exc()))
            self.error_reschedule(message)
            return False
        else:
            log.debug("Dispatched message %s" % message)
            # sent with no problems, done with it.
            self.context.db.delete(message)
            return True

    def error_reschedule(self, message):
        message.error_count += 1
        
        if message.error_count < 10:
            delay = 2**message.error_count
        else:
            delay = 60*10

        resched_time = datetime.utcnow() + timedelta(seconds=delay)
        message.unclaim(self.context.db, resched_time)
        
        log.warn("Rescheduled message %s for %s" % (message.id, resched_time))

    def cleanup(self):
        log.info("Performing cleanup of claimed items...")

        # anything older than this has held the claim for too long
        # and is considered dead.
        cutoff = datetime.utcnow() - self.MAX_CLAIM_TIME
        cutoff_str = DateTimeField()._to_json(cutoff)

        query = dict(
            startkey = [True, cutoff_str, {}],
            endkey = [True],
            limit = 100,
            include_docs = True,
            descending = True
        )

        unclaim_count = 0
        while(True):
            vr = view_deferred_messages_by_timestamp(self.context.db, **query)
            batch = [DeferredAMQPMessage.wrap(r.doc) for r in vr]
            if len(batch) == 0:
                break

            for message in batch:
                self.error_reschedule(message)
                unclaim_count += 1

        if unclaim_count > 0:
            log.warn('Cleanup unclaimed %d items' % unclaim_count)


class ScheduleMessageServiceProcess(Component):
    implements(IWorkerProcess)

    def run(self, context):
        sms = ScheduledMessageService(context)
        sms.run()