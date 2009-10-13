from __future__ import with_statement
from carrot.backends.pyamqplib import Backend as AMQBackend
from carrot.connection import BrokerConnection
from eventlet.api import sleep, timeout, TimeoutError, getcurrent
from eventlet.corolocal import get_ident, local as green_local
from eventlet.coros import event
from eventlet.proc import ProcExit
from greenamqp.client_0_8 import Connection as GreenConnection
import logging
import traceback

from melkman.context import Context

log = logging.getLogger(__name__)

def green_init():
    from eventlet.util import wrap_socket_with_coroutine_socket
    wrap_socket_with_coroutine_socket()

class GreenAMQPBackend(AMQBackend):
    
    def establish_connection(self):
        """Establish connection to the AMQP broker."""
        conninfo = self.connection
        if not conninfo.port:
            conninfo.port = self.default_port
        return GreenConnection(host=conninfo.host,
                               userid=conninfo.userid,
                               password=conninfo.password,
                               virtual_host=conninfo.virtual_host,
                               insist=conninfo.insist,
                               ssl=conninfo.ssl,
                               connect_timeout=conninfo.connect_timeout)

class GreenContext(Context):

    def __init__(self, *args, **kw):
       Context.__init__(self, *args, **kw)
       self._local = green_local()
       
    def close(self):
        Context.close(self)
        # XXX fix this better...
        try:
            del self._local.__dict__['__objs'][get_ident()]
        except KeyError:
            pass

    def create_broker_connection(self):
        kargs = dict(self.config.amqp)
        if 'port' in kargs:
            kargs['port'] = int(kargs['port'])
        kargs['backend_cls'] = GreenAMQPBackend
        return BrokerConnection(**kargs)


        from eventlet.api import sleep
        import logging 
        import traceback

        log = logging.getLogger(__name__)

def consumer_loop(make_consumer, context):
    dead = False
    consumer = None

    try:
        consumer = make_consumer(context)
        it = consumer.iterconsume()
        while it.next():
            pass
    except ProcExit:
        log.debug("consumer_loop: killed")
        dead = True
        if consumer:
            consumer.close()
        context.close()
    finally:
        if not dead:
            if consumer:
                consumer.close()
            context.close()

def resilient_consumer_loop(make_consumer, context):
    seq_reconnects = 0
    total_reconnects = 0
    dead = False
    consumer = None
    while True:
        try:
            consumer = make_consumer(context)
            it = consumer.iterconsume()
            while it.next():
                seq_reconnects = 0
        except ProcExit:
            log.debug("resilient_consumer_loop: killed")
            dead = True
            consumer.close()
            context.close()
        except:
            log.error("Error consuming message: %s" % traceback.format_exc())
        finally:
            if not dead:
                if consumer is not None:
                    consumer.close()
                context.close()
                total_reconnects += 1
                seq_reconnects += 1
                if seq_reconnects < 10:
                    sleep_time = 2**seq_reconnects
                else:
                    sleep_time = 60*10

                log.error("Reconnect #%d (%dth in a row), sleeping for %d seconds..." % (total_reconnects, seq_reconnects, sleep_time))
                sleep(sleep_time)

def timeout_wait(event, timeout_secs):
    """
    trigger the event given in timeout seconds and 
    wait on the event. the caller may be woken by 
    the timeout or the event being triggered for
    other reasons.
    """
    try:
        with timeout(timeout_secs):
            event.wait()
    except TimeoutError:
        pass

