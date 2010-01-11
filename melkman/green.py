from __future__ import with_statement
from carrot.backends.pyamqplib import Backend as AMQBackend
from eventlet.api import sleep, timeout, TimeoutError
from greenamqp.client_0_8 import Connection as GreenConnection
import logging
import traceback

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


def timeout_wait(event, timeout_secs, default=None):
    """
    wait for the event given to be triggered 
    for at most timeout_secs seconds.  
    
    The caller may be woken by the timeout or the 
    event being triggered for other reasons.  The 
    function returns either the value of the event
    or the default value given in the case of a 
    timeout.
    """
    try:
        with timeout(timeout_secs):
            return event.wait()
    except TimeoutError:
        return default