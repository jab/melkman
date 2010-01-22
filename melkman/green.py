from __future__ import with_statement
from carrot.backends.pyamqplib import Backend as AMQBackend
from eventlet import sleep, with_timeout, TimeoutError, GreenPool
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


def waitall(procs):
   for proc in procs:
       proc.wait()

def killall(procs):
   for proc in procs:
       proc.kill()

class Pool(GreenPool):
    
    def killall(self):
        killall(self.coroutines_running)

