from melkman.green import green_init
green_init()

from yaml import load as load_yaml
from carrot.connection import BrokerConnection
from carrot.messaging import Publisher
from copy import deepcopy
from couchdb import ResourceNotFound, Server as CouchDBServer
from eventlet.corolocal import get_ident, local as green_local
import logging
import os
import threading
import traceback
from urlparse import urlparse

from giblets import Component, ExtensionInterface, ExtensionPoint, PatternComponentManager
from giblets.search import find_plugins_by_entry_point
from melk.util.dibject import Dibject, dibjectify, json_wake
from melk.util.typecheck import is_dicty, asbool

from melkman.green import GreenAMQPBackend

log = logging.getLogger(__name__)

DEFAULTS = {
    'couchdb': {
        'hostname': 'localhost',
        'port': 5984,
        'database': 'melkman'
    },
    'amqp': {
        'hostname': 'localhost',
        'port': 5672,
        'virtual_host': 'melkman',
        'userid': 'melkman',
        'password': 'melkman'
    },
}

MELKMAN_PLUGIN_ENTRY_POINT = 'melkman_plugins'


class Context(object):
    """
    holds common configuration and greenlet-safe references to
    resources such as database and message broker connections.
    
    context resources should be used inside a 'with' block to 
    insure that resources are properly allocated and disposed of
    when greenlets exit, eg:
    
    with context:
        # ... do things that require resources
        del context.db['some_item']

    it is okay to use this construct repeatedly, eg:
    
    def foo(context):
        with context: 
            bar(context)
            # ... other stuff
    
    def bar(context):
        with context:
            # ... do stuff
    
    """
    def __init__(self, config):
        self.config = dibjectify(config)
        self._local = green_local()
        find_plugins_by_entry_point(MELKMAN_PLUGIN_ENTRY_POINT)
        self._broker = None

    def __enter__(self):
        self._refcount += 1

    def __exit__(self, type, value, traceback):
        self._refcount -= 1
        assert self._refcount >= 0
        if self._refcount == 0:
            self._close()

        return False # do not suppress exception

    def _get_refcount(self):
        if not hasattr(self._local, 'refcount'):
            self._local.refcount = 0
        return self._local.refcount
    def _set_refcount(self, val):
        self._local.refcount = val
    _refcount = property(_get_refcount, _set_refcount)


    def _close(self):
        self._local_close()
        # check for global no reference condition
        if len(self._locals_by_greenlet) == 0:
            self._shared_close()

    def _local_close(self, greenlet_id=None):
        """
        called when a greenlet is finished with the context, 
        disposes of local resources.
        """

        # if no greenlet is specified, use the calling greenlet
        if greenlet_id is None:
            greenlet_id = get_ident()

        # remove the local storage references for the greenlet
        try:
            del self._locals_by_greenlet[greenlet_id]
        except KeyError:
            pass

    def _shared_close(self):
        """
        closes shared resources
        """
        if self._broker is not None:
            old_broker = self._broker
            self._broker = None
            try:
                old_broker.close()
            except:
                log.error("Error closing broker connection: %s" % traceback.format_exc())


    @property
    def _locals_by_greenlet(self):
        # XXX ugly, fix this better...
        return object.__getattribute__(self._local, '__dict__').setdefault('__objs', {})

    #######################
    # Database
    #######################
    @property
    def db(self):
        if not hasattr(self._local, 'db'):
            self._local.db = self.create_db_connection()
        return self._local.db

    def create_db_connection(self):
        try:
            server = self.create_db_server()
            return server[self.config.couchdb.database]
        except ResourceNotFound:
            log.error("Cannot find database %s on server %s, has it been bootstrapped yet?" % 
                      (self.config.couchdb.database, self.db_server_address))
            raise
        except:
            log.error("Error connecting to database %s on server %s, has it been bootstrapped yet?: %s" % 
                      (self.config.couchdb.database, self.db_server_address, traceback.format_exc()))
            raise

    @property
    def db_server_address(self):
        return 'http://%s:%d' % (self.config.couchdb.hostname, int(self.config.couchdb.port))
    
    def create_db_server(self):
        server_address = self.db_server_address
        return CouchDBServer(server_address)

    ######################
    # AMQP 
    ######################
    @property
    def broker(self):
        if self._broker is None:
            self._broker = self.create_broker_connection()
        return self._broker

    def create_broker_connection(self):
        kargs = dict(self.config.amqp)
        if 'port' in kargs:
            kargs['port'] = int(kargs['port'])
        kargs['backend_cls'] = GreenAMQPBackend
        return BrokerConnection(**kargs)


    ##################################
    # Components
    ##################################
    
    @property
    def component_manager(self):
        if not hasattr(self._local, 'component_manager'):
            self._local.component_manager = MelkmanComponentManager(self)
        return self._local.component_manager

    ##################################
    # Setup
    ##################################
    def bootstrap(self, purge=False):
        from melkman.db import bootstrap as bootstrap_database

        server = self.create_db_server()
        db_name = self.config.couchdb.database

        if purge == True and db_name in server:
            del server[db_name]

        if not db_name in server:
            server.create(db_name)
    
        bootstrap_database(server[db_name])

        # okay, got the basics, now bootstrap any 
        # plugins in the context that support it.
        PluginBootstrapper(self.component_manager).bootstrap(self, purge=purge)

    ##################################
    # Initializers 
    ##################################

    @classmethod
    def from_dict(cls, dict, defaults=DEFAULTS):
        cfg = deepcopy(dict)
        if defaults is not None:
            _deep_setdefault(cfg, defaults)
        return cls(cfg)

    @classmethod
    def from_json(cls, json_string, defaults=DEFAULTS):
        cfg = json_wake(json_string)
        if defaults is not None:
            _deep_setdefault(cfg, defaults)
        return cls(cfg)

    @classmethod
    def from_yaml(cls, yaml_filename, defaults=DEFAULTS):
        yaml_file = open(yaml_filename, 'r')
        cfg = load_yaml(yaml_file)
        if defaults is not None:
            _deep_setdefault(cfg, defaults)
        return cls(cfg)

class IContextConfigurable(ExtensionInterface):
    """
    implement this interface to receive the current context
    at activation time.
    """

    def set_context(context):
        """
        provides the current context to the component
        """

class IRunDuringBootstrap(ExtensionInterface):
    """
    implement this interface to receive a notification 
    when the context is being bootstrapped or upgraded.
    """
    
    def bootstrap(context, purge=False):
        """
        called when a context is bootstrapped to 
        perform any pre-use actions like database 
        construction, setup or migrations. 
        
        If purge is True, then any existing information 
        for this plugin in the context should be 
        discarded before setting up.  Otherwise, the
        plugin may retain existing information and 
        upgrade at it's option.
        """

class PluginBootstrapper(Component):
    
    plugins = ExtensionPoint(IRunDuringBootstrap)
    
    def bootstrap(self, context, purge=False):
        for plugin in self.plugins:
            plugin.bootstrap(context, purge=purge)

class MelkmanComponentManager(PatternComponentManager):

    def __init__(self, context):
        PatternComponentManager.__init__(self)
        self.context = context

        for cfg in self.context.config.get('plugins', []):
            self.append_pattern(cfg.pattern, cfg.enabled)

    def component_activated(self, component):
        if IContextConfigurable.providedBy(component):
            component.set_context(self.context)

def _deep_setdefault(d, defaults):
    """
    >>> d = {'a': 1}
    >>> defaults = {'a': 0, 'b': 0}
    >>> _deep_setdefault(d, defaults)
    >>> d['a'] == 1
    True
    >>> d['b'] == 0
    True
    
    >>> d = {'a': {'b': 1}}
    >>> defaults = {'a': {'b': 0, 'c': 0}}
    >>> _deep_setdefault(d, defaults)
    >>> d['a']['b'] == 1
    True
    >>> d['a']['c'] == 0
    True
    
    >>> d = {'a': 'foo'}
    >>> defaults = {'a': {'b': 0}}
    >>> _deep_setdefault(d, defaults)
    >>> d['a'] == 'foo'
    True

    >>> d = {'a': {'b': 0}}
    >>> defaults = {'a': 'foo'}
    >>> _deep_setdefault(d, defaults)
    >>> d['a']['b'] == 0
    True

    """
    for k, v in defaults.items():
        if not k in d:
            d[k] = deepcopy(v)
        else:
            if is_dicty(d[k]) and is_dicty(defaults[k]):
                _deep_setdefault(d[k], defaults[k])

if __name__ == '__main__':
    import doctest
    doctest.testmod()

