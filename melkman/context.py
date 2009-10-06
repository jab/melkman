from ConfigParser import ConfigParser
from carrot.connection import BrokerConnection
from carrot.messaging import Publisher
from copy import deepcopy
from couchdb import ResourceNotFound, Server as CouchDBServer
import logging
import os
import threading
import traceback
from urlparse import urlparse

from giblets import Component, ExtensionInterface, ExtensionPoint, BlacklistComponentManager
from giblets.search import find_plugins_by_entry_point
from melk.util.dibject import Dibject, dibjectify, json_wake
from melk.util.typecheck import is_dicty, asbool

log = logging.getLogger(__name__)

DEFAULTS = {
    'couchdb': {
        'hostname': 'localhost',
        'port': 5984,
        'database': 'melkman'
    },
    'amqp': {
        'broker': {'hostname': 'localhost',
                   'port': 5672,
                   'virtual_host': 'melkman',
                   'userid': 'melkman',
                   'password': 'melkman'},
    },
}

MELKMAN_PLUGIN_ENTRY_POINT = 'melkman_plugins'

class Context(object):
    """
    holds common configuration related data and methods.
    """
    def __init__(self, config):
        self.config = dibjectify(config)
        self._local = threading.local()
        find_plugins_by_entry_point(MELKMAN_PLUGIN_ENTRY_POINT)

    def close(self):
        """
        signal that the caller is finished with the context.
        """
        if hasattr(self._local, 'db'):
            del self._local.db
        
        if hasattr(self._local, 'broker'):
            self.broker.close()
            del self._local.broker

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
        conn = getattr(self._local, 'broker', None)
        if conn is None or conn._closed == True:
            conn = self.create_broker_connection()
            self._local.broker = conn
        return conn
        
    def create_broker_connection(self):
        kargs = dict(self.config.amqp.broker)
        if 'port' in kargs:
            kargs['port'] = int(kargs['port'])
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
    def from_ini(cls, ini_filename, defaults=DEFAULTS):
        cfg_parser = ConfigParser()
        cfg_parser.readfp(open(ini_filename))
        interp = {'here': os.path.abspath(os.path.dirname(ini_filename))}
        config = {}

        for section_name in cfg_parser.sections():
            section = config.setdefault(section_name, {})
            
            for option in cfg_parser.options(section_name):
                cur = section
                ks = option.split('.')
                for k in ks[0:-1]:
                    cur = cur.setdefault(k, {})
                cur[ks[-1]] = cfg_parser.get(section_name, option, 0, interp)

        if defaults is not None:
            _deep_setdefault(config, defaults)

        return cls(config)
    
class IContextConfigurable(ExtensionInterface):
    """
    implement this interface to recieve the current context
    at activation time.
    """

    def set_context(context):
        """
        provides the current context to the component
        """

class IRunDuringBootstrap(ExtensionInterface):
    """
    implement this interface to recieve a notification 
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

class MelkmanComponentManager(BlacklistComponentManager):

    def __init__(self, context):
        BlacklistComponentManager.__init__(self)
        self.context = context
        for component_id, cfg in self.context.config.items():
            enabled = asbool(cfg.get('enabled', True))
            if not enabled:
                self.disable_component(component_id)

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