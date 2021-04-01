from pymongo.mongo_client import MongoClient
# from motor.motor_tornado import MotorClient
from soa.services.greenmotor.greenmotor import GreenMotorClient
from pymongo.read_preferences import ReadPreference
import collections

__all__ = ['ConnectionError', 'connect', 'set_default_db', 'SlaveOkSettings']

MongoConnections = collections.namedtuple('MongoConnections',
                                           ['sync', 'async'])

SlaveOkSettings = collections.namedtuple('SlaveOkSettings',
                                         ['read_pref', 'tags'])

_connections = {}
_dbs = {}
_db_to_conn = {}
_default_db = 'sweeper'
_slave_ok_settings = {
    False: SlaveOkSettings(ReadPreference.PRIMARY_PREFERRED, [{}]),
    True: SlaveOkSettings(ReadPreference.SECONDARY_PREFERRED, [{}])
}

_proxy_clients = {}

_proxy_dbs_to_conn = {}
_proxy_connections = {}

class OpClass(object):
    READ = 0
    WRITE = 1

    @classmethod
    def all(cls):
        return set([cls.READ, cls.WRITE])

# Default to off if haven't been inited
_proxy_decider_keys = {OpClass.READ  : lambda : False,
                       OpClass.WRITE : lambda : False}

def inject_decider(opclass, func):
    if opclass not in OpClass.all():
        raise Exception('Not a valid OpClass')

    if not callable(func):
        raise Exception('Func must be a callable')

    global _proxy_decider_keys
    _proxy_decider_keys[opclass] = func

class ConnectionError(Exception):
    pass

def _get_proxy_decider(opclass):
    global _proxy_decider_keys
    return _proxy_decider_keys[opclass]()

def _get_proxy_client(db_name='test'):
    global _proxy_clients, _proxy_dbs_to_conn, _proxy_connections
    if not db_name:
        db_name = _default_db

    if db_name not in _proxy_clients:
        if not _proxy_dbs_to_conn:
            conn_name = None
        else:
            if db_name not in _proxy_dbs_to_conn:
                return None
            conn_name = _proxy_dbs_to_conn[db_name]

        if conn_name not in _proxy_connections:
            return None

        conn = _proxy_connections[conn_name]
        _proxy_clients[db_name] = conn

    client = _proxy_clients[db_name]
    return client()


def _get_db(db_name='test', reconnect=False, allow_async=True):
    global _dbs, _connections, _db_to_conn

    if not db_name:
        db_name = _default_db

    if db_name not in _dbs:
        if not _db_to_conn:
            conn_name = None

        else:
            if db_name not in _db_to_conn:
                return None

            conn_name = _db_to_conn[db_name]

        if conn_name not in _connections:
            return None

        conn = _connections[conn_name]
        _dbs[db_name] = (conn.sync[db_name],
                         conn.async[db_name] if conn.async else None)

    sync, async = _dbs[db_name]

    return async if allow_async and async else sync

def _get_slave_ok(slave_ok):
    return _slave_ok_settings[slave_ok]

def connect_proxy(client_func, conn_name=None, db_names=None):
    global _proxy_dbs_to_conn, _proxy_connections
    if conn_name not in _proxy_connections:
        _proxy_connections[conn_name] = client_func
    if db_names:
        for db in db_names:
            _proxy_dbs_to_conn[db] = conn_name
    return _proxy_connections[conn_name]

def connect(host='localhost', conn_name=None, db_names=None, allow_async=False,
            slave_ok_settings=None, **kwargs):
    global _connections, _db_to_conn, _slave_ok_settings

    # Connect to the database if not already connected
    if conn_name not in _connections:
        try:
            pool_size = kwargs.pop('max_pool_size', None)
            if pool_size:
                kwargs['maxPoolSize'] = pool_size

            if allow_async:
                async_conn = GreenMotorClient(host, **kwargs)
            else:
                async_conn = None

            sync_conn = MongoClient(host, **kwargs)

            _connections[conn_name] = MongoConnections(sync_conn, async_conn)
        except Exception as e:
            raise ConnectionError('Cannot connect to the database: %s' % str(e))

        if db_names:
            for db in db_names:
                _db_to_conn[db] = conn_name

        if slave_ok_settings:
            _slave_ok_settings = slave_ok_settings

    return _connections[conn_name]

def set_default_db(db):
    global _default_db

    _default_db = db
