from pymongo.mongo_client import MongoClient
from pymongo.green import GreenletClient
import collections

__all__ = ['ConnectionError', 'connect', 'set_default_db']

_connections = {}
_dbs = {}
_db_to_conn = {}
_default_db = 'sweeper'

class ConnectionError(Exception):
    pass

MONGO_CONNECTIONS = collections.namedtuple('MONGO_CONNECTIONS',
                                           ['sync', 'async'])


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


def connect(host='localhost', conn_name=None, db_names=None, allow_async=False, **kwargs):
    global _connections, _db_to_conn

    # Connect to the database if not already connected
    if conn_name not in _connections:
        try:
            if allow_async:
                async_conn = GreenletClient.sync_connect(host, **kwargs)
            else:
                async_conn = None

            sync_conn = MongoClient(host, **kwargs)

            _connections[conn_name] = MONGO_CONNECTIONS(sync_conn, async_conn)
        except Exception as e:
            raise ConnectionError('Cannot connect to the database: %s' % str(e))

        if db_names:
            for db in db_names:
                _db_to_conn[db] = conn_name

    return _connections[conn_name]

def set_default_db(db):
    global _default_db

    _default_db = db
