from pymongo import Connection
import multiprocessing

__all__ = ['ConnectionError', 'connect']


_connection_defaults = {
    'host': 'localhost:27017',
    'db_name': 'test'
}

_connections = {}
_connection_settings = { }
_dbs = {}


class ConnectionError(Exception):
    pass


def _get_connection(conn_name=None, reconnect=False):
    global _connections, _connection_settings
    identity = get_identity()

    if conn_name not in _connection_settings:
        return None

    if conn_name not in _connections:
        _connections[conn_name] = {}

    # Connect to the database if not already connected
    if _connections[conn_name].get(identity) is None or reconnect:
        try:
            _connections[conn_name][identity] = Connection(_connection_settings[conn_name]['host'])
        except:
            raise ConnectionError('Cannot connect to the database')
    return _connections[conn_name][identity]

def _get_db(conn_name=None, reconnect=False):
    global _dbs, _connections, _connection_settings
    identity = get_identity()

    if conn_name not in _connection_settings:
        return None

    if conn_name not in _dbs:
        _dbs[conn_name] = {}

    if identity not in _dbs[conn_name]:
        settings = _connection_settings[conn_name]

        conn = _get_connection(conn_name, reconnect)

        db = conn[settings['db_name']]

        if 'db_username' in settings and 'db_password' in settings:
            authenticated = db.authenticate(settings['db_username'], settings['db_password'])
            # make sure authentication passes.
            if not authenticated:
                raise ConnectionError('Authentication failed with username %s', settings['db_username'])

        _dbs[conn_name][identity] = db

    return _dbs[conn_name][identity]

def get_identity():
    identity = multiprocessing.current_process()._identity
    identity = 0 if not identity else identity[0]
    return identity

def connect(db, username=None, password=None, conn_name=None, **kwargs):
    """Connect to the database specified by the 'db' argument. Connection
    settings may be provided here as well if the database is not running on
    the default port on localhost. If authentication is needed, provide
    username and password arguments as well.
    """
    global _connection_settings
    _connection_settings[conn_name] = _connection_defaults.copy()
    _connection_settings[conn_name].update(kwargs)
    _connection_settings[conn_name]['db_name'] = db

    if username:
        _connection_settings[conn_name]['db_username'] = username
    if password:
        _connection_settings[conn_name]['db_password'] = username

    return _get_db(conn_name, reconnect=True)
