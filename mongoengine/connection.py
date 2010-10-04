from pymongo import Connection
import multiprocessing

__all__ = ['ConnectionError', 'connect']


_connection_defaults = {
    'host': 'localhost',
    'port': 27017,
}
_connection = {}
_connection_settings = _connection_defaults.copy()

_db_name = None
_db_username = None
_db_password = None
_db = {}


class ConnectionError(Exception):
    pass


def _get_connection(reconnect=False):
    global _connection
    identity = get_identity()
    # Connect to the database if not already connected
    if _connection.get(identity) is None or reconnect:
        try:
            _connection[identity] = Connection(**_connection_settings)
        except:
            raise ConnectionError('Cannot connect to the database')
    return _connection[identity]

def _get_db(reconnect=False):
    global _db, _connection
    identity = get_identity()
    # Connect if not already connected
    if _connection.get(identity) is None or reconnect:
        _connection[identity] = _get_connection(reconnect=reconnect)

    if _db.get(identity) is None or reconnect:
        # _db_name will be None if the user hasn't called connect()
        if _db_name is None:
            raise ConnectionError('Not connected to the database')

        # Get DB from current connection and authenticate if necessary
        _db[identity] = _connection[identity][_db_name]
        if _db_username and _db_password:
            _db[identity].authenticate(_db_username, _db_password)

    return _db[identity]

def get_identity():
    identity = multiprocessing.current_process()._identity
    identity = 0 if not identity else identity[0]
    return identity
    
def connect(db, username=None, password=None, **kwargs):
    """Connect to the database specified by the 'db' argument. Connection 
    settings may be provided here as well if the database is not running on
    the default port on localhost. If authentication is needed, provide
    username and password arguments as well.
    """
    global _connection_settings, _db_name, _db_username, _db_password, _db
    _connection_settings = dict(_connection_defaults, **kwargs)
    _db_name = db
    _db_username = username
    _db_password = password
    return _get_db(reconnect=True)

