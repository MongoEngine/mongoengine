from pymongo import Connection


__all__ = ['ConnectionError', 'connect']


_connection_settings = {
    'host': 'localhost',
    'port': 27017,
}
_connection = None

_db_name = None
_db_username = None
_db_password = None
_db = None


class ConnectionError(Exception):
    pass


def _get_connection():
    global _connection
    # Connect to the database if not already connected
    if _connection is None:
        try:
            _connection = Connection(**_connection_settings)
        except:
            raise ConnectionError('Cannot connect to the database')
    return _connection

def _get_db():
    global _db, _connection
    # Connect if not already connected
    if _connection is None:
        _connection = _get_connection()

    if _db is None:
        # _db_name will be None if the user hasn't called connect()
        if _db_name is None:
            raise ConnectionError('Not connected to the database')

        # Get DB from current connection and authenticate if necessary
        _db = _connection[_db_name]
        if _db_username and _db_password:
            _db.authenticate(_db_username, _db_password)

    return _db

def connect(db, username=None, password=None, **kwargs):
    """Connect to the database specified by the 'db' argument. Connection 
    settings may be provided here as well if the database is not running on
    the default port on localhost. If authentication is needed, provide
    username and password arguments as well.
    """
    global _connection_settings, _db_name, _db_username, _db_password
    _connection_settings.update(kwargs)
    _db_name = db
    _db_username = username
    _db_password = password
    return _get_db()