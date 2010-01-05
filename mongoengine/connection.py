from pymongo import Connection


__all__ = ['ConnectionError', 'connect']


_connection_settings = {
    'host': 'localhost',
    'port': 27017,
    'pool_size': 1,
}
_connection = None
_db = None


class ConnectionError(Exception):
    pass


def _get_connection():
    global _connection
    if _connection is None:
        _connection = Connection(**_connection_settings)
    return _connection

def _get_db():
    global _db
    if _db is None:
        raise ConnectionError('Not connected to database')
    return _db

def connect(db, username=None, password=None, **kwargs):
    """Connect to the database specified by the 'db' argument. Connection 
    settings may be provided here as well if the database is not running on
    the default port on localhost. If authentication is needed, provide
    username and password arguments as well.
    """
    global _db

    _connection_settings.update(kwargs)
    connection = _get_connection()
    # Get DB from connection and auth if necessary
    _db = connection[db]
    if username is not None and password is not None:
        _db.authenticate(username, password)

