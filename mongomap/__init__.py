import document
from document import *
import fields
from fields import *

from pymongo import Connection

__all__ = document.__all__ + fields.__all__ + ['connect']

__author__ = 'Harry Marr'
__version__ = '0.1'

_connection_settings = {
    'host': 'localhost',
    'port': 27017,
    'pool_size': 1,
}
_connection = None
_db = None

def _get_connection():
    if _connection is None:
        _connection = Connection(**_connection_settings)
    return _connection

def connect(db=None, username=None, password=None, **kwargs):
    """Connect to the database specified by the 'db' argument. Connection 
    settings may be provided here as well if the database is not running on
    the default port on localhost. If authentication is needed, provide
    username and password arguments as well.
    """
    if db is None:
        raise TypeError('"db" argument must be provided to connect()')

    _connection_settings.update(kwargs)
    connection = _get_connection()
    # Get DB from connection and auth if necessary
    _db = connection[db]
    if username is not None and password is not None:
        _db.authenticate(username, password)
