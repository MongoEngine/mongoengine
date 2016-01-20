from pymongo import MongoClient, ReadPreference, uri_parser
from mongoengine.python_support import IS_PYMONGO_3

__all__ = ['ConnectionError', 'connect', 'register_connection',
           'DEFAULT_CONNECTION_NAME']


DEFAULT_CONNECTION_NAME = 'default'
if IS_PYMONGO_3:
    READ_PREFERENCE = ReadPreference.PRIMARY
else:
    from pymongo import MongoReplicaSetClient
    READ_PREFERENCE = False


class ConnectionError(Exception):
    pass


_connection_settings = {}
_connections = {}
_dbs = {}


def register_connection(alias, name=None, host=None, port=None,
                        read_preference=READ_PREFERENCE,
                        username=None, password=None, authentication_source=None,
                        **kwargs):
    """Add a connection.

    :param alias: the name that will be used to refer to this connection
        throughout MongoEngine
    :param name: the name of the specific database to use
    :param host: the host name of the :program:`mongod` instance to connect to
    :param port: the port that the :program:`mongod` instance is running on
    :param read_preference: The read preference for the collection
       ** Added pymongo 2.1
    :param username: username to authenticate with
    :param password: password to authenticate with
    :param authentication_source: database to authenticate against
    :param is_mock: explicitly use mongomock for this connection
        (can also be done by using `mongomock://` as db host prefix)
    :param kwargs: allow ad-hoc parameters to be passed into the pymongo driver

    .. versionchanged:: 0.10.6 - added mongomock support
    """
    global _connection_settings

    conn_settings = {
        'name': name or 'test',
        'host': host or 'localhost',
        'port': port or 27017,
        'read_preference': read_preference,
        'username': username,
        'password': password,
        'authentication_source': authentication_source
    }

    # Handle uri style connections
    conn_host = conn_settings['host']
    if conn_host.startswith('mongomock://'):
        conn_settings['is_mock'] = True
        # `mongomock://` is not a valid url prefix and must be replaced by `mongodb://`
        conn_settings['host'] = conn_host.replace('mongomock://', 'mongodb://', 1)
    elif '://' in conn_host:
        uri_dict = uri_parser.parse_uri(conn_host)
        conn_settings.update({
            'name': uri_dict.get('database') or name,
            'username': uri_dict.get('username'),
            'password': uri_dict.get('password'),
            'read_preference': read_preference,
        })
        uri_options = uri_dict['options']
        if 'replicaset' in uri_options:
            conn_settings['replicaSet'] = True
        if 'authsource' in uri_options:
            conn_settings['authentication_source'] = uri_options['authsource']

    # Deprecated parameters that should not be passed on
    kwargs.pop('slaves', None)
    kwargs.pop('is_slave', None)

    conn_settings.update(kwargs)
    _connection_settings[alias] = conn_settings


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    global _connections
    global _dbs

    if alias in _connections:
        get_connection(alias=alias).close()
        del _connections[alias]
    if alias in _dbs:
        del _dbs[alias]


def get_connection(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    global _connections
    # Connect to the database if not already connected
    if reconnect:
        disconnect(alias)

    if alias not in _connections:
        if alias not in _connection_settings:
            msg = 'Connection with alias "%s" has not been defined' % alias
            if alias == DEFAULT_CONNECTION_NAME:
                msg = 'You have not defined a default connection'
            raise ConnectionError(msg)
        conn_settings = _connection_settings[alias].copy()

        conn_settings.pop('name', None)
        conn_settings.pop('username', None)
        conn_settings.pop('password', None)
        conn_settings.pop('authentication_source', None)

        is_mock = conn_settings.pop('is_mock', None)
        if is_mock:
            # Use MongoClient from mongomock
            try:
                import mongomock
            except ImportError:
                raise RuntimeError('You need mongomock installed '
                                   'to mock MongoEngine.')
            connection_class = mongomock.MongoClient
        else:
            # Use MongoClient from pymongo
            connection_class = MongoClient

        if 'replicaSet' in conn_settings:
            # Discard port since it can't be used on MongoReplicaSetClient
            conn_settings.pop('port', None)
            # Discard replicaSet if not base string
            if not isinstance(conn_settings['replicaSet'], basestring):
                conn_settings.pop('replicaSet', None)
            if not IS_PYMONGO_3:
                connection_class = MongoReplicaSetClient
                conn_settings['hosts_or_uri'] = conn_settings.pop('host', None)

        try:
            connection = None
            # check for shared connections
            connection_settings_iterator = (
                (db_alias, settings.copy()) for db_alias, settings in _connection_settings.iteritems())
            for db_alias, connection_settings in connection_settings_iterator:
                connection_settings.pop('name', None)
                connection_settings.pop('username', None)
                connection_settings.pop('password', None)
                connection_settings.pop('authentication_source', None)
                if conn_settings == connection_settings and _connections.get(db_alias, None):
                    connection = _connections[db_alias]
                    break

            _connections[alias] = connection if connection else connection_class(**conn_settings)
        except Exception, e:
            raise ConnectionError("Cannot connect to database %s :\n%s" % (alias, e))
    return _connections[alias]


def get_db(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    global _dbs
    if reconnect:
        disconnect(alias)

    if alias not in _dbs:
        conn = get_connection(alias)
        conn_settings = _connection_settings[alias]
        db = conn[conn_settings['name']]
        # Authenticate if necessary
        if conn_settings['username'] and conn_settings['password']:
            db.authenticate(conn_settings['username'],
                            conn_settings['password'],
                            source=conn_settings['authentication_source'])
        _dbs[alias] = db
    return _dbs[alias]


def connect(db=None, alias=DEFAULT_CONNECTION_NAME, **kwargs):
    """Connect to the database specified by the 'db' argument.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple databases are supported by using aliases.  Provide a separate
    `alias` to connect to a different instance of :program:`mongod`.

    .. versionchanged:: 0.6 - added multiple database support.
    """
    global _connections
    if alias not in _connections:
        register_connection(alias, db, **kwargs)

    return get_connection(alias)


# Support old naming convention
_get_connection = get_connection
_get_db = get_db
