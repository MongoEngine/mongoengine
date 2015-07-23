from pymongo import MongoClient, ReadPreference, uri_parser
from mongoengine.python_support import IS_PYMONGO_3

__all__ = ['ConnectionError', 'BaseConnectionHandler',
           'connect', 'register_connection', 'set_connection_handler']


class ConnectionError(Exception):
   pass


class BaseConnectionHandler(object):

    """Base class for managing MongoEngine's pymongo connections.

    It's descendants should implement get_connection method, but may use
    another algorythms for opening connections, rather than calling
    register_conntection directly.
    """

    def register_conntection(self, *args, **kwargs):
        raise NotImplementedError

    def delete_connection(self, *args, **kwargs):
        raise NotImplementedError

    def get_connection(self, *args, **kwargs):
        raise NotImplementedError

    def get_db(self, *args, **kwargs):
        raise NotImplementedError

    def connect(self, *args, **kwargs):
        raise NotImplementedError

    def purge(self):
        raise NotImplementedError


if IS_PYMONGO_3:
    READ_PREFERENCE = ReadPreference.PRIMARY
else:
    from pymongo import MongoReplicaSetClient
    READ_PREFERENCE = False


class DefaultConnectionHandler(BaseConnectionHandler):

    """Re-implementation of default way to store conntections."""

    DEFAULT_CONNECTION_NAME = 'default'

    def __init__(self):
        self.__connection_settings__ = {}
        self.__connections__ = {}
        self.__dbs__ = {}

    def purge(self):
        self.__connection_settings__.clear()
        self.__connections__.clear()
        self.__dbs__.clear()

    def register_connection(self, alias=None, name=None,
                            host=None, port=None,
                            read_preference=READ_PREFERENCE,
                            username=None, password=None,
                            authentication_source=None, **kwargs):
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
        :param kwargs: allow ad-hoc parameters to be passed into the pymongo driver
        """
        alias = alias or self.DEFAULT_CONNECTION_NAME
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
        if "://" in conn_settings['host']:
            uri_dict = uri_parser.parse_uri(conn_settings['host'])
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
        self.__connection_settings__[alias] = conn_settings

    def delete_connection(self, alias=None):
        alias = alias or self.DEFAULT_CONNECTION_NAME
        if alias in self.__connections__:
            self.__connections__[alias].disconnect()
            del self.__connections__[alias]
        if alias in self.__dbs__:
            del self.__dbs__[alias]

    def get_connection(self, alias=None, reconnect=False):
        alias = alias or self.DEFAULT_CONNECTION_NAME
        # Connect to the database if not already connected
        if reconnect:
            self.delete_connection(alias)

        if alias not in self.__connections__:
            if alias not in self.__connection_settings__:
                msg = 'Connection with alias "%s" has not been defined' % alias
                if alias == self.DEFAULT_CONNECTION_NAME:
                    msg = 'You have not defined a default connection'
                raise ConnectionError(msg)
            conn_settings = self.__connection_settings__[alias].copy()

            conn_settings.pop('name', None)
            conn_settings.pop('username', None)
            conn_settings.pop('password', None)
            conn_settings.pop('authentication_source', None)

            connection_class = MongoClient
            if 'replicaSet' in conn_settings:
                conn_settings['hosts_or_uri'] = conn_settings.pop('host', None)
                # Discard port since it can't be used on MongoReplicaSetClient
                conn_settings.pop('port', None)
                # Discard replicaSet if not base string
                if not isinstance(conn_settings['replicaSet'], basestring):
                    conn_settings.pop('replicaSet', None)
                if not IS_PYMONGO_3:
                    connection_class = MongoReplicaSetClient

            try:
                connection = None
                # check for shared connections
                connection_settings_iterator = (
                    (db_alias, settings.copy()) for db_alias, settings in self.__connection_settings__.iteritems())
                for db_alias, connection_settings in connection_settings_iterator:
                    connection_settings.pop('name', None)
                    connection_settings.pop('username', None)
                    connection_settings.pop('password', None)
                    if conn_settings == connection_settings and _connections.get(db_alias, None):
                        connection = self.__connections__[db_alias]
                        break

                self.__connections__[alias] = connection if connection else connection_class(**conn_settings)
            except Exception, e:
                raise ConnectionError("Cannot connect to database %s :\n%s" % (alias, e))
        return self.__connections__[alias]

    def get_db(self, alias=None, reconnect=False):
        alias = alias or self.DEFAULT_CONNECTION_NAME

        if reconnect:
            self.delete_connection(alias)

        if alias not in self.__dbs__:
            conn = self.get_connection(alias)
            conn_settings = self.__connection_settings__[alias]
            db = conn[conn_settings['name']]
            # Authenticate if necessary
            if conn_settings['username'] and conn_settings['password']:
                db.authenticate(conn_settings['username'],
                                conn_settings['password'],
                                source=conn_settings['authentication_source'])
            self.__dbs__[alias] = db
        return self.__dbs__[alias]

    def connect(self, db=None, alias=None, **kwargs):
        alias = alias or self.DEFAULT_CONNECTION_NAME

        if alias not in self.__connections__:
            self.register_connection(alias=alias, name=db, **kwargs)

        return self.get_connection(alias)


_connection_handler = DefaultConnectionHandler()


def set_connection_handler(handler):
    global _connection_handler

    _connection_handler = handler


def register_connection(*args, **kwargs):
    return _connection_handler.register_connection(*args, **kwargs)


def disconnect(*args, **kwargs):
    return _connection_handler.delete_connection(*args, **kwargs)


def get_connection(*args, **kwargs):
    return _connection_handler.get_connection(*args, **kwargs)


def get_db(*args, **kwargs):
    return _connection_handler.get_db(*args, **kwargs)


def connect(*args, **kwargs):
    return _connection_handler.connect(*args, **kwargs)


def purge():
    return _connection_handler.purge()


# Support old naming convention
_get_connection = get_connection
_get_db = get_db
