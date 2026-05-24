from pymongo import AsyncMongoClient, ReadPreference
from pymongo.asynchronous import uri_parser
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.common import _UUID_REPRESENTATIONS
from pymongo.driver_info import DriverInfo
from pymongo.errors import ConnectionFailure

import mongoengine
from mongoengine.common import _check_db_name, convert_read_preference

__all__ = [
    "async_connect",
    "async_disconnect",
    "async_disconnect_all",
    "async_get_connection",
    "async_get_db",
    "async_register_connection",
]

from mongoengine.registry import _CollectionRegistry

DEFAULT_CONNECTION_NAME = "default"
DEFAULT_DATABASE_NAME = "test"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 27017

READ_PREFERENCE = ReadPreference.PRIMARY

_connection_settings = {}
_connections = {}
_dbs = {}


async def _async_get_connection_settings(
        db=None,
        name=None,
        host=None,
        port=None,
        read_preference=READ_PREFERENCE,
        username=None,
        password=None,
        authentication_source=None,
        authentication_mechanism=None,
        authmechanismproperties=None,
        **kwargs,
):
    """Build clean connection settings (PyMongo >= 4.13)."""

    # Base settings
    conn_settings = {
        "name": name or db or DEFAULT_DATABASE_NAME,
        "host": host or DEFAULT_HOST,
        "port": port or DEFAULT_PORT,
        "read_preference": read_preference,
        "username": username,
        "password": password,
        "authentication_source": authentication_source,
        "authentication_mechanism": authentication_mechanism,
        "authmechanismproperties": authmechanismproperties,
    }

    _check_db_name(conn_settings["name"])

    # Normalize the host list
    hosts = conn_settings["host"]
    if isinstance(hosts, str):
        hosts = [hosts]

    resolved_hosts = []

    # Handle URI-style hosts
    for entity in hosts:
        if "://" not in entity:
            resolved_hosts.append(entity)
            continue

        uri_info = await uri_parser.parse_uri(entity)
        resolved_hosts.append(entity)

        # override DB name from URI if provided
        if uri_info.get("database"):
            conn_settings["name"] = uri_info["database"]

        # simple extraction (username, password, readPreference)
        for key in ("username", "password"):
            if uri_info.get(key):
                conn_settings[key] = uri_info[key]

        # URI options
        opts = uri_info["options"]

        if "readPreference" in opts:
            conn_settings["read_preference"] = convert_read_preference(value=opts["readPreference"],
                                                                       tag_sets=opts.get("readPreferenceTags"))

        if "replicaSet" in opts:
            conn_settings["replicaset"] = opts["replicaSet"]

        if "authsource" in opts:
            conn_settings["authentication_source"] = opts["authsource"]

        if "authmechanism" in opts:
            conn_settings["authentication_mechanism"] = opts["authmechanism"]

        if "uuidrepresentation" in opts:
            # Map from pymongo enum → driver string
            reverse_uuid = {v: k for k, v in _UUID_REPRESENTATIONS.items()}
            conn_settings["uuidrepresentation"] = reverse_uuid[opts["uuidrepresentation"]]

    conn_settings["host"] = resolved_hosts

    # Strip deprecated junk from kwargs
    for deprecated in ("slaves", "is_slave"):
        kwargs.pop(deprecated, None)

    # Merge real pymongo connection kwargs
    conn_settings.update(kwargs)

    return conn_settings


async def async_register_connection(
        alias,
        db=None,
        name=None,
        host=None,
        port=None,
        read_preference=READ_PREFERENCE,
        username=None,
        password=None,
        authentication_source=None,
        authentication_mechanism=None,
        authmechanismproperties=None,
        **kwargs,
):
    """Register the connection settings.

    :param alias: the name that will be used to refer to this connection throughout MongoEngine
    :param db: the name of the database to use, for compatibility with connect
    :param name: the name of the specific database to use
    :param host: the host name of the: program: `mongod` instance to connect to
    :param port: the port that the: program: `mongod` instance is running on
    :param read_preference: The read preference for the collection
    :param username: username to authenticate with
    :param password: password to authenticate with
    :param authentication_source: database to authenticate against
    :param authentication_mechanism: database authentication mechanisms.
        By default, use SCRAM-SHA-1 with MongoDB 3.0 and later,
        MONGODB-CR (MongoDB Challenge Response protocol) for older servers.
    :param authmechanismproperties: None
    :param mongo_client_class: using alternative connection client other than
        pymongo.AsyncMongoClient, e.g., mongomock, montydb, that provides pymongo similar
        interface but not necessarily for connecting to a real mongo instance.
    :param kwargs: adhoc parameters to be passed into the pymongo driver,
        for example, maxpoolsize, tz_aware, etc. See the documentation
        for pymongo's `MongoClient` for a full list.
    """
    conn_settings = await _async_get_connection_settings(
        db=db,
        name=name,
        host=host,
        port=port,
        read_preference=read_preference,
        username=username,
        password=password,
        authentication_source=authentication_source,
        authentication_mechanism=authentication_mechanism,
        authmechanismproperties=authmechanismproperties,
        **kwargs,
    )
    _connection_settings[alias] = conn_settings


async def async_disconnect(alias=DEFAULT_CONNECTION_NAME):
    """Close the async connection with a given alias."""
    from mongoengine.mongodb_support import reset_mongodb_version_cache

    connection: AsyncMongoClient | None = _connections.pop(alias, None)
    if connection:
        # MongoEngine may share the same MongoClient across multiple aliases
        # if connection settings are the same, so we only close
        # the client if we're removing the final reference.
        # Important to use 'is' instead of '==' because clients connected to the same cluster
        # will compare equal even with different options
        if all(connection is not c for c in _connections.values()):
            await connection.close()
        reset_mongodb_version_cache(alias=alias)

    if alias in _dbs:
        # Detach all cached collections in Documents
        _CollectionRegistry.clear(alias)
        del _dbs[alias]

    if alias in _connection_settings:
        del _connection_settings[alias]


async def async_disconnect_all():
    """Close all registered database."""
    for alias in list(_connections.keys()):
        await async_disconnect(alias)
    _connections.clear()
    _connection_settings.clear()
    _dbs.clear()


def _create_connection(alias, mongo_client_class, **connection_settings):
    """
    Create the new connection for this alias. Raise
    ConnectionFailure if it can't be established.
    """
    try:
        return mongo_client_class(**connection_settings)
    except Exception as e:
        raise ConnectionFailure(f"Cannot connect to database {alias} :\n{e}")


async def async_get_connection(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    """Return a connection with a given alias."""

    # Connect to the database if not already connected
    if reconnect:
        await async_disconnect(alias)

    # If the requested alias already exists in the _connections list, return
    # it immediately.
    if alias in _connections and isinstance(_connections[alias], AsyncMongoClient):
        return _connections[alias]

    # Validate that the requested alias exists in the _connection_settings.
    # Raise ConnectionFailure if it doesn't.
    if alias not in _connection_settings:
        if alias == DEFAULT_CONNECTION_NAME:
            msg = "You have not defined a default connection"
        else:
            msg = 'Connection with alias "%s" has not been defined' % alias
        raise ConnectionFailure(msg)

    def _clean_settings(settings_dict):
        irrelevant_fields_set = {"name"}
        rename_fields = {
            "authentication_source": "authSource",
            "authentication_mechanism": "authMechanism",
        }
        return {
            rename_fields.get(k, k): v
            for k, v in settings_dict.items()
            if k not in irrelevant_fields_set and v is not None
        }

    raw_conn_settings = _connection_settings[alias].copy()

    # Retrieve a copy of the connection settings associated with the requested
    # alias and remove the database name and authentication info (we don't
    # care about them at this point).
    conn_settings = _clean_settings(raw_conn_settings)
    if DriverInfo is not None:
        conn_settings.setdefault(
            "driver", DriverInfo("MongoEngine", mongoengine.__version__)
        )

    # Determine if we should use PyMongo's or mongomock's MongoClient.
    if "mongo_client_class" in conn_settings:
        mongo_client_class = conn_settings.pop("mongo_client_class")
    else:
        mongo_client_class = AsyncMongoClient

    # Re-use an existing connection if one is suitable.
    existing_connection = _find_existing_connection(raw_conn_settings)
    if existing_connection:
        connection = existing_connection
    else:
        connection = _create_connection(
            alias=alias, mongo_client_class=mongo_client_class, **conn_settings
        )
    _connections[alias] = connection
    return _connections[alias]


def _find_existing_connection(connection_settings):
    """
    Check if an existing connection could be reused

    Iterate over all the connection settings, and if an existing connection
    with the same parameters is suitable, return it

    :param connection_settings: the settings of the new connection
    :return: An existing connection or None
    """
    connection_settings_bis = (
        (db_alias, settings.copy())
        for db_alias, settings in _connection_settings.items()
    )

    def _clean_settings(settings_dict):
        # Only remove the name, but it's important to
        # keep the username/password/authentication_source/authentication_mechanism
        # to identify if the connection could be shared (cfr https://github.com/MongoEngine/mongoengine/issues/2047)
        return {k: v for k, v in settings_dict.items() if k != "name"}

    cleaned_conn_settings = _clean_settings(connection_settings)
    for db_alias, connection_settings in connection_settings_bis:
        db_conn_settings = _clean_settings(connection_settings)
        if cleaned_conn_settings == db_conn_settings and _connections.get(db_alias):
            return _connections[db_alias]


async def async_get_db(alias=DEFAULT_CONNECTION_NAME, reconnect=False) -> AsyncDatabase:
    if reconnect:
        await async_disconnect(alias)

    if alias not in _dbs or not isinstance(_dbs[alias], AsyncDatabase):
        conn = await async_get_connection(alias)
        conn_settings = _connection_settings[alias]
        db = conn[conn_settings["name"]]
        # Authenticate if necessary
        _dbs[alias] = db
    return _dbs[alias]


async def async_connect(db=None, alias=DEFAULT_CONNECTION_NAME, **kwargs):
    """Connect to the database specified by the 'db' argument.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple databases are supported by using aliases. Provide a separate
    `alias` to connect to a different instance of: program: `mongod`.

    To replace a connection identified by a given alias, you'll
    need to call ``disconnect`` first

    See the docstring for `register_connection` for more details about all
    supported kwargs.
    """
    if alias in _connections:
        prev_conn_setting = _connection_settings[alias]
        new_conn_settings = await _async_get_connection_settings(db, **kwargs)
        if new_conn_settings != prev_conn_setting:
            err_msg = (
                "A different connection with alias `{}` was already "
                "registered. Use async_disconnect() first"
            ).format(alias)
            raise ConnectionFailure(err_msg)
    else:
        await async_register_connection(alias, db, **kwargs)

    return await async_get_connection(alias)
