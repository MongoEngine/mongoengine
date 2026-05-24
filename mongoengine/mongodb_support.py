"""
Helper functions, constants, and types to aid with MongoDB version support
"""

from mongoengine.asynchronous import async_get_connection
from mongoengine.synchronous.connection import DEFAULT_CONNECTION_NAME, get_connection

# Constant that can be used to compare the version retrieved with
# get_mongodb_version()
MONGODB_42 = (4, 2)
MONGODB_44 = (4, 4)
MONGODB_50 = (5, 0)
MONGODB_60 = (6, 0)
MONGODB_70 = (7, 0)
MONGODB_80 = (8, 0)

# Cache the server version per alias — server_info() is a network roundtrip and the
# MongoDB version doesn't change during a process lifetime. Different aliases can point
# to different clusters with different versions, so the cache MUST be keyed per-alias.
_VERSION_CACHE: dict[str, tuple] = {}


def get_mongodb_version(alias: str = DEFAULT_CONNECTION_NAME):
    """Return the version of the connected MongoDB for the given alias (first 2 digits).

    :param alias: Connection alias. Different aliases may point to different clusters.
    :return: tuple(int, int)
    """
    cached = _VERSION_CACHE.get(alias)
    if cached is not None:
        return cached
    version = tuple(get_connection(alias=alias).server_info()["versionArray"][:2])
    _VERSION_CACHE[alias] = version
    return version


async def async_get_mongodb_version(alias: str = DEFAULT_CONNECTION_NAME):
    """Return the version of the connected MongoDB for the given alias (first 2 digits).

    :param alias: Connection alias. Different aliases may point to different clusters.
    :return: tuple(int, int)
    """
    cached = _VERSION_CACHE.get(alias)
    if cached is not None:
        return cached
    conn = await async_get_connection(alias=alias)
    version = tuple((await conn.server_info())["versionArray"][:2])
    _VERSION_CACHE[alias] = version
    return version


def reset_mongodb_version_cache(alias: str | None = None):
    """Clear cached server version. Pass alias to clear one entry, None to clear all."""
    if alias is None:
        _VERSION_CACHE.clear()
    else:
        _VERSION_CACHE.pop(alias, None)
