import functools
import inspect
import operator

import pymongo
import pytest

from mongoengine.mongodb_support import get_mongodb_version, async_get_mongodb_version

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])

MONGO_TEST_DB = "mongoenginetest"  # standard name for the test database


def requires_mongodb_gte_42(func):
    return _decorated_with_ver_requirement(func, (4, 2), oper=operator.ge)


def requires_mongodb_gte_44(func):
    return _decorated_with_ver_requirement(func, (4, 4), oper=operator.ge)


def requires_mongodb_gte_50(func):
    return _decorated_with_ver_requirement(func, (5, 0), oper=operator.ge)


def requires_mongodb_gte_60(func):
    return _decorated_with_ver_requirement(func, (6, 0), oper=operator.ge)


def requires_mongodb_gte_70(func):
    return _decorated_with_ver_requirement(func, (7, 0), oper=operator.ge)


def requires_mongodb_gte_80(func):
    return _decorated_with_ver_requirement(func, (8, 0), oper=operator.ge)


def _decorated_with_ver_requirement(func, mongo_version_req, oper):
    """Return a MongoDB version requirement decorator.

    Automatically supports both sync and async test functions.

    Uses async_get_mongodb_version() when the test function is async.
    """

    @functools.wraps(func)
    async def _inner_async(*args, **kwargs):

        mongodb_v = await async_get_mongodb_version()
        if not oper(mongodb_v, mongo_version_req):
            pretty_version = ".".join(str(n) for n in mongo_version_req)
            pytest.skip(f"Needs MongoDB {oper.__name__} v{pretty_version}")

        return await func(*args, **kwargs)

    @functools.wraps(func)
    def _inner_sync(*args, **kwargs):

        mongodb_v = get_mongodb_version()
        if not oper(mongodb_v, mongo_version_req):
            pretty_version = ".".join(str(n) for n in mongo_version_req)
            pytest.skip(f"Needs MongoDB {oper.__name__} v{pretty_version}")

        return func(*args, **kwargs)

    # Detect if the decorated function itself is async
    if inspect.iscoroutinefunction(func):
        return _inner_async
    return _inner_sync
