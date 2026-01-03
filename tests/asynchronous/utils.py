import functools
import inspect
import operator
import unittest

import pytest

from mongoengine.asynchronous import async_disconnect_all, async_connect, async_get_db, \
    async_disconnect
from mongoengine.base import _DocumentRegistry
from mongoengine.context_managers import async_query_counter
from mongoengine.mongodb_support import get_mongodb_version, async_get_mongodb_version
from mongoengine.registry import _CollectionRegistry

from tests.utils import MONGO_TEST_DB, PYMONGO_VERSION


class MongoDBAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """Base class for tests that need a mongodb connection
    It ensures that the db is clean at the beginning and dropped at the end automatically
    """

    async def asyncSetUp(self):
        await async_disconnect_all()
        self._connection = await async_connect(db=MONGO_TEST_DB)
        await self._connection.drop_database(MONGO_TEST_DB)
        self.db = await async_get_db()

    async def asyncTearDown(self):
        await self._connection.drop_database(MONGO_TEST_DB)
        await async_disconnect()
        await reset_async_connections()
        _DocumentRegistry.clear()
        _CollectionRegistry.clear()


async def async_get_as_pymongo(doc, select_related=None, no_dereference=False):
    """Fetch the pymongo version of a certain Document"""
    if select_related:
        return await doc.__class__.aobjects.as_pymongo().select_related(select_related).get(id=doc.id)
    else:
        return await doc.__class__.aobjects.as_pymongo().get(id=doc.id)


def requires_mongodb_lt_42(func):
    return _decorated_with_ver_requirement(func, (4, 2), oper=operator.lt)


def requires_mongodb_gte_40(func):
    return _decorated_with_ver_requirement(func, (4, 0), oper=operator.ge)


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


class async_db_ops_tracker(async_query_counter):
    async def get_ops(self):
        ignore_query = dict(self._ignored_query)
        ignore_query["command.count"] = {
            "$ne": "system.profile"
        }  # Ignore the query issued by query_counter
        return [doc async for doc in (await self.db).system.profile.find(ignore_query)]


async def reset_async_connections():
    from mongoengine.asynchronous.connection import _connections, _connection_settings, _dbs
    for alias, client in list(_connections.items()):
        try:
            await client.close()
        except Exception:
            pass

    _connections.clear()
    _connection_settings.clear()
    _dbs.clear()
