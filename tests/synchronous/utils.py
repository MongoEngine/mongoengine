import functools
import inspect
import operator
import unittest

import pytest

from mongoengine import connect
from mongoengine.registry import _CollectionRegistry
from mongoengine.synchronous.connection import disconnect_all, get_db
from mongoengine.context_managers import query_counter
from mongoengine.mongodb_support import get_mongodb_version, async_get_mongodb_version

from tests.utils import MONGO_TEST_DB, PYMONGO_VERSION


class MongoDBTestCase(unittest.TestCase):
    """Base class for tests that need a mongodb connection
    It ensures that the db is clean at the beginning and dropped at the end automatically
    """

    def setUp(self):
        disconnect_all()
        self._connection = connect(db=MONGO_TEST_DB)
        self._connection.drop_database(MONGO_TEST_DB)
        self.db = get_db()

    def tearDown(self):
        self._connection.drop_database(MONGO_TEST_DB)
        disconnect_all()
        _CollectionRegistry.clear()


def get_as_pymongo(doc, select_related=None, no_dereference=False):
    """Fetch the pymongo version of a certain Document"""
    if select_related:
        return doc.__class__.objects.as_pymongo().select_related(select_related).get(id=doc.id)
    else:
        return doc.__class__.objects.as_pymongo().get(id=doc.id)


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


class db_ops_tracker(query_counter):
    def get_ops(self):
        ignore_query = dict(self._ignored_query)
        ignore_query["command.count"] = {
            "$ne": "system.profile"
        }  # Ignore the query issued by query_counter
        return list(self.db.system.profile.find(ignore_query))


def reset_connections():
    from mongoengine.synchronous.connection import _connections, _connection_settings, _dbs
    for alias, client in list(_connections.items()):
        try:
            client.close()
        except Exception:
            pass

    _connections.clear()
    _connection_settings.clear()
    _dbs.clear()
