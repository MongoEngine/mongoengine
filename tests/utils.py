import operator
import unittest

from nose.plugins.skip import SkipTest

from mongoengine import connect
from mongoengine.connection import get_db, disconnect_all
from mongoengine.mongodb_support import get_mongodb_version, MONGODB_26, MONGODB_3, MONGODB_32, MONGODB_34
from mongoengine.pymongo_support import IS_PYMONGO_3


MONGO_TEST_DB = 'mongoenginetest'   # standard name for the test database


class MongoDBTestCase(unittest.TestCase):
    """Base class for tests that need a mongodb connection
    It ensures that the db is clean at the beginning and dropped at the end automatically
    """

    @classmethod
    def setUpClass(cls):
        disconnect_all()
        cls._connection = connect(db=MONGO_TEST_DB)
        cls._connection.drop_database(MONGO_TEST_DB)
        cls.db = get_db()

    @classmethod
    def tearDownClass(cls):
        cls._connection.drop_database(MONGO_TEST_DB)
        disconnect_all()


def get_as_pymongo(doc):
    """Fetch the pymongo version of a certain Document"""
    return doc.__class__.objects.as_pymongo().get(id=doc.id)


def _decorated_with_ver_requirement(func, mongo_version_req, oper):
    """Return a given function decorated with the version requirement
    for a particular MongoDB version tuple.

    :param mongo_version_req: The mongodb version requirement (tuple(int, int))
    :param oper: The operator to apply (e.g: operator.ge)
    """
    def _inner(*args, **kwargs):
        mongodb_v = get_mongodb_version()
        if oper(mongodb_v, mongo_version_req):
            return func(*args, **kwargs)

        raise SkipTest('Needs MongoDB v{}+'.format('.'.join(str(n) for n in mongo_version_req)))

    _inner.__name__ = func.__name__
    _inner.__doc__ = func.__doc__
    return _inner


def requires_mongodb_gte_34(func):
    """Raise a SkipTest exception if we're working with MongoDB version
    lower than v3.4
    """
    return _decorated_with_ver_requirement(func, MONGODB_34, oper=operator.ge)


def requires_mongodb_lte_32(func):
    """Raise a SkipTest exception if we're working with MongoDB version
    greater than v3.2.
    """
    return _decorated_with_ver_requirement(func, MONGODB_32, oper=operator.le)


def requires_mongodb_gte_26(func):
    """Raise a SkipTest exception if we're working with MongoDB version
    lower than v2.6.
    """
    return _decorated_with_ver_requirement(func, MONGODB_26, oper=operator.ge)


def requires_mongodb_gte_3(func):
    """Raise a SkipTest exception if we're working with MongoDB version
    lower than v3.0.
    """
    return _decorated_with_ver_requirement(func, MONGODB_3, oper=operator.ge)


def skip_pymongo3(f):
    """Raise a SkipTest exception if we're running a test against
    PyMongo v3.x.
    """
    def _inner(*args, **kwargs):
        if IS_PYMONGO_3:
            raise SkipTest("Useless with PyMongo 3+")
        return f(*args, **kwargs)

    _inner.__name__ = f.__name__
    _inner.__doc__ = f.__doc__

    return _inner

