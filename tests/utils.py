import unittest

from nose.plugins.skip import SkipTest

from mongoengine import connect
from mongoengine.connection import get_db, get_connection
from mongoengine.python_support import IS_PYMONGO_3


MONGO_TEST_DB = 'mongoenginetest'   # standard name for the test database


# Constant that can be used to compare the version retrieved with
# get_mongodb_version()
MONGODB_26 = (2, 6)
MONGODB_3 = (3, 0)
MONGODB_32 = (3, 2)


class MongoDBTestCase(unittest.TestCase):
    """Base class for tests that need a mongodb connection
    It ensures that the db is clean at the beginning and dropped at the end automatically
    """

    @classmethod
    def setUpClass(cls):
        cls._connection = connect(db=MONGO_TEST_DB)
        cls._connection.drop_database(MONGO_TEST_DB)
        cls.db = get_db()

    @classmethod
    def tearDownClass(cls):
        cls._connection.drop_database(MONGO_TEST_DB)


def get_as_pymongo(doc):
    """Fetch the pymongo version of a certain Document"""
    return doc.__class__.objects.as_pymongo().get(id=doc.id)


def get_mongodb_version():
    """Return the version of the connected mongoDB (first 2 digits)

    :return: tuple(int, int)
    """
    version_list = get_connection().server_info()['versionArray'][:2]     # e.g: (3, 2)
    return tuple(version_list)


def _decorated_with_ver_requirement(func, version):
    """Return a given function decorated with the version requirement
    for a particular MongoDB version tuple.

    :param version: The version required (tuple(int, int))
    """
    def _inner(*args, **kwargs):
        MONGODB_V = get_mongodb_version()
        if MONGODB_V >= version:
            return func(*args, **kwargs)

        raise SkipTest('Needs MongoDB v{}+'.format('.'.join(str(n) for n in version)))

    _inner.__name__ = func.__name__
    _inner.__doc__ = func.__doc__

    return _inner


def requires_mongodb_gte_26(func):
    """Raise a SkipTest exception if we're working with MongoDB version
    lower than v2.6.
    """
    return _decorated_with_ver_requirement(func, MONGODB_26)


def requires_mongodb_gte_3(func):
    """Raise a SkipTest exception if we're working with MongoDB version
    lower than v3.0.
    """
    return _decorated_with_ver_requirement(func, MONGODB_3)


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

